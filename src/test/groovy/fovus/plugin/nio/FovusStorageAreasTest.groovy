package fovus.plugin.nio

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusS3ClientTest
import fovus.plugin.s3.S3Entry
import nextflow.file.FileHelper
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CommonPrefix
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.CopyObjectResponse
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import java.nio.file.AccessDeniedException
import java.nio.file.AccessMode
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.NotDirectoryException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.util.stream.Collectors

/** The files/, jobs/ and pipelines/ areas of Fovus storage: one file system each, all read through the provider's one S3 client. */
class FovusStorageAreasTest extends Specification {

    static final List<String> AREAS = ['files', 'jobs', 'pipelines']

    /** Each operation that needs the S3 client, on a path just below an area root. */
    static final Map<String, Closure> ACCESSES = [
            'exists'                           : { Path p -> Files.exists(p) },
            'readAttributes'                   : { Path p -> Files.readAttributes(p, BasicFileAttributes) },
            'newInputStream'                   : { Path p -> Files.newInputStream(p) },
            'newDirectoryStream of the parent' : { Path p -> Files.newDirectoryStream(p.parent) },
    ]

    static final String JOBS_READ_ONLY = 'Fovus storage jobs/ is read-only'

    @TempDir
    Path tempDir

    S3Client s3 = Mock()
    FovusFileSystemProvider provider = new FovusFileSystemProvider()

    /** The bucket behind {@link #bucketWith}: key to text. */
    Map<String, String> objects = new TreeMap<>()
    /** The GetObject, PutObject, CopyObject and DeleteObject requests it received, in order. */
    List<String> calls = []
    ListAppender<ILoggingEvent> storageLog

    def setup() {
        // As in WorkDirStorageFactoryTest: FileHelper.asPath(URI) then resolves fovus:// paths as it does under
        // Nextflow, creating one file system per area, all on this provider
        FileHelper.providersMap['fovus'] = provider
    }

    def cleanup() {
        FileHelper.providersMap.remove('fovus')
        if (storageLog != null) (LoggerFactory.getLogger(S3Storage) as Logger).detachAppender(storageLog)
    }

    /** {@code fovus:///fovus-storage/<path>}, resolved the way Nextflow resolves it. */
    private static Path fovus(String path) {
        return FileHelper.asPath(URI.create("fovus:///fovus-storage/${path}"))
    }

    /** What direct mode does once credentials exist: one client for the pipeline, attached to the provider. */
    private void attachClient() {
        provider.attachS3Client(new FovusS3Client(s3, s3, 'bucket', 'pipelines/p-1-user/', null))
    }

    private void listingsArePaginated() {
        s3.listObjectsV2Paginator(_ as ListObjectsV2Request) >> { ListObjectsV2Request request -> new ListObjectsV2Iterable(s3, request) }
    }

    /**
     * Back the mocked client with {@link #objects}, a bucket holding {@code initial}. As the write token does, it
     * denies CopyObject and DeleteObject (403) unless they are allowed.
     */
    private void bucketWith(Map<String, String> initial, boolean copyAllowed = false, boolean deleteAllowed = false) {
        objects.putAll(initial)
        listingsArePaginated()
        s3.headObject(_ as HeadObjectRequest) >> { HeadObjectRequest request ->
            if (!objects.containsKey(request.key())) throw FovusS3ClientTest.s3Error(404, 'NotFound')
            HeadObjectResponse.builder().contentLength((long) objects[request.key()].length()).build()
        }
        s3.listObjectsV2(_ as ListObjectsV2Request) >> { ListObjectsV2Request request -> listObjects(request) }
        s3.getObject(_ as GetObjectRequest) >> { GetObjectRequest request ->
            calls << "GET ${request.key()}".toString()
            if (!objects.containsKey(request.key())) throw FovusS3ClientTest.s3Error(404, 'NoSuchKey')
            body(objects[request.key()])
        }
        s3.putObject(_ as PutObjectRequest, _ as RequestBody) >> { PutObjectRequest request, RequestBody body ->
            calls << "PUT ${request.key()}".toString()
            objects[request.key()] = body.contentStreamProvider().newStream().withCloseable { InputStream stream -> stream.text }
            PutObjectResponse.builder().build()
        }
        s3.copyObject(_ as CopyObjectRequest) >> { CopyObjectRequest request ->
            calls << "COPY ${request.sourceKey()} -> ${request.destinationKey()}".toString()
            if (!copyAllowed) throw FovusS3ClientTest.s3Error(403, 'AccessDenied')
            objects[request.destinationKey()] = objects[request.sourceKey()]
            CopyObjectResponse.builder().build()
        }
        s3.deleteObject(_ as DeleteObjectRequest) >> { DeleteObjectRequest request ->
            calls << "DELETE ${request.key()}".toString()
            if (!deleteAllowed) throw FovusS3ClientTest.s3Error(403, 'AccessDenied')
            objects.remove(request.key())
            DeleteObjectResponse.builder().build()
        }
    }

    /** What S3 answers for a prefix, with or without the {@code /} delimiter. */
    private ListObjectsV2Response listObjects(ListObjectsV2Request request) {
        final List<S3Object> contents = []
        final Set<String> folders = new TreeSet<>()
        for (String key : objects.keySet()) {
            if (!key.startsWith(request.prefix())) continue
            final rest = key.substring(request.prefix().length())
            final slash = request.delimiter() == null ? -1 : rest.indexOf('/')
            if (slash >= 0) folders.add(request.prefix() + rest.substring(0, slash + 1))
            else contents.add(S3Object.builder().key(key).size((long) objects[key].length()).build())
        }
        return ListObjectsV2Response.builder()
                .contents(contents)
                .commonPrefixes(folders.collect { String folder -> CommonPrefix.builder().prefix(folder).build() })
                .keyCount(contents.size() + folders.size())
                .isTruncated(false)
                .build()
    }

    /** The log of {@link S3Storage}, where a delete that is left in place is reported. */
    private ListAppender<ILoggingEvent> captureStorageLog() {
        final logger = LoggerFactory.getLogger(S3Storage) as Logger
        logger.level = Level.DEBUG
        storageLog = new ListAppender<ILoggingEvent>()
        storageLog.start()
        logger.addAppender(storageLog)
        return storageLog
    }

    private Map<String, String> objectsUnder(String prefix) {
        return objects.findAll { String key, String text -> key.startsWith(prefix) }
    }

    /** A folder output of a task: the folder, a file and a sub-folder with a file, with their markers. */
    private static Map<String, String> folderOutput() {
        return ['pipelines/p-1-user/out/dir/'          : '',
                'pipelines/p-1-user/out/dir/a.txt'     : 'a',
                'pipelines/p-1-user/out/dir/sub/'      : '',
                'pipelines/p-1-user/out/dir/sub/b.txt' : 'b']
    }

    /** {@link #folderOutput} as published to files/results/dir. */
    private static Map<String, String> publishedFolder() {
        return ['files/results/dir/'          : '',
                'files/results/dir/a.txt'     : 'a',
                'files/results/dir/sub/'      : '',
                'files/results/dir/sub/b.txt' : 'b']
    }

    private static ResponseInputStream<GetObjectResponse> body(String text) {
        return new ResponseInputStream<GetObjectResponse>(GetObjectResponse.builder().build(),
                                                          AbortableInputStream.create(new ByteArrayInputStream(text.bytes)))
    }

    private static ListObjectsV2Response listing(List<String> keys, List<String> folders = []) {
        return ListObjectsV2Response.builder()
                .contents(keys.collect { String key -> S3Object.builder().key(key).size(1L).build() })
                .commonPrefixes(folders.collect { String folder -> CommonPrefix.builder().prefix(folder).build() })
                .isTruncated(false)
                .build()
    }

    def 'pipelines paths should parse and print as the compute node sees them'() {
        given:
        def fs = StorageTestSupport.fileSystem()

        when:
        def path = (FovusPath) fs.getPath('/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef')

        then:
        path.fileType == 'pipelines'
        path.key == 'p-1-user/fovus-work/ab/cdef'
        path.toString() == '/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef'
        path.toUri().toString() == 'fovus:///fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef'
        path.parent.toString() == '/fovus-storage/pipelines/p-1-user/fovus-work/ab'
        !fs.isReadOnly()
    }

    def 'an area root should be /fovus-storage/<area> itself, in every area'() {
        given:
        def fs = StorageTestSupport.fileSystem()

        expect:
        (fs.getPath("/fovus-storage/${area}") as FovusPath).isAreaRoot()
        (fs.getPath("/fovus-storage/${area}/") as FovusPath).isAreaRoot()
        !(fs.getPath("/fovus-storage/${area}/p-1-user") as FovusPath).isAreaRoot()
        !(fs.getPath('') as FovusPath).isAreaRoot()

        where:
        area << AREAS
    }

    def 'only the jobs area should be read-only'() {
        expect:
        !fovus('files').fileSystem.isReadOnly()
        fovus('jobs').fileSystem.isReadOnly()
        !fovus('pipelines').fileSystem.isReadOnly()
    }

    def 'every area root should exist and be a folder before any client is attached'() {
        given:
        def root = fovus(area)

        when:
        Files.createDirectories(root)

        then: 'answered without the S3 client, which is not there to call: Session.init() does this with the workDir'
        Files.exists(root)
        Files.isDirectory(root)
        Files.isReadable(root)
        !provider.hasS3Client()

        where:
        area << AREAS
    }

    def 'any other access before the S3 client is attached should explain direct mode'() {
        given: 'a path just below the area root, so its parent is the root, which is listed through S3'
        def path = fovus("${area}/x")

        when:
        ACCESSES[access].call(path)

        then: 'exists() throws rather than answer false, as it does for pipelines/: a missing input would hide the reason'
        def e = thrown(IllegalStateException)
        e.message == FovusFileSystemProvider.NOT_ATTACHED_MESSAGE
        e.message == "Fovus storage paths (fovus://) can only be used in direct mode (workDir = 'fovus:///fovus-storage/pipelines'), " +
                "after the Fovus executor has started. With a Fovus storage mount, use the mounted path instead."

        where:
        [area, access] << [AREAS, ACCESSES.keySet() as List].combinations()
    }

    def 'a files/ input should be read through S3'() {
        given:
        attachClient()
        def input = fovus('files/data/in.txt')

        when:
        def size = Files.size(input)
        def text = Files.newInputStream(input).withCloseable { InputStream stream -> stream.text }

        then:
        1 * s3.headObject({ HeadObjectRequest r -> r.bucket() == 'bucket' && r.key() == 'files/data/in.txt' }) >>
                HeadObjectResponse.builder().contentLength(5L).build()
        1 * s3.getObject({ GetObjectRequest r -> r.bucket() == 'bucket' && r.key() == 'files/data/in.txt' }) >> body('hello')
        0 * s3._
        size == 5
        text == 'hello'
    }

    def 'a jobs/ output should be read through S3'() {
        given:
        attachClient()
        def output = fovus('jobs/j-1/out.txt')

        when:
        def size = Files.size(output)
        def text = Files.newInputStream(output).withCloseable { InputStream stream -> stream.text }

        then:
        1 * s3.headObject({ HeadObjectRequest r -> r.bucket() == 'bucket' && r.key() == 'jobs/j-1/out.txt' }) >>
                HeadObjectResponse.builder().contentLength(4L).build()
        1 * s3.getObject({ GetObjectRequest r -> r.bucket() == 'bucket' && r.key() == 'jobs/j-1/out.txt' }) >> body('done')
        0 * s3._
        size == 4
        text == 'done'
    }

    def 'listing the files area root should list files/ once'() {
        given:
        attachClient()
        listingsArePaginated()

        when:
        def children = Files.newDirectoryStream(fovus('files')).withCloseable { stream -> stream.collect { Path p -> p.toString() } }

        then: 'one listing of files/, never files//'
        1 * s3.listObjectsV2({ ListObjectsV2Request r -> r.prefix() == 'files/' && r.delimiter() == '/' }) >>
                listing(['files/a.txt'], ['files/data/'])
        0 * s3.listObjectsV2(_)
        0 * s3.headObject(_)
        children.sort() == ['/fovus-storage/files/a.txt', '/fovus-storage/files/data']
    }

    def 'a glob over a files/ folder should not look up each file'() {
        given:
        attachClient()
        listingsArePaginated()
        def folder = fovus('files/data')
        def pages = ['files/data/'       : listing(['files/data/a.txt', 'files/data/b.txt', 'files/data/c.log'], ['files/data/nested/']),
                     'files/data/nested/': listing(['files/data/nested/d.txt'])]
        // hasChildren asks for one key; a listing asks for a page
        s3.listObjectsV2(_ as ListObjectsV2Request) >> { ListObjectsV2Request r ->
            r.maxKeys() == 1 ? ListObjectsV2Response.builder().keyCount(1).build() : pages[r.prefix()]
        }
        def found = []

        when: 'the walk output collection does; a glob pattern would need --add-opens for sun.nio.fs, which this JVM lacks'
        FileHelper.visitFiles([type: 'file', syntax: 'regex', maxDepth: 2], folder, '.*\\.txt') { Path p -> found << folder.relativize(p).toString() }

        then: 'only the folder walked from is looked up: every file below it comes with its listing'
        1 * s3.headObject({ HeadObjectRequest r -> r.key() == 'files/data' }) >> { throw FovusS3ClientTest.s3Error(404, 'NotFound') }
        0 * s3.headObject(_)
        found.sort() == ['a.txt', 'b.txt', 'nested/d.txt']
    }

    def 'a delete the write token does not allow should only warn'() {
        given:
        def client = Stub(FovusS3Client) {
            head(_) >> new S3Entry('pipelines/p-1-user/x', 1L, null, false)
            delete(_) >> {
                throw new AccessDeniedException('fovus:///fovus-storage/pipelines/p-1-user/x', null,
                                                "Fovus storage credentials don't allow delete on pipelines/p-1-user/x (write token)")
            }
        }
        def path = StorageTestSupport.fileSystem(client).getPath('/fovus-storage/pipelines/p-1-user/x')

        when:
        Files.delete(path)

        then:
        noExceptionThrown()
    }

    @Unroll
    def 'the root of an empty #area area should list as an empty folder'() {
        given:
        attachClient()
        bucketWith([:])

        expect: 'it exists and is a folder, so listing it cannot be a missing folder'
        Files.isDirectory(fovus(area))
        Files.newDirectoryStream(fovus(area)).withCloseable { stream -> stream.collect { Path p -> p.toString() } } == []

        where:
        area << AREAS
    }

    def 'a folder below an area root should keep its missing-folder and not-a-folder errors'() {
        given:
        attachClient()
        bucketWith(['files/a.txt': 'a'])

        when:
        Files.newDirectoryStream(fovus('files/nothing'))

        then:
        thrown(NoSuchFileException)

        when:
        Files.newDirectoryStream(fovus('files/a.txt'))

        then:
        thrown(NotDirectoryException)
    }

    def 'publishDir into files/ should copy a pipeline output across areas'() {
        given:
        attachClient()
        bucketWith(['pipelines/p-1-user/fovus-work/ab/cdef/out.txt': 'hello'])
        def source = fovus('pipelines/p-1-user/fovus-work/ab/cdef/out.txt')
        def target = fovus('files/results/out.txt')

        when: 'as publishDir does it: both paths share the provider, so Nextflow copies through it'
        FileHelper.copyPath(source, target)

        then: 'the denied CopyObject falls back to reading the pipeline object and uploading it to files/'
        calls == ['COPY pipelines/p-1-user/fovus-work/ab/cdef/out.txt -> files/results/out.txt',
                  'GET pipelines/p-1-user/fovus-work/ab/cdef/out.txt',
                  'PUT files/results/out.txt']
        objects['files/results/out.txt'] == 'hello'
        objects['pipelines/p-1-user/fovus-work/ab/cdef/out.txt'] == 'hello'
    }

    def 'publishDir into files/ should copy with CopyObject when it is allowed'() {
        given:
        attachClient()
        bucketWith(['pipelines/p-1-user/out.txt': 'hello'], true)

        when:
        FileHelper.copyPath(fovus('pipelines/p-1-user/out.txt'), fovus('files/results/out.txt'))

        then:
        calls == ['COPY pipelines/p-1-user/out.txt -> files/results/out.txt']
        objects['files/results/out.txt'] == 'hello'
    }

    def 'publishDir into files/ should copy a folder output object by object'() {
        given:
        attachClient()
        bucketWith(folderOutput())

        when:
        FileHelper.copyPath(fovus('pipelines/p-1-user/out/dir'), fovus('files/results/dir'))

        then: 'the target folder marker and every file land under files/results/, and the source is untouched'
        objectsUnder('files/') == publishedFolder()
        objectsUnder('pipelines/') == folderOutput()
        calls.findAll { String call -> call.startsWith('DELETE') } == []
    }

    def 'a move of a folder into files/ should copy every object and leave the source with one warning'() {
        given:
        attachClient()
        bucketWith(folderOutput())
        def logged = captureStorageLog()

        when: 'as publishDir does it with mode: move'
        FileHelper.movePath(fovus('pipelines/p-1-user/out/dir'), fovus('files/results/dir'))

        then: 'every object was copied, and the deletes the write token does not allow left the source in place'
        objectsUnder('files/') == publishedFolder()
        objectsUnder('pipelines/') == folderOutput()
        def warnings = logged.list.findAll { ILoggingEvent event -> event.level == Level.WARN }
        warnings.size() == 1
        warnings[0].formattedMessage.contains('/fovus-storage/pipelines/p-1-user/out/dir was left in place')
        calls.findAll { String call -> call.startsWith('DELETE') }.size() == 1
        !warnings[0].formattedMessage.contains('SECRET-BODY')
    }

    def 'a move of a folder into files/ should leave only the target when deletes are allowed'() {
        given:
        attachClient()
        bucketWith(folderOutput(), true, true)

        when:
        FileHelper.movePath(fovus('pipelines/p-1-user/out/dir'), fovus('files/results/dir'))

        then:
        objectsUnder('files/') == publishedFolder()
        objectsUnder('pipelines/') == [:]
    }

    def 'a move of a folder should keep the keys it has, with no marker objects added'() {
        given: 'no marker objects: out/dir and its sub-folder exist only as key prefixes'
        attachClient()
        bucketWith(['pipelines/p-1-user/out/dir/a.txt': 'a', 'pipelines/p-1-user/out/dir/sub/b.txt': 'b'], true, true)

        when:
        FileHelper.movePath(fovus('pipelines/p-1-user/out/dir'), fovus('files/results/dir'))

        then:
        objectsUnder('files/') == ['files/results/dir/a.txt': 'a', 'files/results/dir/sub/b.txt': 'b']
        objectsUnder('pipelines/') == [:]
    }

    def 'a move of a folder onto an existing one should fail unless it replaces it, and copy nothing'() {
        given:
        attachClient()
        bucketWith(folderOutput() + ['files/results/dir/old.txt': 'old'], true, true)

        when:
        FileHelper.movePath(fovus('pipelines/p-1-user/out/dir'), fovus('files/results/dir'))

        then:
        thrown(FileAlreadyExistsException)
        objectsUnder('files/') == ['files/results/dir/old.txt': 'old']
        objectsUnder('pipelines/') == folderOutput()
    }

    def 'a move of a file into files/ should copy it, then leave the source with a warning'() {
        given:
        attachClient()
        bucketWith(['pipelines/p-1-user/out.txt': 'hello'], true)
        def logged = captureStorageLog()

        when:
        FileHelper.movePath(fovus('pipelines/p-1-user/out.txt'), fovus('files/results/out.txt'))

        then:
        calls == ['COPY pipelines/p-1-user/out.txt -> files/results/out.txt', 'DELETE pipelines/p-1-user/out.txt']
        objects == ['pipelines/p-1-user/out.txt': 'hello', 'files/results/out.txt': 'hello']
        logged.list.findAll { ILoggingEvent event -> event.level == Level.WARN }.size() == 1
    }

    def 'a local file and folder should upload into files/'() {
        given:
        attachClient()
        bucketWith([:])
        def file = Files.writeString(tempDir.resolve('x.txt'), 'x')
        def folder = Files.createDirectories(tempDir.resolve('dir/sub')).parent
        Files.writeString(folder.resolve('a.txt'), 'a')
        Files.writeString(folder.resolve('sub/b.txt'), 'b')

        when: 'as input staging and publishing do it, through FileHelper'
        FileHelper.copyPath(file, fovus('files/in/x.txt'))
        FileHelper.copyPath(folder, fovus('files/in/dir'))

        then:
        objects == ['files/in/x.txt'        : 'x',
                    'files/in/dir/'         : '',
                    'files/in/dir/a.txt'    : 'a',
                    'files/in/dir/sub/'     : '',
                    'files/in/dir/sub/b.txt': 'b']
    }

    @Unroll
    def 'writes into jobs/ should be refused as read-only before calling S3: #operation'() {
        given:
        attachClient()
        def jobs = fovus('jobs/j-1/out.txt')
        def other = fovus('files/data/in.txt')
        def local = Files.writeString(tempDir.resolve('in.txt'), 'x')

        when:
        action.call(jobs, other, local)

        then:
        def e = thrown(AccessDeniedException)
        e.message.contains(JOBS_READ_ONLY)
        0 * s3._

        where:
        operation                       | action
        'newOutputStream'               | { Path inJobs, Path inFiles, Path onDisk -> Files.newOutputStream(inJobs) }
        'newOutputStream (CREATE_NEW)'  | { Path inJobs, Path inFiles, Path onDisk -> Files.newOutputStream(inJobs, StandardOpenOption.CREATE_NEW) }
        'createDirectory'               | { Path inJobs, Path inFiles, Path onDisk -> Files.createDirectory(inJobs) }
        'upload'                        | { Path inJobs, Path inFiles, Path onDisk -> inJobs.fileSystem.provider().upload(onDisk, inJobs) }
        'copy target'                   | { Path inJobs, Path inFiles, Path onDisk -> Files.copy(inFiles, inJobs) }
        'move target'                   | { Path inJobs, Path inFiles, Path onDisk -> Files.move(inFiles, inJobs) }
        'move source, which is deleted' | { Path inJobs, Path inFiles, Path onDisk -> Files.move(inJobs, inFiles) }
        'delete'                        | { Path inJobs, Path inFiles, Path onDisk -> Files.delete(inJobs) }
    }

    def 'checkAccess for WRITE should fail on a jobs path as read-only, and on nothing else'() {
        given:
        attachClient()
        bucketWith(['files/x': 'x', 'pipelines/p-1-user/x': 'x', 'jobs/j-1/x': 'x'])
        def jobsRoot = fovus('jobs')

        when:
        provider.checkAccess(fovus('jobs/j-1/x'), AccessMode.WRITE)

        then:
        def e = thrown(AccessDeniedException)
        e.message.contains(JOBS_READ_ONLY)

        when:
        provider.checkAccess(jobsRoot, AccessMode.READ, AccessMode.WRITE)

        then:
        thrown(AccessDeniedException)

        and: 'reading a jobs path, and writing in the other areas, works as before'
        !Files.isWritable(fovus('jobs/j-1/x'))
        !Files.isWritable(jobsRoot)
        Files.isReadable(fovus('jobs/j-1/x'))
        Files.isWritable(fovus('files/x'))
        Files.isWritable(fovus('pipelines/p-1-user/x'))
        Files.isWritable(fovus('files'))
        Files.isWritable(fovus('pipelines'))
    }

    @Unroll
    def 'canUpload should accept pipelines and files targets from any file system, and refuse jobs: #kind source, #area target'() {
        given:
        FileSystem foreignFileSystem = Stub()
        Path foreign = Stub { getFileSystem() >> foreignFileSystem }
        def source = kind == 'default' ? Files.writeString(tempDir.resolve('in.txt'), 'x') : foreign
        def target = fovus(area + '/x')

        expect: 'jobs is read-only; the other areas stream a remote source, publishing it only once it was read in full'
        provider.canUpload(source, target) == expected

        where:
        [kind, area] << [['default', 'https'], AREAS].combinations()
        expected = area != 'jobs'
    }

    // The two cases below stand in for the MinIO integration tests (S3StorageIT), which are not run here.

    def 'an empty file should be a regular file of size 0'() {
        given:
        def client = Stub(FovusS3Client) {
            head('pipelines/p-1-user/.command.err') >> new S3Entry('pipelines/p-1-user/.command.err', 0L, null, false)
        }
        def empty = StorageTestSupport.fileSystem(client).getPath('/fovus-storage/pipelines/p-1-user/.command.err')

        expect:
        Files.exists(empty)
        Files.isRegularFile(empty)
        !Files.isDirectory(empty)
        Files.size(empty) == 0
    }

    def 'names that share a prefix should not be confused'() {
        given: 'out.txt.bak exists, but neither an out.txt object nor an out.txt/ folder does'
        def client = Stub(FovusS3Client) {
            head('pipelines/p-1-user/out.txt') >> null
            hasChildren('pipelines/p-1-user/out.txt/') >> false
            list('pipelines/p-1-user/sample/') >> [new S3Entry('pipelines/p-1-user/sample/y.txt', 1L, null, false)]
        }
        def fs = StorageTestSupport.fileSystem(client)

        expect:
        !Files.exists(fs.getPath('/fovus-storage/pipelines/p-1-user/out.txt'))
        names(fs.getPath('/fovus-storage/pipelines/p-1-user/sample')) == ['y.txt']
    }

    private static List<String> names(Path folder) {
        return Files.list(folder).withCloseable { stream ->
            stream.map { Path p -> p.fileName.toString() }.sorted().collect(Collectors.toList())
        }
    }
}
