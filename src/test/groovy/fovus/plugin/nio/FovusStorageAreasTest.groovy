package fovus.plugin.nio

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusS3ClientTest
import fovus.plugin.s3.RecordingUploadStream
import fovus.plugin.s3.S3Entry
import fovus.plugin.s3.S3Transfers
import fovus.plugin.s3.S3UploadStream
import fovus.plugin.util.FovusPathFactory
import fovus.plugin.util.FovusPathSerializer
import nextflow.extension.FilesEx
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
import java.nio.file.StandardCopyOption
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

    /** Names a user may give a folder or file that a URI cannot hold as they are. */
    static final List<String> AWKWARD_NAMES = ['out.txt', 'x[1].txt', 'a#b.txt', '100%.txt', 'p%20q.txt', 'what?.txt', 'é ü.txt']

    @TempDir
    Path tempDir

    S3Client s3 = Mock()
    S3Transfers transfers = Mock()
    FovusFileSystemProvider provider = new FovusFileSystemProvider()

    /** The bucket behind {@link #bucketWith}: key to text. */
    Map<String, String> objects = new TreeMap<>()
    /**
     * The requests it received, in order: GetObject, CopyObject and DeleteObject, and a PutObject for each folder marker,
     * file upload and streamed write (the last two through the transfers, as the Transfer Manager sends them).
     */
    List<String> calls = []
    ListAppender<ILoggingEvent> storageLog
    Level storageLevelBefore

    def setup() {
        // As in WorkDirStorageFactoryTest: FileHelper.asPath(URI) then resolves fovus:// paths as it does under
        // Nextflow, creating one file system per area, all on this provider
        FileHelper.providersMap['fovus'] = provider
    }

    def cleanup() {
        FileHelper.providersMap.remove('fovus')
        if (storageLog != null) {
            final logger = LoggerFactory.getLogger(S3Storage) as Logger
            logger.detachAppender(storageLog)
            logger.level = storageLevelBefore
        }
    }

    /**
     * {@code fovus:///fovus-storage/<path>}, resolved the way Nextflow resolves it: the text as a pipeline names it
     * (spaces and all) made a URI as {@code FileHelper.toPathURI} does, then {@code FileHelper.asPath(URI)}.
     */
    private static Path fovus(String path) {
        return FileHelper.asPath(new URI(null, null, "fovus:///fovus-storage/${path}".toString(), null, null))
    }

    /** What direct mode does once credentials exist: one client for the pipeline, attached to the provider. */
    private void attachClient() {
        provider.attachS3Client(new FovusS3Client(s3, s3, transfers, 'bucket', 'pipelines/p-1-user/', null))
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
        transfers.uploadFile(_ as Path, _ as String) >> { Path file, String key ->
            calls << "PUT ${key}".toString()
            objects[key] = file.text
        }
        transfers.newUploadStream(_ as String, _) >> { String key, Long length ->
            new RecordingUploadStream({ byte[] bytes ->
                calls << "PUT ${key}".toString()
                objects[key] = new String(bytes)
            })
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
        storageLevelBefore = logger.level
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

    def 'a URI should encode what a URI cannot hold, and decode back to the name'() {
        expect:
        fovus('files/My Results/x[1].txt').toUri().toString() == 'fovus:///fovus-storage/files/My%20Results/x%5B1%5D.txt'
        fovus('files/a#b%20c?.txt').toUri().toString() == 'fovus:///fovus-storage/files/a%23b%2520c%3F.txt'
        fovus('files/My Results/x[1].txt').toUri().path == '/fovus-storage/files/My Results/x[1].txt'
        fovus('files').toUri().toString() == 'fovus:///fovus-storage/files/'
    }

    @Unroll
    def 'a path with #name in it should come back equal from each form Nextflow keeps it in'() {
        given:
        def path = fovus("files/My Results/${name}")
        def kryo = new Kryo()
        def serialized = new Output(1024, -1)
        new FovusPathSerializer().write(kryo, serialized, (FovusPath) path)

        expect: 'its URI, and that URI as text'
        path.toUri().path == "/fovus-storage/files/My Results/${name}".toString()
        FovusPath.getFileTypeOfUri(path.toUri()) == 'files'
        FileHelper.asPath(path.toUri()) == path
        FileHelper.asPath(URI.create(path.toUri().toString())) == path

        and: 'the URI string of the plugin path factory, which FilesEx.toUriString returns, parsed back by FileHelper.asPath'
        new FovusPathFactory().toUriString(path) == "fovus://fovus-storage/files/My Results/${name}".toString()
        FilesEx.toUriString(path) == new FovusPathFactory().toUriString(path)
        FileHelper.asPath(FilesEx.toUriString(path)) == path

        and: 'the -resume cache entry the plugin serializer writes'
        new FovusPathSerializer().read(kryo, new Input(serialized.toBytes()), FovusPath) == path

        where:
        name << AWKWARD_NAMES
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

    @Unroll
    def 'a missing files/ path with #name in a folder with a space should not exist, and its attributes should be missing'() {
        given:
        attachClient()
        bucketWith([:])
        def missing = fovus("files/My Results/${name}")

        expect:
        !Files.exists(missing)

        when:
        Files.readAttributes(missing, BasicFileAttributes)

        then: 'the message names the path as the pipeline spells it'
        def e = thrown(NoSuchFileException)
        e.file == "fovus:///fovus-storage/files/My Results/${name}".toString()

        when:
        Files.newDirectoryStream(missing)

        then:
        def listing = thrown(NoSuchFileException)
        listing.file == "fovus:///fovus-storage/files/My Results/${name}".toString()

        where:
        name << AWKWARD_NAMES
    }

    def 'publishDir into a files/ folder with a space in its name should copy the output'() {
        given:
        attachClient()
        bucketWith(['pipelines/p-1-user/fovus-work/ab/cdef/out.txt': 'hello'])
        def folder = fovus('files/My Results')

        when: 'as publishDir does it: the target resolved against the publish folder, then copied through the provider'
        FileHelper.copyPath(fovus('pipelines/p-1-user/fovus-work/ab/cdef/out.txt'), folder.resolve('out [1].txt'))

        then:
        objects['files/My Results/out [1].txt'] == 'hello'
    }

    def 'a write check on a jobs/ path with a space should fail as read-only'() {
        given:
        attachClient()

        when:
        provider.checkAccess(fovus('jobs/j 1/out [1].txt'), AccessMode.WRITE)

        then:
        def e = thrown(AccessDeniedException)
        e.reason == JOBS_READ_ONLY
        e.file == 'fovus:///fovus-storage/jobs/j 1/out [1].txt'
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
        warnings[0].formattedMessage.contains('fovus:///fovus-storage/pipelines/p-1-user/out/dir stays in Fovus storage')
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
        0 * transfers._

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
        'delete of the area root'       | { Path inJobs, Path inFiles, Path onDisk -> Files.delete(inJobs.fileSystem.getPath('/fovus-storage/jobs')) }
        'move source, the area root'    | { Path inJobs, Path inFiles, Path onDisk -> Files.move(inJobs.fileSystem.getPath('/fovus-storage/jobs'), inFiles) }
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

    def 'a copy over an existing files/ object should fail, unless the delete that S3 denied came first'() {
        given:
        attachClient()
        bucketWith(['pipelines/p-1-user/out.txt': 'new', 'files/results/out.txt': 'old'])
        def logged = captureStorageLog()
        def source = fovus('pipelines/p-1-user/out.txt')
        def target = fovus('files/results/out.txt')

        when: 'no delete came first'
        FileHelper.copyPath(source, target)

        then:
        thrown(FileAlreadyExistsException)
        objects['files/results/out.txt'] == 'old'

        when: 'as PublishDir overwrites: delete the target (S3 denies it, so it stays), then copy again'
        FileHelper.deletePath(target)
        FileHelper.copyPath(source, target)

        then: 'the object stays visible until it is replaced, and then holds the new content'
        noExceptionThrown()
        objects['files/results/out.txt'] == 'new'
        logged.list.findAll { ILoggingEvent event -> event.level == Level.WARN }.size() == 1

        when: 'the denied delete allowed one replacement only'
        FileHelper.copyPath(source, target)

        then:
        thrown(FileAlreadyExistsException)
    }

    def 'the denied delete of an existing object should not hide it'() {
        given:
        attachClient()
        bucketWith(['files/results/out.txt': 'old'])

        when:
        Files.delete(fovus('files/results/out.txt'))

        then:
        Files.exists(fovus('files/results/out.txt'))
        Files.size(fovus('files/results/out.txt')) == 3
    }

    def 'publishDir overwriting a folder output in files/ should replace every file, as one object after the other'() {
        given:
        attachClient()
        bucketWith(folderOutput() + ['files/results/dir/'            : '',
                                     'files/results/dir/a.txt'       : 'old a',
                                     'files/results/dir/sub/'        : '',
                                     'files/results/dir/sub/b.txt'   : 'old b',
                                     'files/results/dir/stale.txt'   : 'stale'])
        def source = fovus('pipelines/p-1-user/out/dir')
        def target = fovus('files/results/dir')

        when: 'no delete came first'
        FileHelper.copyPath(source, target)

        then:
        thrown(FileAlreadyExistsException)

        when: 'as PublishDir overwrites: delete the target folder (S3 denies it), then copy again'
        FileHelper.deletePath(target)
        FileHelper.copyPath(source, target)

        then: 'every file holds the new content; one that is not in the output was left in place'
        noExceptionThrown()
        objectsUnder('files/') == publishedFolder() + ['files/results/dir/stale.txt': 'stale']
    }

    def 'a move of a folder over an existing files/ folder should replace it after its denied delete'() {
        given:
        attachClient()
        bucketWith(folderOutput() + ['files/results/dir/old.txt': 'old'])
        def source = fovus('pipelines/p-1-user/out/dir')
        def target = fovus('files/results/dir')

        when: 'no delete came first'
        FileHelper.movePath(source, target)

        then:
        thrown(FileAlreadyExistsException)
        objectsUnder('files/') == ['files/results/dir/old.txt': 'old']

        when: 'as PublishDir overwrites: delete the target folder (S3 denies it), then move again'
        FileHelper.deletePath(target)
        FileHelper.movePath(source, target)

        then:
        noExceptionThrown()
        objectsUnder('files/') == publishedFolder() + ['files/results/dir/old.txt': 'old']

        when: 'the denied delete allowed one replacement only'
        FileHelper.movePath(source, target)

        then:
        thrown(FileAlreadyExistsException)
    }

    def 'an upload and a CREATE_NEW write should replace an object whose delete was denied, and only then'() {
        given:
        attachClient()
        bucketWith(['files/in/x.txt': 'old', 'files/in/y.txt': 'old'])
        def local = Files.writeString(tempDir.resolve('x.txt'), 'new')

        when: 'no delete came first'
        provider.upload(local, fovus('files/in/x.txt'))

        then:
        thrown(FileAlreadyExistsException)

        when:
        Files.newOutputStream(fovus('files/in/y.txt'), StandardOpenOption.CREATE_NEW)

        then:
        thrown(FileAlreadyExistsException)

        when:
        Files.delete(fovus('files/in/x.txt'))
        Files.delete(fovus('files/in/y.txt'))
        provider.upload(local, fovus('files/in/x.txt'))
        Files.newOutputStream(fovus('files/in/y.txt'), StandardOpenOption.CREATE_NEW).withCloseable { OutputStream out -> out.write('newer'.bytes) }

        then:
        objects == ['files/in/x.txt': 'new', 'files/in/y.txt': 'newer']
    }

    def 'an upload of a folder should replace the files of a folder whose delete was denied'() {
        given:
        attachClient()
        bucketWith(['files/in/dir/': '', 'files/in/dir/a.txt': 'old'])
        def local = Files.createDirectories(tempDir.resolve('dir'))
        Files.writeString(local.resolve('a.txt'), 'new')

        when: 'no delete came first'
        provider.upload(local, fovus('files/in/dir'))

        then:
        thrown(FileAlreadyExistsException)

        when:
        FileHelper.deletePath(fovus('files/in/dir'))
        provider.upload(local, fovus('files/in/dir'))

        then:
        objects == ['files/in/dir/': '', 'files/in/dir/a.txt': 'new']
    }

    @Unroll
    def 'a write that replaces an object whose delete was denied should use up that denial: #write'() {
        given:
        attachClient()
        bucketWith(['pipelines/p-1-user/out.txt': 'new', 'files/results/out.txt': 'old'])
        def source = fovus('pipelines/p-1-user/out.txt')
        def target = fovus('files/results/out.txt')
        def local = Files.writeString(tempDir.resolve('out.txt'), 'new')

        when: 'the delete is denied, then the object is written again by a write that replaces whatever is there'
        Files.delete(target)
        action.call(source, target, local)

        then:
        objects['files/results/out.txt'] == 'new'

        when: 'a later copy that must not replace an existing object'
        Files.copy(source, target)

        then: 'it finds the object written since, not the one whose delete was denied'
        thrown(FileAlreadyExistsException)

        where:
        write                          | action
        'copy with REPLACE_EXISTING'   | { Path s, Path t, Path l -> Files.copy(s, t, StandardCopyOption.REPLACE_EXISTING) }
        'move with REPLACE_EXISTING'   | { Path s, Path t, Path l -> Files.move(s, t, StandardCopyOption.REPLACE_EXISTING) }
        'upload with REPLACE_EXISTING' | { Path s, Path t, Path l -> t.fileSystem.provider().upload(l, t, StandardCopyOption.REPLACE_EXISTING) }
        'newOutputStream'              | { Path s, Path t, Path l -> Files.newOutputStream(t).withCloseable { OutputStream out -> out.write('new'.bytes) } }
        'newByteChannel'               | { Path s, Path t, Path l -> Files.write(t, 'new'.bytes) }
    }

    def 'an abandoned write should keep the denial of the delete it did not replace'() {
        given:
        attachClient()
        bucketWith(['pipelines/p-1-user/out.txt': 'new', 'files/results/out.txt': 'old'])
        def target = fovus('files/results/out.txt')

        when: 'the delete is denied, then a write starts but is abandoned, so the old object is still there'
        Files.delete(target)
        def out = (S3UploadStream) Files.newOutputStream(target)
        out.write('partial'.bytes)
        out.abort()
        out.close()
        Files.copy(fovus('pipelines/p-1-user/out.txt'), target)

        then: 'the copy replaces the old object, as the delete asked'
        objects['files/results/out.txt'] == 'new'
    }

    def 'a folder upload that replaces a folder whose delete was denied should use up that denial'() {
        given:
        attachClient()
        bucketWith(['files/in/dir/': '', 'files/in/dir/a.txt': 'old'])
        def local = Files.createDirectories(tempDir.resolve('dir'))
        Files.writeString(local.resolve('a.txt'), 'new')

        when: 'the folder delete is denied, then the folder is uploaded again with REPLACE_EXISTING'
        FileHelper.deletePath(fovus('files/in/dir'))
        provider.upload(local, fovus('files/in/dir'), StandardCopyOption.REPLACE_EXISTING)

        then:
        objects == ['files/in/dir/': '', 'files/in/dir/a.txt': 'new']

        when: 'a later upload that must not replace it'
        provider.upload(local, fovus('files/in/dir'))

        then:
        thrown(FileAlreadyExistsException)
    }

    def 'a denied delete should be warned about once per area, and further ones at debug level'() {
        given:
        attachClient()
        bucketWith(['pipelines/p-1-user/a.txt': 'a', 'pipelines/p-1-user/b.txt': 'b', 'files/x.txt': 'x', 'files/y.txt': 'y'])
        def logged = captureStorageLog()

        when: 'the source deletes of a move, in pipelines/, and the deletes before an overwrite, in files/'
        ['pipelines/p-1-user/a.txt', 'pipelines/p-1-user/b.txt', 'files/x.txt', 'files/y.txt'].each { String key -> Files.delete(fovus(key)) }

        then:
        def warnings = logged.list.findAll { ILoggingEvent event -> event.level == Level.WARN }*.formattedMessage
        warnings == ["[FOVUS] Fovus storage credentials don't allow delete on pipelines/p-1-user/a.txt (write token) -- " +
                             "fovus:///fovus-storage/pipelines/p-1-user/a.txt stays in Fovus storage: the direct-mode credentials cannot delete. " +
                             "A later write to the same path replaces it. (Further ones in pipelines/ are logged at debug level.)",
                     "[FOVUS] Fovus storage credentials don't allow delete on files/x.txt (write token) -- " +
                             "fovus:///fovus-storage/files/x.txt stays in Fovus storage: the direct-mode credentials cannot delete. " +
                             "A later write to the same path replaces it. (Further ones in files/ are logged at debug level.)"]
        def debug = logged.list.findAll { ILoggingEvent event -> event.level == Level.DEBUG }*.formattedMessage
        debug.contains("[FOVUS] Fovus storage credentials don't allow delete on pipelines/p-1-user/b.txt (write token) -- " +
                               "fovus:///fovus-storage/pipelines/p-1-user/b.txt stays in Fovus storage")
        debug.contains("[FOVUS] Fovus storage credentials don't allow delete on files/y.txt (write token) -- " +
                               "fovus:///fovus-storage/files/y.txt stays in Fovus storage")
        !logged.list.any { ILoggingEvent event -> event.formattedMessage.contains('SECRET-BODY') }
    }

    def 'a delete S3 allows should leave nothing to replace'() {
        given:
        attachClient()
        bucketWith(['pipelines/p-1-user/out.txt': 'new', 'files/results/out.txt': 'old'], true, true)
        def target = fovus('files/results/out.txt')

        when: 'the object is deleted for real, and then somebody else writes it again'
        Files.delete(target)
        objects['files/results/out.txt'] = 'other'
        Files.copy(fovus('pipelines/p-1-user/out.txt'), target)

        then: 'that is a plain existing target, not the one whose delete was denied'
        thrown(FileAlreadyExistsException)
        objects['files/results/out.txt'] == 'other'
    }

    def 'the root of a writable folder should be deleted as the folder it is'() {
        given:
        attachClient()
        bucketWith(['files/a.txt': 'a'])

        when: 'the marker delete is denied by S3, and left in place'
        Files.delete(fovus('files'))

        then:
        noExceptionThrown()
        calls == ['DELETE files/']
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
