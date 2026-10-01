package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusS3ClientTest
import fovus.plugin.s3.S3Entry
import nextflow.file.FileHelper
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CommonPrefix
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import spock.lang.Specification

import java.nio.file.AccessDeniedException
import java.nio.file.Files
import java.nio.file.Path
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

    S3Client s3 = Mock()
    FovusFileSystemProvider provider = new FovusFileSystemProvider()

    def setup() {
        // As in WorkDirStorageFactoryTest: FileHelper.asPath(URI) then resolves fovus:// paths as it does under
        // Nextflow, creating one file system per area, all on this provider
        FileHelper.providersMap['fovus'] = provider
    }

    def cleanup() {
        FileHelper.providersMap.remove('fovus')
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
