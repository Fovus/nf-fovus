package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusS3ClientTest
import fovus.plugin.s3.RecordingUploadStream
import fovus.plugin.s3.S3Transfers
import nextflow.file.FileHelper
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import java.nio.file.FileSystem
import java.nio.file.FileSystemNotFoundException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.spi.FileSystemProvider

/**
 * Staging an input from another file system (https://, ftp://, the user's own s3://) into the pipelines/ area, or
 * publishing one into files/: a source that fails part way must never leave a truncated object behind, since
 * FilePorter would reuse it.
 */
class ForeignSourceUploadTest extends Specification {

    static final String KEY = 'pipelines/p-1-user/fovus-work/stage-1/ab/cdef/in.bin'

    @TempDir
    Path tempDir

    S3Client s3 = Mock()
    S3Transfers transfers = Mock()
    /** The upload of the staged or published file: published on close, discarded on abort. */
    RecordingUploadStream upload = new RecordingUploadStream()
    FovusS3Client client = new FovusS3Client(s3, s3, transfers, 'bucket', 'pipelines/p-1-user/', null)
    FovusFileSystem fs = StorageTestSupport.fileSystem(client)
    Path target = fs.getPath('/fovus-storage/' + KEY)

    /** A file on a file system other than the default one (https://, s3://), whose size and content each feature sets. */
    FileSystemProvider sourceProvider = Mock()
    FileSystem sourceFileSystem = Mock()
    Path source = Mock()
    BasicFileAttributes sourceAttributes = Stub()
    long reportedSize
    Closure<InputStream> content

    def setup() {
        // the target does not exist yet
        s3.headObject(_ as HeadObjectRequest) >> { throw FovusS3ClientTest.s3Error(404, 'NotFound') }
        s3.listObjectsV2(_ as ListObjectsV2Request) >> ListObjectsV2Response.builder().keyCount(0).build()

        source.getFileSystem() >> sourceFileSystem
        sourceFileSystem.provider() >> sourceProvider
        sourceAttributes.size() >> { reportedSize }
        sourceAttributes.isRegularFile() >> true
        sourceProvider.readAttributes(source, BasicFileAttributes, *_) >> sourceAttributes
        sourceProvider.readAttributesIfExists(source, BasicFileAttributes, *_) >> sourceAttributes
        sourceProvider.newInputStream(source, *_) >> { content.call() }
    }

    /** A path in another area of the provider that {@link #fs} belongs to. */
    private Path inArea(String area, String key) {
        final uri = URI.create("fovus:///fovus-storage/${area}")
        FileSystem areaFileSystem
        try {
            areaFileSystem = fs.provider().getFileSystem(uri)
        }
        catch (FileSystemNotFoundException ignored) {
            areaFileSystem = fs.provider().newFileSystem(uri, [:])
        }
        return areaFileSystem.getPath("/fovus-storage/${area}/${key}")
    }

    private Path foreignFile(long size, Closure<InputStream> stream) {
        reportedSize = size
        content = stream
        return source
    }

    /** Serves {@code count} bytes of {@code value}, then throws {@code error} (or ends, when it is null). */
    private static InputStream failingAfter(int count, Exception error, byte value = 7) {
        return new InputStream() {
            int served = 0

            @Override
            int read() {
                final bytes = new byte[1]
                return read(bytes, 0, 1) < 0 ? -1 : bytes[0]
            }

            @Override
            int read(byte[] bytes, int offset, int length) {
                if (served >= count) {
                    if (error != null) throw error
                    return -1
                }
                final chunk = Math.min(length, count - served)
                Arrays.fill(bytes, offset, offset + chunk, value)
                served += chunk
                return chunk
            }
        }
    }

    def 'the pipelines and files areas should take uploads from any file system, and jobs none'() {
        given:
        def foreign = foreignFile(1L) { new ByteArrayInputStream(new byte[1]) }
        def local = Files.writeString(tempDir.resolve('in.txt'), 'x')

        and: 'the other areas of the same provider'
        def provider = fs.provider()
        def filesTarget = inArea('files', 'x')
        def jobsTarget = inArea('jobs', 'x')

        expect:
        provider.canUpload(foreign, target)
        provider.canUpload(local, target)
        provider.canUpload(foreign, filesTarget)
        provider.canUpload(local, filesTarget)
        !provider.canUpload(foreign, jobsTarget)
        !provider.canUpload(local, jobsTarget)
    }

    def 'a complete remote source should be uploaded into files/ too'() {
        given:
        def source = foreignFile(1000L) { failingAfter(1000, null) }

        when:
        FileHelper.copyPath(source, inArea('files', 'in/in.bin'))

        then:
        1 * transfers.newUploadStream('files/in/in.bin') >> upload
        upload.published
        upload.bytes.size() == 1000
    }

    def 'a remote source failing into files/ should publish nothing'() {
        given:
        def source = foreignFile(2000L) { failingAfter(1000, new IOException('connection reset')) }

        when:
        FileHelper.copyPath(source, inArea('files', 'in/in.bin'))

        then:
        def e = thrown(IOException)
        e.message == 'connection reset'
        1 * transfers.newUploadStream('files/in/in.bin') >> upload
        upload.aborted
        !upload.published
    }

    @Unroll
    def 'a remote source failing after #served bytes should publish nothing, discard the upload and fail'() {
        given:
        def source = foreignFile(2L * served + 10) { failingAfter(served, new IOException('connection reset')) }

        when:
        FileHelper.copyPath(source, target)

        then:
        def e = thrown(IOException)
        e.message == 'connection reset'
        1 * transfers.newUploadStream(KEY) >> upload
        upload.aborted
        !upload.published
        upload.bytes.size() == served

        where:
        served << [0, 1000, 3 * 1024 * 1024]
    }

    def 'a runtime failure of the remote source should also publish nothing'() {
        given:
        def source = foreignFile(2000L) { failingAfter(1000, new IllegalStateException('stream broke')) }

        when:
        FileHelper.copyPath(source, target)

        then:
        thrown(IllegalStateException)
        1 * transfers.newUploadStream(KEY) >> upload
        upload.aborted
        !upload.published
    }

    def 'a remote source that ends before its known size should publish nothing and fail'() {
        given:
        def source = foreignFile(2000L) { failingAfter(1000, null) }

        when:
        FileHelper.copyPath(source, target)

        then:
        def e = thrown(IOException)
        e.message.contains('1000 of 2000 bytes')
        1 * transfers.newUploadStream(KEY) >> upload
        upload.aborted
        !upload.published
    }

    @Unroll
    def 'a complete remote source should be uploaded once when its size is #reported'() {
        given:
        def source = foreignFile(reported) { failingAfter(1000, null) }

        when:
        FileHelper.copyPath(source, target)

        then:
        1 * transfers.newUploadStream(KEY) >> upload
        upload.published
        !upload.aborted
        upload.bytes.size() == 1000

        where:
        reported << [1000L, -1L]
    }
}
