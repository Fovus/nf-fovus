package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusS3ClientTest
import nextflow.file.FileHelper
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.spi.FileSystemProvider

/**
 * Staging an input from another file system (https://, ftp://, the user's own s3://) into the pipelines/ area:
 * a source that fails part way must never leave a truncated object behind, since FilePorter would reuse it.
 */
class ForeignSourceUploadTest extends Specification {

    static final String KEY = 'pipelines/p-1-user/fovus-work/stage-1/ab/cdef/in.bin'
    static final int PART = FovusS3Client.MIN_PART_SIZE

    @TempDir
    Path tempDir

    S3Client s3 = Mock()
    FovusS3Client client = new FovusS3Client(s3, s3, 'bucket', 'pipelines/p-1-user/', null, PART)
    FovusFileSystem fs = PipelinesTestSupport.fileSystem(client)
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

    def 'the pipelines area should take uploads from any file system; files/ keeps its current rule'() {
        given:
        def foreign = foreignFile(1L) { new ByteArrayInputStream(new byte[1]) }
        def local = Files.writeString(tempDir.resolve('in.txt'), 'x')
        def files = fs.getPath('/fovus-storage/files/x')

        expect:
        fs.provider().canUpload(foreign, target)
        fs.provider().canUpload(local, target)
        !fs.provider().canUpload(foreign, files)
        fs.provider().canUpload(local, files)
    }

    @Unroll
    def 'a remote source failing after #served bytes should publish nothing, abort any multipart upload and fail'() {
        given:
        def source = foreignFile(2L * PART + 10) { failingAfter(served, new IOException('connection reset')) }

        when:
        FileHelper.copyPath(source, target)

        then:
        def e = thrown(IOException)
        e.message == 'connection reset'
        multipart * s3.createMultipartUpload(_ as CreateMultipartUploadRequest) >> CreateMultipartUploadResponse.builder().uploadId('u-1').build()
        (served.intdiv(PART)) * s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> UploadPartResponse.builder().eTag('e').build()
        multipart * s3.abortMultipartUpload({ AbortMultipartUploadRequest r -> r.uploadId() == 'u-1' && r.key() == KEY })
        0 * s3.putObject(_, _)
        0 * s3.completeMultipartUpload(_)

        where:
        served       | multipart
        1000         | 0
        PART + 1000  | 1
    }

    def 'a runtime failure of the remote source should also publish nothing'() {
        given:
        def source = foreignFile(2000L) { failingAfter(1000, new IllegalStateException('stream broke')) }

        when:
        FileHelper.copyPath(source, target)

        then:
        thrown(IllegalStateException)
        0 * s3.putObject(_, _)
        0 * s3.completeMultipartUpload(_)
    }

    def 'a remote source that ends before its known size should publish nothing and fail'() {
        given:
        def source = foreignFile(2000L) { failingAfter(1000, null) }

        when:
        FileHelper.copyPath(source, target)

        then:
        def e = thrown(IOException)
        e.message.contains('1000 of 2000 bytes')
        0 * s3.putObject(_, _)
        0 * s3.completeMultipartUpload(_)
    }

    @Unroll
    def 'a complete remote source should be uploaded once when its size is #reported'() {
        given:
        def source = foreignFile(reported) { failingAfter(1000, null) }

        when:
        FileHelper.copyPath(source, target)

        then:
        1 * s3.putObject({ PutObjectRequest r -> r.key() == KEY }, { RequestBody b -> b.contentLength() == 1000L })
        0 * s3.abortMultipartUpload(_)

        where:
        reported << [1000L, -1L]
    }
}
