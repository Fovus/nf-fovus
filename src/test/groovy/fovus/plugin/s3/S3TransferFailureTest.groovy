package fovus.plugin.s3

import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.exception.AbortedException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.AccessDeniedException
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path

class S3TransferFailureTest extends Specification {

    static final String KEY = 'pipelines/p-1-user/fovus-work/ab/cdef/out.bin'
    static final String SOURCE = 'pipelines/p-1-user/fovus-work/ab/cdef/in.bin'
    /** Keys that a write or read must treat as outside the pipeline: another pipeline, or a '..' escape from this one. */
    static final List<String> OUTSIDE_KEYS = [FovusS3ClientTest.OTHER_PIPELINE_KEY] + FovusS3ClientTest.DOT_SEGMENT_KEYS

    @TempDir
    Path tempDir

    S3Client s3 = Mock()
    FovusS3Client client = new FovusS3Client(s3, s3, 'bucket', 'pipelines/p-1-user/', null, FovusS3Client.MIN_PART_SIZE)

    def 'a failed part upload should abort the upload and publish nothing'() {
        given:
        def out = client.newOutputStream(KEY)
        def part = new byte[FovusS3Client.MIN_PART_SIZE]

        when:
        out.write(part)
        try {
            out.write(part)
        }
        catch (IOException ignored) {
        }
        out.close()

        then:
        1 * s3.createMultipartUpload(_ as CreateMultipartUploadRequest) >> CreateMultipartUploadResponse.builder().uploadId('u-1').build()
        1 * s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> UploadPartResponse.builder().eTag('e1').build()
        1 * s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> { throw FovusS3ClientTest.s3Error(500, 'InternalError') }
        1 * s3.abortMultipartUpload({ AbortMultipartUploadRequest r -> r.uploadId() == 'u-1' })
        0 * s3.completeMultipartUpload(_)
        0 * s3.putObject(_, _)
    }

    def 'a failed ranged download should leave no file behind'() {
        given:
        def target = tempDir.resolve('out.bin')
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(3L * FovusS3Client.MIN_PART_SIZE).build()
        s3.getObjectAsBytes(_ as GetObjectRequest) >> { throw FovusS3ClientTest.s3Error(500, 'InternalError') }

        when:
        client.downloadFile(KEY, target)

        then:
        thrown(IOException)
        !Files.exists(target)
        Files.list(tempDir).withCloseable { it.count() } == 0
    }

    def 'a failed part of a file upload should abort the upload and publish nothing'() {
        given:
        def file = Files.write(tempDir.resolve('big.bin'), new byte[FovusS3Client.MIN_PART_SIZE + 1])

        when:
        client.uploadFile(file, KEY)

        then:
        thrown(IOException)
        1 * s3.createMultipartUpload(_ as CreateMultipartUploadRequest) >> CreateMultipartUploadResponse.builder().uploadId('u-1').build()
        1 * s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> UploadPartResponse.builder().eTag('e1').build()
        1 * s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> { throw FovusS3ClientTest.s3Error(500, 'InternalError') }
        1 * s3.abortMultipartUpload({ AbortMultipartUploadRequest r -> r.uploadId() == 'u-1' })
        0 * s3.completeMultipartUpload(_)
    }

    def 'an interrupted ranged download should leave no file behind'() {
        given:
        def target = tempDir.resolve('out.bin')
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(3L * FovusS3Client.MIN_PART_SIZE).build()
        s3.getObjectAsBytes(_ as GetObjectRequest) >> { Thread.sleep(200); throw FovusS3ClientTest.s3Error(500, 'InternalError') }

        when:
        Thread.currentThread().interrupt()
        client.downloadFile(KEY, target)

        then:
        thrown(InterruptedIOException)
        Thread.interrupted()
        !Files.exists(target)
        Files.list(tempDir).withCloseable { it.count() } == 0
    }

    def 'a denied CopyObject should fall back to a streamed copy'() {
        when:
        client.copy(SOURCE, KEY, 5L)

        then:
        1 * s3.copyObject(_ as CopyObjectRequest) >> { throw FovusS3ClientTest.s3Error(403, 'AccessDenied') }
        1 * s3.getObject({ GetObjectRequest r -> r.key() == SOURCE }) >> responseStream(new ByteArrayInputStream('hello'.bytes))
        1 * s3.putObject({ PutObjectRequest r -> r.key() == KEY }, { RequestBody b -> b.contentLength() == 5L })
        0 * s3.abortMultipartUpload(_)
    }

    def 'a CopyObject failure other than access denied should not fall back'() {
        when:
        client.copy(SOURCE, KEY, 5L)

        then:
        thrown(IOException)
        1 * s3.copyObject(_ as CopyObjectRequest) >> { throw FovusS3ClientTest.s3Error(500, 'InternalError') }
        0 * s3.getObject(_)
        0 * s3.putObject(_, _)
    }

    def 'a streamed copy whose source fails part way with #failure.class.simpleName should abort the upload and publish nothing'() {
        given:
        def error = failure
        def failing = new InputStream() {
            int served = 0

            @Override
            int read() {
                throw new UnsupportedOperationException()
            }

            @Override
            int read(byte[] bytes, int offset, int length) {
                if (served >= FovusS3Client.MIN_PART_SIZE) throw error
                final count = Math.min(length, FovusS3Client.MIN_PART_SIZE - served)
                served += count
                return count
            }
        }

        when:
        client.copy(SOURCE, KEY, FovusS3Client.MAX_COPY_OBJECT_SIZE + 1)

        then:
        def thrown = thrown(IOException)
        thrown.message == expectedMessage
        thrown.cause == null
        0 * s3.copyObject(_)
        1 * s3.getObject(_ as GetObjectRequest) >> responseStream(failing)
        1 * s3.createMultipartUpload(_ as CreateMultipartUploadRequest) >> CreateMultipartUploadResponse.builder().uploadId('u-1').build()
        1 * s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> UploadPartResponse.builder().eTag('e1').build()
        1 * s3.abortMultipartUpload({ AbortMultipartUploadRequest r -> r.uploadId() == 'u-1' })
        0 * s3.completeMultipartUpload(_)
        0 * s3.putObject(_, _)

        where:
        failure                                                                   | expectedMessage
        new IOException('connection reset')                                       | 'connection reset'
        SdkClientException.create('Unable to execute HTTP request: timed out')    | "S3 read failed on ${SOURCE}: SdkClientException".toString()
    }

    def 'a single-GET download that fails part way with #failure.class.simpleName should leave no file behind'() {
        given:
        def directory = tempDir.resolve('downloads')
        def target = directory.resolve('out.bin')
        def error = failure
        def failing = new InputStream() {
            int served = 0

            @Override
            int read() {
                throw new UnsupportedOperationException()
            }

            @Override
            int read(byte[] bytes, int offset, int length) {
                if (served >= 4) throw error
                served += 4
                return 4
            }
        }
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(10L).build()
        s3.getObject(_ as GetObjectRequest) >> responseStream(failing)

        when:
        client.downloadFile(KEY, target)

        then:
        def thrown = thrown(IOException)
        thrown.message == "S3 read failed on ${KEY}: ${failure.class.simpleName}".toString()
        thrown.cause == null
        !Files.exists(target)
        Files.list(directory).withCloseable { it.count() } == 0

        where:
        failure << [SdkClientException.create('Unable to execute HTTP request: timed out SECRET-BODY'),
                    AbortedException.create('Thread was interrupted SECRET-BODY')]
    }

    @Requires({ FileSystems.default.supportedFileAttributeViews().contains('posix') })
    def 'a #kind download should get the default permissions rather than the owner-only mode of a temp file'() {
        given:
        def target = tempDir.resolve('out.bin')
        def reference = Files.createFile(tempDir.resolve('reference.bin'))
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(size).build()
        s3.getObject(_ as GetObjectRequest) >> responseStream(new ByteArrayInputStream(new byte[(int) size]))
        s3.getObjectAsBytes(_ as GetObjectRequest) >> ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[FovusS3Client.MIN_PART_SIZE])

        when:
        client.downloadFile(KEY, target)

        then:
        Files.size(target) == size
        Files.getPosixFilePermissions(target) == Files.getPosixFilePermissions(reference)

        where:
        kind     | size
        'single' | 5L
        'ranged' | 3L * FovusS3Client.MIN_PART_SIZE
    }

    def 'newOutputStream should refuse #key outside the pipeline before calling S3'() {
        when:
        client.newOutputStream(key)

        then:
        thrown(AccessDeniedException)
        0 * s3._

        where:
        key << OUTSIDE_KEYS
    }

    def 'uploadFile should refuse #key outside the pipeline before calling S3'() {
        given:
        def file = Files.write(tempDir.resolve('small.txt'), 'hello'.bytes)

        when:
        client.uploadFile(file, key)

        then:
        thrown(AccessDeniedException)
        0 * s3._

        where:
        key << OUTSIDE_KEYS
    }

    def 'copy should refuse the target #key outside the pipeline before calling S3'() {
        when:
        client.copy(SOURCE, key, 5L)

        then:
        thrown(AccessDeniedException)
        0 * s3._

        where:
        key << OUTSIDE_KEYS
    }

    def 'copy should treat the source #key outside the pipeline as missing before calling S3'() {
        when:
        client.copy(key, KEY, 5L)

        then:
        thrown(NoSuchFileException)
        0 * s3._

        where:
        key << OUTSIDE_KEYS
    }

    def 'downloadFile should treat #key outside the pipeline as missing before calling S3'() {
        given:
        def target = tempDir.resolve('downloads/out.bin')

        when:
        client.downloadFile(key, target)

        then:
        thrown(NoSuchFileException)
        0 * s3._
        !Files.exists(target.parent)

        where:
        key << OUTSIDE_KEYS
    }

    private static ResponseInputStream<GetObjectResponse> responseStream(InputStream input) {
        return new ResponseInputStream<GetObjectResponse>(GetObjectResponse.builder().build(), AbortableInputStream.create(input))
    }
}
