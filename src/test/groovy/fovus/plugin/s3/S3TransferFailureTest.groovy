package fovus.plugin.s3

import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.exception.SdkClientException
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
    S3Transfers transfers = Mock()
    FovusS3Client client = new FovusS3Client(s3, s3, transfers, 'bucket', 'pipelines/p-1-user/', null)

    def 'a failed download should leave no file and no temp file behind'() {
        given:
        def directory = tempDir.resolve('downloads')
        def target = directory.resolve('out.bin')
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(10L).build()

        when:
        client.downloadFile(KEY, target)

        then:
        1 * transfers.downloadFile(KEY, _ as Path) >> { String key, Path destination ->
            Files.write(destination, 'part'.bytes)
            throw FovusS3ClientTest.s3Error(500, 'InternalError')
        }
        def e = thrown(IOException)
        e.message == "S3 read failed on ${KEY}: InternalError (HTTP 500, request req-1)".toString()
        e.cause == null
        !Files.exists(target)
        Files.list(directory).withCloseable { it.count() } == 0
    }

    def 'an interrupted download should leave no file behind'() {
        given:
        def directory = tempDir.resolve('downloads')
        def target = directory.resolve('out.bin')
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(10L).build()

        when:
        client.downloadFile(KEY, target)

        then: 'as the transfers report an interrupted wait: the flag kept, an InterruptedIOException thrown'
        1 * transfers.downloadFile(KEY, _ as Path) >> { String key, Path destination ->
            Files.write(destination, 'part'.bytes)
            Thread.currentThread().interrupt()
            throw new InterruptedIOException('Interrupted while waiting for the S3 read of ' + key)
        }
        thrown(InterruptedIOException)
        Thread.interrupted()
        !Files.exists(target)
        Files.list(directory).withCloseable { it.count() } == 0
    }

    def 'a download of a missing object should fail as missing, with no temp file and no transfer'() {
        given:
        def directory = tempDir.resolve('downloads')
        s3.headObject(_ as HeadObjectRequest) >> { throw FovusS3ClientTest.s3Error(404, 'NotFound') }

        when:
        client.downloadFile(KEY, directory.resolve('out.bin'))

        then:
        thrown(NoSuchFileException)
        0 * transfers._
        !Files.exists(directory)
    }

    @Requires({ FileSystems.default.supportedFileAttributeViews().contains('posix') })
    def 'a #kind download should get the default permissions rather than the owner-only mode of a temp file'() {
        given:
        def target = tempDir.resolve('out.bin')
        def reference = Files.createFile(tempDir.resolve('reference.bin'))
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(size).build()
        // the SDK writes into the destination it is given, which the client created
        transfers.downloadFile(KEY, _ as Path) >> { String key, Path destination -> if (size > 0) Files.write(destination, new byte[(int) size]) }

        when:
        client.downloadFile(KEY, target)

        then:
        Files.size(target) == size
        Files.getPosixFilePermissions(target) == Files.getPosixFilePermissions(reference)

        where:
        kind    | size
        'small' | 5L
        'empty' | 0L
    }

    def 'a denied CopyObject should fall back to a streamed copy'() {
        given:
        def upload = new RecordingUploadStream()

        when:
        client.copy(SOURCE, KEY, 5L)

        then:
        1 * s3.copyObject(_ as CopyObjectRequest) >> { throw FovusS3ClientTest.s3Error(403, 'AccessDenied') }
        1 * s3.getObject({ GetObjectRequest r -> r.key() == SOURCE }) >> responseStream(new ByteArrayInputStream('hello'.bytes))
        1 * transfers.newUploadStream(KEY) >> upload
        upload.published
        !upload.aborted
        upload.text == 'hello'
    }

    def 'a CopyObject failure other than access denied should not fall back'() {
        when:
        client.copy(SOURCE, KEY, 5L)

        then:
        thrown(IOException)
        1 * s3.copyObject(_ as CopyObjectRequest) >> { throw FovusS3ClientTest.s3Error(500, 'InternalError') }
        0 * s3.getObject(_)
        0 * transfers._
    }

    def 'an object too large for CopyObject should be copied as a stream'() {
        given:
        def upload = new RecordingUploadStream()

        when:
        client.copy(SOURCE, KEY, FovusS3Client.MAX_COPY_OBJECT_SIZE + 1)

        then:
        0 * s3.copyObject(_)
        1 * s3.getObject({ GetObjectRequest r -> r.key() == SOURCE }) >> responseStream(new ByteArrayInputStream('hello'.bytes))
        1 * transfers.newUploadStream(KEY) >> upload
        upload.text == 'hello'
        upload.published
    }

    def 'a streamed copy whose source fails part way with #failure.class.simpleName should abort the upload'() {
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
                if (served >= 1000) throw error
                final count = Math.min(length, 1000 - served)
                served += count
                return count
            }
        }
        S3UploadStream upload = Mock()

        when:
        client.copy(SOURCE, KEY, FovusS3Client.MAX_COPY_OBJECT_SIZE + 1)

        then:
        def thrown = thrown(IOException)
        thrown.message == expectedMessage
        thrown.cause == null
        1 * s3.getObject(_ as GetObjectRequest) >> responseStream(failing)
        1 * transfers.newUploadStream(KEY) >> upload
        (1.._) * upload.write(_, _, _)
        1 * upload.abort()
        0 * upload.close()

        where:
        failure                                                                   | expectedMessage
        new IOException('connection reset')                                       | 'connection reset'
        SdkClientException.create('Unable to execute HTTP request: timed out')    | "S3 read failed on ${SOURCE}: SdkClientException".toString()
    }

    def 'newOutputStream should refuse #key outside the pipeline before calling S3'() {
        when:
        client.newOutputStream(key)

        then:
        thrown(AccessDeniedException)
        0 * s3._
        0 * transfers._

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
        0 * transfers._

        where:
        key << OUTSIDE_KEYS
    }

    def 'copy should refuse the target #key outside the pipeline before calling S3'() {
        when:
        client.copy(SOURCE, key, 5L)

        then:
        thrown(AccessDeniedException)
        0 * s3._
        0 * transfers._

        where:
        key << OUTSIDE_KEYS
    }

    def 'copy should treat the source #key outside the pipeline as missing before calling S3'() {
        when:
        client.copy(key, KEY, 5L)

        then:
        thrown(NoSuchFileException)
        0 * s3._
        0 * transfers._

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
        0 * transfers._
        !Files.exists(target.parent)

        where:
        key << OUTSIDE_KEYS
    }

    private static ResponseInputStream<GetObjectResponse> responseStream(InputStream input) {
        return new ResponseInputStream<GetObjectResponse>(GetObjectResponse.builder().build(), AbortableInputStream.create(input))
    }
}
