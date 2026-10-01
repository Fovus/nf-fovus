package fovus.plugin.s3

import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.services.s3.S3Client
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Tag
import spock.lang.TempDir

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

@Tag('integration')
class FovusS3ClientIT extends Specification {

    static final long PART = TransferManagerTransfers.MIN_PART_SIZE

    @Shared MinIOContainer minio
    @Shared S3Client s3
    @Shared TransferManagerTransfers transfers
    @Shared CountingInterceptor requests = new CountingInterceptor()

    @TempDir
    Path tempDir

    FovusS3Client client
    String base

    def setupSpec() {
        minio = MinioSupport.start()
        s3 = MinioSupport.s3Client(minio, requests)
        transfers = MinioSupport.transfers(minio, PART, requests)
    }

    def cleanupSpec() {
        transfers?.close()
        s3?.close()
        minio?.stop()
    }

    def setup() {
        client = MinioSupport.fovusClient(s3, transfers)
        base = "${MinioSupport.PREFIX}${UUID.randomUUID()}/".toString()
        requests.reset()
    }

    private static byte[] randomBytes(int size) {
        final bytes = new byte[size]
        new Random(42).nextBytes(bytes)
        return bytes
    }

    private byte[] read(String key) {
        return client.getObject(key).withCloseable { it.readAllBytes() }
    }

    def 'a small stream should become one PutObject'() {
        given:
        def key = base + 'small.txt'

        when:
        client.newOutputStream(key).withCloseable { it.write('hello'.bytes) }

        then:
        read(key) == 'hello'.bytes
        requests.count('PutObjectRequest') == 1
        requests.count('CreateMultipartUploadRequest') == 0
    }

    /** Write {@code data} to {@code key} through a stream, 1 MiB at a time: the length is not known up front. */
    private void stream(String key, byte[] data) {
        client.newOutputStream(key).withCloseable { out ->
            for (int offset = 0; offset < data.length; offset += 1024 * 1024) {
                out.write(data, offset, Math.min(1024 * 1024, data.length - offset))
            }
        }
    }

    def 'a stream of unknown length over two parts should become a multipart upload'() {
        given:
        def key = base + 'large.bin'
        def data = randomBytes((int) (2 * PART + 1024 * 1024))

        when:
        stream(key, data)

        then:
        read(key) == data
        requests.count('CreateMultipartUploadRequest') == 1
        requests.count('UploadPartRequest') == 3
        requests.count('CompleteMultipartUploadRequest') == 1
    }

    def 'a stream of exactly one part should become one PutObject'() {
        given:
        def key = base + 'one-part.bin'
        def data = randomBytes((int) PART)

        when:
        stream(key, data)

        then:
        read(key) == data
        requests.count('PutObjectRequest') == 1
        requests.count('CreateMultipartUploadRequest') == 0
    }

    def 'an aborted stream should leave no object'() {
        given:
        def key = base + 'aborted.bin'
        def out = client.newOutputStream(key)
        out.write(randomBytes((int) (2 * PART + 1)))

        when:
        out.abort()
        out.close()
        // a window for a wrong CompleteMultipartUpload to show: a part the SDK was still sending ends without one
        Thread.sleep(1000)

        then:
        client.head(key) == null
        requests.count('CompleteMultipartUploadRequest') == 0
    }

    def 'files should upload, in parts when large, and download whole'() {
        given:
        def small = Files.write(tempDir.resolve('small.txt'), 'hello'.bytes)
        def large = Files.write(tempDir.resolve('large.bin'), randomBytes((int) (3 * PART)))

        when:
        client.uploadFile(small, base + 'small.txt')
        client.uploadFile(large, base + 'large.bin')

        then: 'three parts of 5 MiB'
        requests.count('CreateMultipartUploadRequest') == 1
        requests.count('UploadPartRequest') == 3

        when:
        requests.reset()
        client.downloadFile(base + 'small.txt', tempDir.resolve('out/small.txt'))
        client.downloadFile(base + 'large.bin', tempDir.resolve('out/large.bin'))

        then: 'one GET each'
        Files.readAllBytes(tempDir.resolve('out/small.txt')) == Files.readAllBytes(small)
        Files.readAllBytes(tempDir.resolve('out/large.bin')) == Files.readAllBytes(large)
        requests.count('GetObjectRequest') == 2
    }

    def 'copy should duplicate an object inside the pipeline'() {
        given:
        client.putObject(base + 'a.txt', 'a'.bytes)

        when:
        client.copy(base + 'a.txt', base + 'b.txt', 1L)

        then:
        read(base + 'b.txt') == 'a'.bytes
    }

    def 'copy should stream the object when it is too large for CopyObject'() {
        given:
        client.putObject(base + 'a.txt', 'a'.bytes)

        when:
        client.copy(base + 'a.txt', base + 'b.txt', FovusS3Client.MAX_COPY_OBJECT_SIZE + 1)

        then:
        read(base + 'b.txt') == 'a'.bytes
        requests.count('CopyObjectRequest') == 0
    }

    def 'a read channel should seek within an object'() {
        given:
        client.putObject(base + 'seek.txt', '0123456789'.bytes)
        def channel = new S3ReadChannel(client, base + 'seek.txt', 10L)
        def buffer = ByteBuffer.allocate(3)

        when:
        channel.position(4)
        channel.read(buffer)

        then:
        new String(buffer.array()) == '456'
        channel.position() == 7

        cleanup:
        channel.close()
    }
}
