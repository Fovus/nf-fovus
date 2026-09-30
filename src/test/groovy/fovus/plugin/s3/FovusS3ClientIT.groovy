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

    @Shared MinIOContainer minio
    @Shared S3Client s3
    @Shared CountingInterceptor requests = new CountingInterceptor()

    @TempDir
    Path tempDir

    FovusS3Client client
    String base

    def setupSpec() {
        minio = MinioSupport.start()
        s3 = MinioSupport.s3Client(minio, requests)
    }

    def cleanupSpec() {
        s3?.close()
        minio?.stop()
    }

    def setup() {
        client = MinioSupport.fovusClient(s3)
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

    def 'a large stream should become a multipart upload'() {
        given:
        def key = base + 'large.bin'
        def data = randomBytes(3 * FovusS3Client.MIN_PART_SIZE + 17)

        when:
        client.newOutputStream(key).withCloseable { out ->
            for (int offset = 0; offset < data.length; offset += 1024 * 1024) {
                out.write(data, offset, Math.min(1024 * 1024, data.length - offset))
            }
        }

        then:
        read(key) == data
        requests.count('CreateMultipartUploadRequest') == 1
        requests.count('UploadPartRequest') == 4
    }

    def 'a stream that is never closed should leave no object'() {
        given:
        def key = base + 'interrupted.bin'
        def out = client.newOutputStream(key)

        when:
        out.write(randomBytes(2 * FovusS3Client.MIN_PART_SIZE + 1))

        then:
        client.head(key) == null

        cleanup:
        out.abort()
    }

    def 'files should upload and download whole, in parts when large'() {
        given:
        def small = Files.write(tempDir.resolve('small.txt'), 'hello'.bytes)
        def large = Files.write(tempDir.resolve('large.bin'), randomBytes(3 * FovusS3Client.MIN_PART_SIZE + 1))

        when:
        client.uploadFile(small, base + 'small.txt')
        client.uploadFile(large, base + 'large.bin')
        requests.reset()
        client.downloadFile(base + 'small.txt', tempDir.resolve('out/small.txt'))
        client.downloadFile(base + 'large.bin', tempDir.resolve('out/large.bin'))

        then:
        Files.readAllBytes(tempDir.resolve('out/small.txt')) == Files.readAllBytes(small)
        Files.readAllBytes(tempDir.resolve('out/large.bin')) == Files.readAllBytes(large)
        requests.count('GetObjectRequest') == 1 + 4
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
