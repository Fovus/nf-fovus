package fovus.plugin.s3

import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import spock.lang.Specification

import java.nio.ByteBuffer

class S3ReadChannelTest extends Specification {

    static final String KEY = 'pipelines/p-1-user/fovus-work/ab/cdef/.command.log'
    static final int BLOCK = 64 * 1024

    /** Serves {@code bytes} a few at a time, as a network stream may. */
    private static InputStream trickling(byte[] bytes) {
        return new FilterInputStream(new ByteArrayInputStream(bytes)) {
            @Override
            int read(byte[] buffer, int offset, int length) {
                return super.read(buffer, offset, Math.min(length, 7))
            }
        }
    }

    private static byte[] content(int size) {
        final bytes = new byte[size]
        for (int i = 0; i < size; i++) bytes[i] = (byte) i
        return bytes
    }

    def 'a read should fill the buffer up to one block even when the stream returns short reads'() {
        given:
        def bytes = content(BLOCK + 100)
        def client = Stub(FovusS3Client) { getObject(KEY, 0L) >> trickling(bytes) }
        def channel = new S3ReadChannel(client, KEY, bytes.length)
        def buffer = ByteBuffer.allocate(2 * BLOCK)

        when:
        def first = channel.read(buffer)
        def second = channel.read(buffer)
        def third = channel.read(buffer)

        then:
        first == BLOCK
        second == 100
        third == -1
        buffer.flip()
        def read = new byte[buffer.remaining()]
        buffer.get(read)
        read == bytes
        channel.position() == bytes.length
    }

    def 'a read into a small buffer should fill it'() {
        given:
        def bytes = content(1000)
        def client = Stub(FovusS3Client) { getObject(KEY, 0L) >> trickling(bytes) }
        def channel = new S3ReadChannel(client, KEY, bytes.length)
        def buffer = ByteBuffer.allocate(100)

        expect:
        channel.read(buffer) == 100
        !buffer.hasRemaining()
        channel.read(buffer) == 0
    }

    def 'a stream that ends early should return what it had, then the end'() {
        given:
        def client = Stub(FovusS3Client) { getObject(KEY, 0L) >> trickling(content(50)) }
        def channel = new S3ReadChannel(client, KEY, 1000L)
        def buffer = ByteBuffer.allocate(BLOCK)

        expect:
        channel.read(buffer) == 50
        channel.read(buffer) == -1
    }

    def 'a ranged body that ends before its declared length should fail the read'() {
        given: 'from byte 100 of a 1000-byte object, S3 declares the 900 bytes left but the body ends after 50'
        S3Client s3 = Mock()
        def client = new FovusS3Client(s3, s3, Mock(S3Transfers), 'bucket', 'pipelines/p-1-user/', null)
        def channel = new S3ReadChannel(client, KEY, 1000L)
        channel.position(100L)

        when:
        channel.read(ByteBuffer.allocate(BLOCK))

        then:
        1 * s3.getObject({ GetObjectRequest r -> r.key() == KEY && r.range() == 'bytes=100-' }) >>
                new ResponseInputStream<GetObjectResponse>(GetObjectResponse.builder().contentLength(900L).build(),
                                                           AbortableInputStream.create(trickling(content(50))))
        def e = thrown(IOException)
        e.message == "S3 read failed on ${KEY}: the body ended after 50 of 900 bytes".toString()
        e.cause == null
    }
}
