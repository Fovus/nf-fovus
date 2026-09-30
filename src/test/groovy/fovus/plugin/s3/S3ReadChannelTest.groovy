package fovus.plugin.s3

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
}
