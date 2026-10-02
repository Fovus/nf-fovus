package fovus.plugin.s3

import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import spock.lang.Specification
import spock.lang.Unroll

/** An object's body as it streams from S3: a body that ends before the length its response declared is an I/O error. */
class S3BodyStreamTest extends Specification {

    static final String KEY = 'pipelines/p-1-user/fovus-work/ab/cdef/in.bin'

    /** A body of {@code size} bytes, whose response declares {@code declared} bytes (none when null). */
    private static S3BodyStream body(int size, Long declared) {
        final bytes = new byte[size]
        for (int i = 0; i < size; i++) bytes[i] = (byte) (i + 1)
        final response = GetObjectResponse.builder().contentLength(declared).build()
        return new S3BodyStream(new ResponseInputStream<GetObjectResponse>(response, AbortableInputStream.create(new ByteArrayInputStream(bytes))), KEY)
    }

    @Unroll
    def 'a body that ends after 6 of its declared 10 bytes should fail at the end, read by #how'() {
        given:
        def stream = body(6, 10L)

        when:
        reader.call(stream)

        then:
        def e = thrown(IOException)
        e.message == "S3 read failed on ${KEY}: the body ended after 6 of 10 bytes".toString()
        e.cause == null

        where:
        how                | reader
        'read()'           | { InputStream s -> while (s.read() >= 0) {} }
        'read(byte[])'     | { InputStream s -> final buffer = new byte[4]; while (s.read(buffer) >= 0) {} }
        'transferTo'       | { InputStream s -> s.transferTo(new ByteArrayOutputStream()) }
        'readAllBytes'     | { InputStream s -> s.readAllBytes() }
    }

    def 'the bytes before the end should still be delivered'() {
        given:
        def stream = body(6, 10L)
        def buffer = new byte[100]

        expect:
        stream.read(buffer) == 6
        buffer[0..5] == [1, 2, 3, 4, 5, 6] as byte[]

        when:
        stream.read(buffer)

        then:
        thrown(IOException)
    }

    @Unroll
    def 'a body of #size bytes declared as #declared should read normally to the end'() {
        given:
        def stream = body(size, declared)

        expect:
        stream.readAllBytes().length == size
        stream.read() == -1
        stream.read(new byte[4]) == -1

        where:
        size | declared
        6    | 6L
        6    | null
        0    | 0L
        0    | null
    }

    def 'skipped bytes should count toward the declared length'() {
        given:
        def complete = body(6, 6L)
        def truncated = body(6, 10L)

        expect:
        complete.skip(4) == 4
        complete.readAllBytes() == [5, 6] as byte[]

        when:
        truncated.skip(4)
        truncated.readAllBytes()

        then:
        def e = thrown(IOException)
        e.message == "S3 read failed on ${KEY}: the body ended after 6 of 10 bytes".toString()
    }

    def 'a read of no bytes should not count as the end'() {
        given:
        def stream = body(6, 10L)

        expect:
        stream.read(new byte[4], 0, 0) == 0
    }

    def 'mark and reset should not be supported, so a reread cannot be counted twice'() {
        given:
        def stream = body(6, 6L)

        when:
        stream.mark(100)
        stream.read()
        stream.reset()

        then:
        !stream.markSupported()
        thrown(IOException)
    }
}
