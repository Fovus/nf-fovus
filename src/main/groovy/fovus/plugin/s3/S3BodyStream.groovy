package fovus.plugin.s3

import groovy.transform.CompileStatic
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.s3.model.GetObjectResponse

/**
 * An object's body as it streams from S3. The SDK reports a failure while the body streams (a dropped
 * connection, an aborted request) as an unchecked exception; here it becomes an I/O error that names only the
 * key and the exception class, with no cause, like every other S3 failure. Callers such as the exit status
 * reader then see an I/O error to retry, never a raw SDK exception.
 *
 * A body that ends before the length its response declared (for a ranged GET, the bytes left from its start) is
 * an I/O error too, not an early end: the url-connection client reports a connection dropped part way as a normal
 * end, and a copy would otherwise publish a truncated object. A response without a length is not checked.
 */
@CompileStatic
class S3BodyStream extends FilterInputStream {

    private final String key
    /** The length the response declared, or -1 when it declared none. */
    private final long declaredLength
    /** The bytes read or skipped so far. */
    private long delivered

    S3BodyStream(ResponseInputStream<GetObjectResponse> body, String key) {
        super(body)
        this.key = key
        final Long length = body.response()?.contentLength()
        this.declaredLength = length != null ? length.longValue() : -1L
    }

    @Override
    int read() throws IOException {
        final int value
        try {
            value = super.read()
        }
        catch (SdkException e) {
            throw failure(e)
        }
        if (value < 0) checkComplete()
        else delivered++
        return value
    }

    @Override
    int read(byte[] bytes, int offset, int length) throws IOException {
        final int count
        try {
            count = super.read(bytes, offset, length)
        }
        catch (SdkException e) {
            throw failure(e)
        }
        if (count < 0) checkComplete()
        else delivered += count
        return count
    }

    @Override
    long skip(long count) throws IOException {
        final long skipped
        try {
            skipped = super.skip(count)
        }
        catch (SdkException e) {
            throw failure(e)
        }
        if (skipped > 0) delivered += skipped
        return skipped
    }

    @Override
    int available() throws IOException {
        try {
            return super.available()
        }
        catch (SdkException e) {
            throw failure(e)
        }
    }

    /** Not supported: a reread would be counted twice. */
    @Override
    boolean markSupported() {
        return false
    }

    @Override
    void mark(int readLimit) {
    }

    @Override
    void reset() throws IOException {
        throw new IOException('mark/reset not supported')
    }

    @Override
    void close() throws IOException {
        try {
            super.close()
        }
        catch (SdkException e) {
            throw failure(e)
        }
    }

    /** At the end of the body: it must have held every byte its response declared. */
    private void checkComplete() throws IOException {
        if (declaredLength >= 0 && delivered < declaredLength) {
            throw new IOException("S3 read failed on ${key}: the body ended after ${delivered} of ${declaredLength} bytes".toString())
        }
    }

    private IOException failure(SdkException error) {
        return new IOException("S3 read failed on ${key}: ${error.class.simpleName}".toString())
    }
}
