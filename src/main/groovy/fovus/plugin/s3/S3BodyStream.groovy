package fovus.plugin.s3

import groovy.transform.CompileStatic
import software.amazon.awssdk.core.exception.SdkException

/**
 * An object's body as it streams from S3. The SDK reports a failure while the body streams (a dropped
 * connection, an aborted request) as an unchecked exception; here it becomes an I/O error that names only the
 * key and the exception class, with no cause, like every other S3 failure. Callers such as the exit status
 * reader then see an I/O error to retry, never a raw SDK exception.
 */
@CompileStatic
class S3BodyStream extends FilterInputStream {

    private final String key

    S3BodyStream(InputStream body, String key) {
        super(body)
        this.key = key
    }

    @Override
    int read() throws IOException {
        try {
            return super.read()
        }
        catch (SdkException e) {
            throw failure(e)
        }
    }

    @Override
    int read(byte[] bytes, int offset, int length) throws IOException {
        try {
            return super.read(bytes, offset, length)
        }
        catch (SdkException e) {
            throw failure(e)
        }
    }

    @Override
    long skip(long count) throws IOException {
        try {
            return super.skip(count)
        }
        catch (SdkException e) {
            throw failure(e)
        }
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

    @Override
    void close() throws IOException {
        try {
            super.close()
        }
        catch (SdkException e) {
            throw failure(e)
        }
    }

    private IOException failure(SdkException error) {
        return new IOException("S3 read failed on ${key}: ${error.class.simpleName}".toString())
    }
}
