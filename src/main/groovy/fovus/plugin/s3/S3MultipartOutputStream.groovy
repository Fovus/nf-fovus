package fovus.plugin.s3

import groovy.transform.CompileStatic
import software.amazon.awssdk.services.s3.model.CompletedPart

/**
 * Writes one object. Up to one part is buffered in memory; closing publishes it with a single PutObject,
 * larger streams become a multipart upload completed on close. Nothing is visible in storage until
 * {@link #close()} succeeds: a failed part aborts the upload, and a stream that is never closed (e.g. the
 * JVM is killed) leaves only an invisible, incomplete upload.
 */
@CompileStatic
class S3MultipartOutputStream extends OutputStream {

    private final FovusS3Client client
    private final String key
    private final List<CompletedPart> parts = []
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream(8192)
    private String uploadId
    private boolean closed

    S3MultipartOutputStream(FovusS3Client client, String key) {
        this.client = client
        this.key = key
    }

    @Override
    void write(int b) throws IOException {
        ensureOpen()
        buffer.write(b)
        if (buffer.size() >= client.partSize) flushPart()
    }

    @Override
    void write(byte[] bytes, int offset, int length) throws IOException {
        ensureOpen()
        int position = offset
        int remaining = length
        while (remaining > 0) {
            final int chunk = Math.min(remaining, client.partSize - buffer.size())
            buffer.write(bytes, position, chunk)
            position += chunk
            remaining -= chunk
            if (buffer.size() >= client.partSize) flushPart()
        }
    }

    @Override
    void close() throws IOException {
        if (closed) return
        closed = true
        try {
            if (uploadId == null) {
                client.putObject(key, buffer.toByteArray())
                return
            }
            if (buffer.size() > 0) uploadBufferedPart()
            client.completeMultipart(key, uploadId, parts)
        }
        catch (IOException e) {
            discard()
            throw e
        }
        finally {
            buffer = null
        }
    }

    /** Throw away everything written so far; nothing becomes visible in storage. */
    void abort() {
        if (closed) return
        closed = true
        discard()
        buffer = null
    }

    private void flushPart() throws IOException {
        try {
            uploadBufferedPart()
        }
        catch (IOException e) {
            abort()
            throw e
        }
    }

    private void uploadBufferedPart() throws IOException {
        if (uploadId == null) uploadId = client.createMultipart(key)
        parts.add(client.uploadPart(key, uploadId, parts.size() + 1, buffer.toByteArray()))
        buffer.reset()
    }

    private void discard() {
        if (uploadId != null) client.abortMultipart(key, uploadId)
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("The stream to ${key} is closed".toString())
    }
}
