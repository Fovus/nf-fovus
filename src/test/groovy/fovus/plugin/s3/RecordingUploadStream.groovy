package fovus.plugin.s3

/**
 * An upload stream for tests that mock {@link S3Transfers}. The bytes are handed to {@code onPublish} only when the stream
 * is closed without {@link #abort()}, as the object becomes visible in S3 only then.
 */
class RecordingUploadStream extends S3UploadStream {

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream()
    private final Closure onPublish
    boolean published
    boolean aborted
    private boolean finished

    RecordingUploadStream(Closure onPublish = {}) {
        this.onPublish = onPublish
    }

    @Override
    void write(int b) throws IOException {
        ensureOpen()
        bytes.write(b)
    }

    @Override
    void write(byte[] data, int offset, int length) throws IOException {
        ensureOpen()
        bytes.write(data, offset, length)
    }

    @Override
    void close() throws IOException {
        if (finished) return
        finished = true
        published = true
        onPublish.call(bytes.toByteArray())
    }

    @Override
    void abort() {
        if (finished) return
        finished = true
        aborted = true
    }

    String getText() {
        return new String(bytes.toByteArray())
    }

    private void ensureOpen() throws IOException {
        if (finished) throw new IOException('The upload stream is closed')
    }
}
