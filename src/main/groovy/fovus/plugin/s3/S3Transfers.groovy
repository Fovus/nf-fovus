package fovus.plugin.s3

import java.nio.file.Path

/** File transfers and streamed writes for one bucket; implemented with the SDK's S3 Transfer Manager. */
interface S3Transfers extends Closeable {

    /** Upload a local file to {@code key} (multipart above the threshold). SDK failures are thrown unwrapped. */
    void uploadFile(Path file, String key) throws IOException

    /**
     * Download {@code key} into {@code destination}, an existing file the caller owns (a temp file): it is written in
     * place, never created. SDK failures are thrown unwrapped.
     */
    void downloadFile(String key, Path destination) throws IOException

    /**
     * A stream whose bytes become the object at {@code key} only when it is closed without error. With a
     * {@code contentLength} (null when unknown) the stream must receive exactly that many bytes: a write past it, or
     * a close before it, fails and discards the upload. A known length lets a large stream use larger parts; a length
     * no stream can take fails here, before the upload starts.
     */
    S3UploadStream newUploadStream(String key, Long contentLength) throws IOException
}
