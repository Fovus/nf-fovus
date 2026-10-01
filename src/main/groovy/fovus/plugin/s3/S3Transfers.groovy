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

    /** A stream whose bytes become the object at {@code key} only when it is closed without error. */
    S3UploadStream newUploadStream(String key) throws IOException
}
