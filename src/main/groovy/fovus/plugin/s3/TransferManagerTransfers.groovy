package fovus.plugin.s3

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.async.BlockingOutputStreamAsyncRequestBody
import software.amazon.awssdk.core.async.BufferedSplittableAsyncRequestBody
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest
import software.amazon.awssdk.utils.CancellableOutputStream
import software.amazon.awssdk.utils.SdkAutoCloseable

import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * File transfers and streamed writes through the SDK's S3 Transfer Manager, on the Java-based async client over
 * Netty. Every transfer is one the SDK can retry:
 *
 * <ul>
 * <li>downloads use a plain (not multipart) reader client: one GET into the destination file, retried as a whole.
 *     The SDK's parallel multipart download is left out on purpose;</li>
 * <li>uploads use a multipart writer client: one PutObject up to the part size, parallel parts above it (larger
 *     ones when a file would need more than 10,000), each part read again from the file on a retry;</li>
 * <li>a streamed write has no known length. The writer client splits it into parts, each buffered whole before it
 *     is sent, so that a part (or the single PutObject of a short stream) can be sent again. An open stream holds
 *     at most {@link #STREAM_BUFFER_PARTS} parts in memory, 32 MiB at the default part size: the writer blocks
 *     until a part is sent.</li>
 * </ul>
 *
 * The SDK's threads (Netty's event loop, its async response and scheduler threads) are daemons and start with the
 * first request. Nextflow ends the JVM with {@code System.exit}, so there is no shutdown hook; {@link #close()} is
 * for tests and an orderly shutdown.
 */
@CompileStatic
class TransferManagerTransfers implements S3Transfers {

    /** S3's smallest part, and the part size of the integration tests. */
    static final long MIN_PART_SIZE = 5L * 1024 * 1024
    static final long DEFAULT_PART_SIZE = 16L * 1024 * 1024
    /**
     * How many parts of a streamed write may be held in memory at once, being filled or sent. At least two: with a
     * buffer of one part, SDK 2.55 never starts reading the stream.
     */
    static final int STREAM_BUFFER_PARTS = 2
    /**
     * How long a stream waits for the SDK to start reading it. The multipart client subscribes before
     * {@code putObject} returns, so this is a safety net; the SDK's default of 10 seconds would be short if a
     * request first had to fetch credentials through the Fovus CLI.
     */
    static final Duration SUBSCRIBE_TIMEOUT = Duration.ofMinutes(1)
    /** How long a failed write waits for the SDK to report why the upload failed; see {@link UploadStream#failed}. */
    static final long UPLOAD_FAILURE_WAIT_SECONDS = 10

    private final S3TransferManager reader
    private final S3TransferManager writer
    private final S3AsyncClient writerClient
    private final String bucket
    /** The clients built for the transfer managers, which do not close a client they were given. */
    private final List<? extends SdkAutoCloseable> clients

    @PackageScope
    TransferManagerTransfers(S3TransferManager reader, S3TransferManager writer, S3AsyncClient writerClient, String bucket,
                             List<? extends SdkAutoCloseable> clients = []) {
        this.reader = reader
        this.writer = writer
        this.writerClient = writerClient
        this.bucket = bucket
        this.clients = clients
    }

    /** The transfers of Fovus storage in {@code region}, with the settings of every Fovus client ({@link FovusS3Client#withFovusSettings}). */
    static TransferManagerTransfers create(String bucket, String region, AwsCredentialsProvider read, AwsCredentialsProvider write,
                                           List<ExecutionInterceptor> interceptors, long partSize = DEFAULT_PART_SIZE) {
        return fromBuilders(FovusS3Client.withFovusSettings(S3AsyncClient.builder(), region, read, interceptors),
                            FovusS3Client.withFovusSettings(S3AsyncClient.builder(), region, write, interceptors), bucket, partSize)
    }

    /**
     * The reader and writer clients from these builders, with the HTTP client and the part handling described
     * above. Tests point the builders at MinIO or a local fake.
     */
    @PackageScope
    static TransferManagerTransfers fromBuilders(S3AsyncClientBuilder readerBuilder, S3AsyncClientBuilder writerBuilder,
                                                 String bucket, long partSize) {
        final readerClient = readerBuilder.httpClientBuilder(httpClient()).build()
        final writerClient = writerBuilder.httpClientBuilder(httpClient())
                .multipartEnabled(true)
                .multipartConfiguration(MultipartConfiguration.builder()
                        .thresholdInBytes(partSize)
                        .minimumPartSizeInBytes(partSize)
                        // what bounds the memory of a streamed write; a file's parts are read from the file
                        .apiCallBufferSizeInBytes(STREAM_BUFFER_PARTS * partSize)
                        .build())
                .build()
        return new TransferManagerTransfers(S3TransferManager.builder().s3Client(readerClient).build(),
                                            S3TransferManager.builder().s3Client(writerClient).build(),
                                            writerClient, bucket, [readerClient, writerClient])
    }

    private static NettyNioAsyncHttpClient.Builder httpClient() {
        return NettyNioAsyncHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(30))
                .readTimeout(Duration.ofMinutes(5))
                .writeTimeout(Duration.ofMinutes(5))
    }

    @Override
    void uploadFile(Path file, String key) throws IOException {
        final request = UploadFileRequest.builder()
                .putObjectRequest(PutObjectRequest.builder().bucket(bucket).key(key).build())
                .source(file)
                .build()
        await(writer.uploadFile(request).completionFuture(), 'write', key)
    }

    @Override
    void downloadFile(String key, Path destination) throws IOException {
        final request = DownloadFileRequest.builder()
                .getObjectRequest(GetObjectRequest.builder().bucket(bucket).key(key).build())
                .destination(destination)
                .build()
        await(reader.downloadFile(request).completionFuture(), 'read', key)
    }

    @Override
    S3UploadStream newUploadStream(String key) throws IOException {
        // No length: the multipart client then splits the stream into parts as it comes
        final body = BlockingOutputStreamAsyncRequestBody.builder()
                .contentLength(null)
                .subscribeTimeout(SUBSCRIBE_TIMEOUT)
                .build()
        // Each part buffered whole before it is sent, so the SDK can send it again on a retry
        final retryable = BufferedSplittableAsyncRequestBody.builder()
                .asyncRequestBody(body)
                .bufferBeforeSend(true)
                .build()
        final upload = writerClient.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), retryable)
        return new UploadStream(key, body, upload)
    }

    @Override
    void close() {
        reader.close()
        writer.close()
        for (SdkAutoCloseable client : clients) client.close()
    }

    /**
     * Wait for a transfer. An interrupt cancels it and keeps the thread's flag; a failure comes out as described at
     * {@link #reportable}.
     */
    private static <T> T await(CompletableFuture<T> transfer, String operation, String key) throws IOException {
        try {
            return transfer.get()
        }
        catch (InterruptedException ignored) {
            transfer.cancel(true)
            Thread.currentThread().interrupt()
            throw new InterruptedIOException("Interrupted while waiting for the S3 ${operation} of ${key}".toString())
        }
        catch (Exception e) {
            throw reportable(e, operation, key)
        }
    }

    /**
     * A failure as the client maps it: the SDK's own exception or an I/O error as it is, without the
     * {@code CompletionException}s and {@code ExecutionException}s around it, and anything else as an I/O error that
     * names only its class (its message could hold anything).
     */
    private static Exception reportable(Throwable failure, String operation, String key) {
        Throwable cause = failure
        while ((cause instanceof CompletionException || cause instanceof ExecutionException) && cause.cause != null) {
            cause = cause.cause
        }
        if (cause instanceof Error) throw (Error) cause
        if (cause instanceof SdkException || cause instanceof IOException) return (Exception) cause
        return new IOException("S3 ${operation} failed on ${key}: ${cause.class.simpleName}".toString())
    }

    /**
     * A streamed write: the bytes go to the SDK's blocking body as they are written, and {@link #close()} ends the
     * body and waits for the upload. Not thread-safe, as an {@code OutputStream} is not.
     */
    private static final class UploadStream extends S3UploadStream {

        private final String key
        private final BlockingOutputStreamAsyncRequestBody body
        private final CompletableFuture<PutObjectResponse> upload
        /** The body's stream, once the SDK reads the body. */
        private CancellableOutputStream output
        /** Closed or aborted: the upload is over, one way or the other. */
        private boolean finished

        UploadStream(String key, BlockingOutputStreamAsyncRequestBody body, CompletableFuture<PutObjectResponse> upload) {
            this.key = key
            this.body = body
            this.upload = upload
        }

        @Override
        void write(int b) throws IOException {
            ensureOpen()
            try {
                output().write(b)
            }
            catch (RuntimeException e) {
                throw failed(e)
            }
        }

        @Override
        void write(byte[] bytes, int offset, int length) throws IOException {
            ensureOpen()
            try {
                output().write(bytes, offset, length)
            }
            catch (RuntimeException e) {
                throw failed(e)
            }
        }

        /** End the body, so the SDK sends what is left and completes the upload, and wait for it. */
        @Override
        void close() throws IOException {
            if (finished) return
            try {
                output().close()
            }
            catch (RuntimeException e) {
                throw failed(e)
            }
            finished = true
            await(upload, 'write', key)
        }

        /** Cancel the body and the upload; nothing becomes visible. */
        @Override
        void abort() {
            if (finished) return
            finished = true
            // When nothing was written the body's stream is not needed: the SDK cancels the body with the upload
            output?.cancel()
            upload.cancel(true)
        }

        private CancellableOutputStream output() {
            if (output == null) {
                // An upload that failed before the SDK read the body (no credentials, say) would leave this waiting
                // for the subscribe timeout: report its failure instead
                if (upload.isCompletedExceptionally()) upload.join()
                output = body.outputStream()
            }
            return output
        }

        /**
         * Why a write or close failed. Once an upload fails, the SDK cancels the body, and the body then says only
         * that it was cancelled: the upload's own failure says why, so it is waited for, briefly. An interrupt keeps
         * the thread's flag. Either way the upload is discarded.
         */
        private Exception failed(RuntimeException failure) {
            try {
                if (Thread.currentThread().isInterrupted()) {
                    return new InterruptedIOException("Interrupted while writing to ${key}".toString())
                }
                try {
                    upload.get(UPLOAD_FAILURE_WAIT_SECONDS, TimeUnit.SECONDS)
                }
                catch (ExecutionException e) {
                    return reportable(e, 'write', key)
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt()
                    return new InterruptedIOException("Interrupted while writing to ${key}".toString())
                }
                catch (TimeoutException | RuntimeException ignored) {
                    // the upload is still going, or was cancelled: the write's own failure is all there is
                }
                return reportable(failure, 'write', key)
            }
            finally {
                abort()
            }
        }

        private void ensureOpen() throws IOException {
            if (finished) throw new IOException("The upload to ${key} is closed".toString())
        }
    }
}
