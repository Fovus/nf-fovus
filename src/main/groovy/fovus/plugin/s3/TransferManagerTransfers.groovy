package fovus.plugin.s3

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.FileTransformerConfiguration
import software.amazon.awssdk.core.FileTransformerConfiguration.FailureBehavior
import software.amazon.awssdk.core.FileTransformerConfiguration.FileWriteOption
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.async.BlockingOutputStreamAsyncRequestBody
import software.amazon.awssdk.core.async.BufferedSplittableAsyncRequestBody
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.DownloadRequest
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest
import software.amazon.awssdk.utils.CancellableOutputStream
import software.amazon.awssdk.utils.SdkAutoCloseable

import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
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
 * <li>downloads use a plain (not multipart) reader client: one GET into the destination file, retried as a whole,
 *     also when its body ends before its declared length. The SDK's parallel multipart download is left out on
 *     purpose. The file is written in place, never created: see {@link #INTO_EXISTING_FILE};</li>
 * <li>uploads use a multipart writer client: one PutObject up to the part size, parallel parts above it (larger
 *     ones when a file would need more than 10,000), each part read again from the file on a retry;</li>
 * <li>a streamed write is split into parts by the writer client, each buffered whole before it is sent, so that
 *     a part (or the single PutObject of a short stream) can be sent again. An open stream holds at most
 *     {@link #STREAM_BUFFER_PARTS} parts' worth of memory, 32 MiB at the default part size: the writer blocks until
 *     a part is sent. A stream's length may be known in advance, which lets a large one use larger parts: see
 *     {@link #newUploadStream} for the sizes a stream can take.</li>
 * </ul>
 *
 * An upload, of a file or a stream, has at most {@link #MAX_IN_FLIGHT_PARTS} parts in flight, far fewer than the
 * writer's {@link #MAX_CONNECTIONS} connections, so that one large upload cannot take them all from the small writes
 * (task files) that share the writer. A request that still finds every connection busy waits for one as long as a
 * read may take ({@link #IO_TIMEOUT}): the SDK does not retry a connection it failed to get.
 *
 * The SDK's threads (Netty's event loop, which the two clients share, and the SDK's async response and scheduler
 * threads) are daemons and start with the first request. Nextflow ends the JVM with {@code System.exit}, so there is
 * no shutdown hook; {@link #close()} is for tests and an orderly shutdown.
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
    /**
     * The parts of one file upload or streamed write in flight at once. Four, as our own multipart code had: at
     * 16 MiB parts that is 64 MiB on the wire per upload, enough to keep a link busy, and a dozen uploads at once
     * still leave connections free.
     */
    static final int MAX_IN_FLIGHT_PARTS = 4
    /** S3's limit on the parts of one upload. */
    static final long MAX_PARTS = 10_000
    /** The connections of each client (the SDK's default, made explicit next to {@link #MAX_IN_FLIGHT_PARTS}). */
    static final int MAX_CONNECTIONS = 50
    /** How long a read or write may stall, and how long a request waits for a free connection. */
    static final Duration IO_TIMEOUT = Duration.ofMinutes(5)
    /**
     * How long the event loop must be idle before it stops, once the clients are closed. Closing a client closes its
     * connections, and each one, as it closes, hands a last task to an event loop of the group, maybe another one:
     * with no quiet period at all a loop that already stopped rejects it, and Netty logs a warning
     * ({@code RejectedExecutionException: event executor terminated}). The SDK's own 2 seconds would slow every close.
     */
    static final Duration EVENT_LOOP_QUIET_PERIOD = Duration.ofMillis(100)
    /** How long the event loop is given to stop, quiet period included, and how long {@link #close()} waits for it. */
    static final Duration EVENT_LOOP_SHUTDOWN_TIMEOUT = Duration.ofSeconds(5)
    /**
     * A download writes into the destination from its start, opening it for writing only: unlike the
     * {@code downloadFile} default, it never creates the file. The caller deletes its temp file on failure or
     * interrupt; an SDK attempt whose response arrives later then fails to open it instead of re-creating it.
     */
    private static final FileTransformerConfiguration INTO_EXISTING_FILE = FileTransformerConfiguration.builder()
            .fileWriteOption(FileWriteOption.WRITE_TO_POSITION)
            .position(0L)
            .failureBehavior(FailureBehavior.LEAVE)
            .build()

    private final S3TransferManager reader
    private final S3TransferManager writer
    private final S3AsyncClient writerClient
    private final String bucket
    /** The clients built for the transfer managers, which do not close a client they were given. */
    private final List<? extends SdkAutoCloseable> clients
    /** The event loop of those clients, which do not shut down an event loop they were given. */
    private final SdkEventLoopGroup eventLoops
    /** The part size of the writer client: its threshold, its smallest part, and half its stream buffer. */
    private final long partSize

    @PackageScope
    TransferManagerTransfers(S3TransferManager reader, S3TransferManager writer, S3AsyncClient writerClient, String bucket,
                             List<? extends SdkAutoCloseable> clients = [], SdkEventLoopGroup eventLoops = null,
                             long partSize = DEFAULT_PART_SIZE) {
        this.reader = reader
        this.writer = writer
        this.writerClient = writerClient
        this.bucket = bucket
        this.clients = clients
        this.eventLoops = eventLoops
        this.partSize = partSize
    }

    /** The transfers of Fovus storage in {@code region}, with the settings of every Fovus client ({@link FovusS3Client#withFovusSettings}). */
    static TransferManagerTransfers create(String bucket, String region, AwsCredentialsProvider read, AwsCredentialsProvider write,
                                           List<ExecutionInterceptor> interceptors, long partSize = DEFAULT_PART_SIZE) {
        return fromBuilders(FovusS3Client.withFovusSettings(S3AsyncClient.builder(), region, read, interceptors),
                            FovusS3Client.withFovusSettings(S3AsyncClient.builder(), region, write, interceptors), bucket, partSize)
    }

    /**
     * The reader and writer clients from these builders, with the HTTP client and the part handling described
     * above, on {@code eventLoops}, which the transfers then own. Tests point the builders at MinIO or a local fake,
     * and may narrow the connection pool. When a step fails, what was built before it is closed, the event loop
     * included.
     */
    @PackageScope
    static TransferManagerTransfers fromBuilders(S3AsyncClientBuilder readerBuilder, S3AsyncClientBuilder writerBuilder,
                                                 String bucket, long partSize, int maxConnections = MAX_CONNECTIONS,
                                                 SdkEventLoopGroup eventLoops = SdkEventLoopGroup.builder().build()) {
        // In the order close() closes them: the transfer managers first, then the clients they were given
        final List<SdkAutoCloseable> built = []
        try {
            final readerClient = readerBuilder.httpClientBuilder(httpClient(eventLoops, maxConnections)).build()
            built.add(0, readerClient)
            final writerClient = writerBuilder.httpClientBuilder(httpClient(eventLoops, maxConnections))
                    .multipartEnabled(true)
                    .multipartConfiguration(MultipartConfiguration.builder()
                            .thresholdInBytes(partSize)
                            .minimumPartSizeInBytes(partSize)
                            // what bounds the memory of a streamed write; a file's parts are read from the file
                            .apiCallBufferSizeInBytes(STREAM_BUFFER_PARTS * partSize)
                            .parallelConfiguration { it.maxInFlightParts(MAX_IN_FLIGHT_PARTS) }
                            .build())
                    .build()
            built.add(0, writerClient)
            final reader = S3TransferManager.builder().s3Client(readerClient).build()
            built.add(0, reader)
            final writer = S3TransferManager.builder().s3Client(writerClient).build()
            return new TransferManagerTransfers(reader, writer, writerClient, bucket, [readerClient, writerClient], eventLoops, partSize)
        }
        catch (Throwable failure) {
            throw closeAll(built, eventLoops, failure)
        }
    }

    private static NettyNioAsyncHttpClient.Builder httpClient(SdkEventLoopGroup eventLoops, int maxConnections) {
        return NettyNioAsyncHttpClient.builder()
                .eventLoopGroup(eventLoops)
                .maxConcurrency(maxConnections)
                .connectionAcquisitionTimeout(IO_TIMEOUT)
                .connectionTimeout(Duration.ofSeconds(30))
                .readTimeout(IO_TIMEOUT)
                .writeTimeout(IO_TIMEOUT)
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
        final request = DownloadRequest.builder()
                .getObjectRequest(GetObjectRequest.builder().bucket(bucket).key(key).build())
                .responseTransformer(AsyncResponseTransformer.<GetObjectResponse> toFile(destination, INTO_EXISTING_FILE))
                .build()
        final GetObjectResponse response = await(reader.download(request).completionFuture(), 'read', key).result()
        final Long length = response.contentLength()
        if (length == null) return
        FileChannel.open(destination, StandardOpenOption.WRITE).withCloseable { FileChannel file ->
            // The SDK fails (and retries) a body cut short of its declared length; should one get through, it is an error
            final long written = file.size()
            if (written < length) {
                throw new IOException("S3 read failed on ${key}: the body ended after ${written} of ${length} bytes".toString())
            }
            // Each attempt writes from the start without truncating: cut what a longer attempt before it left at the end
            file.truncate(length)
        }
    }

    /**
     * A streamed write, of {@code contentLength} bytes when known. How large a stream can be depends on that:
     *
     * <ul>
     * <li>of unknown length, the writer client splits it into parts of the part size as it comes, so it is limited
     *     to {@link #MAX_PARTS} of them: about 156 GiB at 16 MiB. Larger, it fails at the part past the limit;</li>
     * <li>of known length, the writer client picks the part size itself, larger than ours when the stream would
     *     otherwise need more than {@link #MAX_PARTS}. A part must still fit in the stream's buffer (the SDK refuses
     *     to split it otherwise), so a stream is limited to {@link #MAX_PARTS} parts of
     *     {@link #STREAM_BUFFER_PARTS} part sizes: about 312 GiB at 16 MiB. A larger length fails here, before
     *     anything is sent. The buffer is not raised for it: it is the memory every open stream may take.</li>
     * </ul>
     *
     * A local file ({@link #uploadFile}) has no such limit: its parts are read from the file, not buffered.
     */
    @Override
    S3UploadStream newUploadStream(String key, Long contentLength) throws IOException {
        if (contentLength != null && contentLength < 0) {
            throw new IllegalArgumentException("A stream's length cannot be negative: ${contentLength}".toString())
        }
        final long largest = MAX_PARTS * STREAM_BUFFER_PARTS * partSize
        if (contentLength != null && contentLength > largest) {
            throw new IOException("Cannot upload ${key}: ${contentLength} bytes (${gib(contentLength)}) is more than the ${gib(largest)} a streamed upload supports".toString())
        }
        // A length is declared only above one part, where the writer client sends parts, each buffered: a declared
        // length within one part would be sent as one PutObject straight from the stream, which cannot be sent again.
        // Undeclared, the client splits the stream as it comes, and a stream within one part becomes one buffered PutObject
        final Long declared = contentLength != null && contentLength > partSize ? contentLength : null
        final body = BlockingOutputStreamAsyncRequestBody.builder()
                .contentLength(declared)
                .subscribeTimeout(SUBSCRIBE_TIMEOUT)
                .build()
        // Each part buffered whole before it is sent, so the SDK can send it again on a retry
        final retryable = BufferedSplittableAsyncRequestBody.builder()
                .asyncRequestBody(body)
                .bufferBeforeSend(true)
                .build()
        final upload = writerClient.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), retryable)
        return new UploadStream(key, body, upload, contentLength)
    }

    private static String gib(long bytes) {
        return String.format(Locale.ROOT, '%.1f GiB', bytes / (double) (1L << 30))
    }

    /** Close the transfer managers, then their clients, then shut the event loop down, whatever fails on the way. */
    @Override
    void close() {
        final List<SdkAutoCloseable> all = [reader, writer] as List<SdkAutoCloseable>
        all.addAll(clients)
        final failure = closeAll(all, eventLoops, null)
        if (failure != null) throw failure
    }

    /**
     * Close each of {@code closeables} in turn, then shut {@code eventLoops} down, each step whether or not the ones
     * before it failed. Returns {@code failure}, or else the first failure, with any later ones suppressed in it.
     */
    private static Throwable closeAll(List<? extends SdkAutoCloseable> closeables, SdkEventLoopGroup eventLoops, Throwable failure) {
        Throwable first = failure
        try {
            for (SdkAutoCloseable closeable : closeables) {
                try {
                    closeable.close()
                }
                catch (Throwable e) {
                    if (first == null) first = e
                    else first.addSuppressed(e)
                }
            }
        }
        finally {
            eventLoops?.eventLoopGroup()
                    ?.shutdownGracefully(EVENT_LOOP_QUIET_PERIOD.toMillis(), EVENT_LOOP_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    ?.awaitUninterruptibly(EVENT_LOOP_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
        }
        return first
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
     * body and waits for the upload. With a length, the stream must receive exactly that many bytes, or the upload is
     * discarded. Not thread-safe, as an {@code OutputStream} is not.
     */
    private static final class UploadStream extends S3UploadStream {

        private final String key
        private final BlockingOutputStreamAsyncRequestBody body
        private final CompletableFuture<PutObjectResponse> upload
        /** The bytes the stream must receive, or null when any number will do. */
        private final Long length
        /** The bytes written so far. */
        private long written
        /** The body's stream, once the SDK reads the body. */
        private CancellableOutputStream output
        /** Closed or aborted: the upload is over, one way or the other. */
        private boolean finished

        UploadStream(String key, BlockingOutputStreamAsyncRequestBody body, CompletableFuture<PutObjectResponse> upload, Long length) {
            this.key = key
            this.body = body
            this.upload = upload
            this.length = length
        }

        @Override
        void write(int b) throws IOException {
            ensureOpen()
            ensureRoom(1)
            try {
                output().write(b)
            }
            catch (RuntimeException e) {
                throw failed(e)
            }
            written++
        }

        @Override
        void write(byte[] bytes, int offset, int count) throws IOException {
            ensureOpen()
            ensureRoom(count)
            try {
                output().write(bytes, offset, count)
            }
            catch (RuntimeException e) {
                throw failed(e)
            }
            written += count
        }

        /** End the body, so the SDK sends what is left and completes the upload, and wait for it. */
        @Override
        void close() throws IOException {
            if (finished) return
            if (length != null && written != length) {
                abort()
                throw new IOException("S3 write failed on ${key}: the stream ended after ${written} of its declared ${length} bytes".toString())
            }
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

        /** A write past the declared length discards the upload: whatever the source holds, it is not that object. */
        private void ensureRoom(int count) throws IOException {
            if (length != null && written + count > length) {
                abort()
                throw new IOException("S3 write failed on ${key}: the stream is longer than its declared ${length} bytes".toString())
            }
        }
    }
}
