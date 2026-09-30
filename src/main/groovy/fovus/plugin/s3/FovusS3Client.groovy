package fovus.plugin.s3

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.profiles.ProfileFile
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.AccessDeniedException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * S3 access to the direct-mode work directory: {@code pipelines/<pid>/} in the user's Fovus bucket, plus
 * Nextflow's session scratch folders next to it, {@code pipelines/tmp/} and {@code pipelines/collect-file/}.
 * Nextflow writes those under its {@code workDir} itself (collectFile without {@code storeDir}, and the list
 * of collected files kept for {@code -resume}); mount mode writes them to the same keys through the mount.
 *
 * Reads use the download token and writes the upload token. Neither token is limited to the pipeline: both
 * reach the whole bucket. Every key is therefore checked before any call -- it must be inside the pipeline
 * folder or one of the scratch folders, with no {@code .} or {@code ..} segment -- and this guard is what
 * keeps the plugin there. Writes elsewhere are refused; reads elsewhere look like missing files. S3 errors
 * are reported by code, HTTP status, request ID and key only -- never the S3 error body, which can echo
 * the access key ID.
 */
@Slf4j
@CompileStatic
class FovusS3Client {

    static final int DEFAULT_PART_SIZE = 16 * 1024 * 1024
    static final int MIN_PART_SIZE = 5 * 1024 * 1024
    static final int DEFAULT_LIST_PAGE_SIZE = 1000
    static final int MAX_ATTEMPTS = 10
    static final int TRANSFER_THREADS = 4
    /** S3's limit on the parts of one multipart upload. */
    static final int MAX_PARTS = 10000
    static final long MAX_COPY_OBJECT_SIZE = 5L * 1024 * 1024 * 1024
    private static final int COPY_BUFFER_SIZE = 64 * 1024
    static final Set<String> EXPIRED_TOKEN_CODES =
            ['ExpiredToken', 'ExpiredTokenException', 'InvalidToken', 'TokenRefreshRequired'] as Set<String>
    /** Nextflow's session scratch folders, directly under its {@code workDir} (the pipelines area). */
    static final List<String> SESSION_SCRATCH_FOLDERS = ['tmp/', 'collect-file/']

    private final S3Client reader
    private final S3Client writer
    final String bucket
    final String prefix
    final int partSize
    private final int listPageSize
    private final RefreshingStorageCredentials credentials
    /** The pipeline folder and the session scratch folders, each ending with {@code /}. */
    private final List<String> allowedFolders
    /** The parts of every parallel upload and download of this client; see {@link #newTransferPool()}. */
    private final ThreadPoolExecutor transfers

    FovusS3Client(S3Client reader, S3Client writer, String bucket, String prefix, RefreshingStorageCredentials credentials,
                  int partSize = DEFAULT_PART_SIZE, int listPageSize = DEFAULT_LIST_PAGE_SIZE) {
        // The guard treats the prefix as a folder: without the trailing slash it would also admit sibling pipelines
        if (!prefix || !prefix.endsWith('/')) {
            throw new IllegalArgumentException("The pipeline prefix must end with '/': ${prefix}".toString())
        }
        this.reader = reader
        this.writer = writer
        this.bucket = bucket
        this.prefix = prefix
        this.credentials = credentials
        this.partSize = partSize
        this.listPageSize = listPageSize
        this.allowedFolders = allowedFolders(prefix)
        this.transfers = newTransferPool()
    }

    /**
     * One bounded pool per client, so at most {@link #TRANSFER_THREADS} parts are in flight however many files
     * Nextflow stages or publishes at once. Callers wait on their own parts, and parts never submit work, so
     * waiting cannot deadlock. The threads are daemons and end after a minute without work.
     */
    private static ThreadPoolExecutor newTransferPool() {
        final counter = new AtomicInteger()
        final factory = { Runnable task ->
            final thread = new Thread(task, "fovus-s3-transfer-${counter.incrementAndGet()}".toString())
            thread.daemon = true
            return thread
        } as ThreadFactory
        final pool = new ThreadPoolExecutor(TRANSFER_THREADS, TRANSFER_THREADS, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), factory)
        pool.allowCoreThreadTimeOut(true)
        return pool
    }

    /** Stop the transfer threads now. Tests only: in a run they are daemons that end on their own when idle. */
    @PackageScope
    void shutdownTransfers() {
        transfers.shutdownNow()
    }

    /** The pipeline folder, and the scratch folders in the pipelines area it belongs to (the prefix's first segment). */
    private static List<String> allowedFolders(String prefix) {
        final area = prefix.substring(0, prefix.indexOf('/') + 1)
        final List<String> folders = [prefix]
        for (String scratch : SESSION_SCRATCH_FOLDERS) folders.add(area + scratch)
        return folders.asImmutable()
    }

    /** Clients for the bucket and region named by the (already initialized) credentials. */
    static FovusS3Client create(RefreshingStorageCredentials credentials) throws StorageCredentialsException {
        return createWithInterceptors(credentials, [])
    }

    /** Tests only: the same clients, with extra SDK interceptors to observe the requests. */
    @PackageScope
    static FovusS3Client createWithInterceptors(RefreshingStorageCredentials credentials, List<ExecutionInterceptor> interceptors)
            throws StorageCredentialsException {
        final first = credentials.get()
        return new FovusS3Client(buildClient(first.region, credentials.readProvider(), interceptors),
                                 buildClient(first.region, credentials.writeProvider(), interceptors),
                                 first.bucket, first.prefix, credentials)
    }

    /**
     * The user's own AWS configuration must not reach these clients: an explicit endpoint, so neither
     * {@code AWS_ENDPOINT_URL(_S3)}, {@code aws.endpointUrl(S3)} nor a profile's {@code endpoint_url} can send
     * Fovus-signed requests elsewhere, and an empty profile file, so {@code ~/.aws/config} and
     * {@code ~/.aws/credentials} are not read at all. FIPS and dual-stack are switched off explicitly: the SDK
     * refuses either one next to an explicit endpoint, so a user's {@code AWS_USE_FIPS_ENDPOINT} would
     * otherwise fail every request.
     */
    private static S3Client buildClient(String region, AwsCredentialsProvider provider, List<ExecutionInterceptor> interceptors) {
        final retries = AwsRetryStrategy.standardRetryStrategy().toBuilder().maxAttempts(MAX_ATTEMPTS).build()
        final overrides = ClientOverrideConfiguration.builder()
                .retryStrategy(retries)
                .defaultProfileFile(emptyProfileFile())
        for (ExecutionInterceptor interceptor : interceptors) overrides.addExecutionInterceptor(interceptor)
        return S3Client.builder()
                .region(Region.of(region))
                .endpointOverride(URI.create("https://s3.${region}.amazonaws.com".toString()))
                .fipsEnabled(false)
                .dualstackEnabled(false)
                .credentialsProvider(provider)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(30))
                        .socketTimeout(Duration.ofMinutes(5)))
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .overrideConfiguration(overrides.build())
                .build()
    }

    private static ProfileFile emptyProfileFile() {
        return ProfileFile.builder()
                .content(new ByteArrayInputStream(new byte[0]))
                .type(ProfileFile.Type.CONFIGURATION)
                .build()
    }

    // -- reads

    /** Size and time of an object, or {@code null} when it does not exist or is outside the pipeline. */
    S3Entry head(String key) throws IOException {
        if (!readable(key)) return null
        final request = HeadObjectRequest.builder().bucket(bucket).key(key).build()
        try {
            final response = call('read', key) { reader.headObject(request) }
            return new S3Entry(key, response.contentLength() ?: 0L, response.lastModified(), false)
        }
        catch (NoSuchFileException ignored) {
            return null
        }
    }

    /** True when at least one object is under {@code dirKey}; a missing trailing {@code /} is added. */
    boolean hasChildren(String dirKey) throws IOException {
        final folder = asFolder(dirKey)
        if (!readable(folder)) return false
        final request = ListObjectsV2Request.builder().bucket(bucket).prefix(folder).maxKeys(1).build()
        final response = call('list', folder) { reader.listObjectsV2(request) }
        return (response.keyCount() ?: 0) > 0
    }

    /** The objects and sub-folder prefixes directly under {@code dirKey}; a missing trailing {@code /} is added. */
    List<S3Entry> list(String dirKey) throws IOException {
        return listing(dirKey, '/')
    }

    /** Every object under {@code dirKey}, at any depth; a missing trailing {@code /} is added. */
    List<S3Entry> listAll(String dirKey) throws IOException {
        return listing(dirKey, null)
    }

    InputStream getObject(String key) throws IOException {
        return getObject(key, 0L)
    }

    InputStream getObject(String key, long fromByte) throws IOException {
        if (!readable(key)) throw new NoSuchFileException(uri(key))
        final builder = GetObjectRequest.builder().bucket(bucket).key(key)
        if (fromByte > 0) builder.range("bytes=${fromByte}-".toString())
        final request = builder.build()
        return call('read', key) { reader.getObject(request) }
    }

    // -- writes

    void putObject(String key, byte[] bytes) throws IOException {
        writable(key)
        final request = PutObjectRequest.builder().bucket(bucket).key(key).build()
        call('write', key) { writer.putObject(request, RequestBody.fromBytes(bytes)) }
    }

    /** A zero-byte {@code <key>/} object, so an empty folder exists the way Nextflow expects. */
    void putDirectoryMarker(String key) throws IOException {
        putObject(key.endsWith('/') ? key : key + '/', new byte[0])
    }

    void delete(String key) throws IOException {
        writable(key)
        final request = DeleteObjectRequest.builder().bucket(bucket).key(key).build()
        call('delete', key) { writer.deleteObject(request) }
    }

    String createMultipart(String key) throws IOException {
        writable(key)
        final request = CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build()
        return call('write', key) { writer.createMultipartUpload(request).uploadId() }
    }

    CompletedPart uploadPart(String key, String uploadId, int partNumber, byte[] bytes) throws IOException {
        writable(key)
        final request = UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber).build()
        final response = call('write', key) { writer.uploadPart(request, RequestBody.fromBytes(bytes)) }
        return CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build()
    }

    void completeMultipart(String key, String uploadId, List<CompletedPart> parts) throws IOException {
        writable(key)
        final request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                .build()
        call('write', key) { writer.completeMultipartUpload(request) }
    }

    /** Best effort: an upload that cannot be aborted stays invisible until the bucket's lifecycle rule removes it. */
    void abortMultipart(String key, String uploadId) {
        if (!inScope(key)) {
            log.debug "[FOVUS] Not aborting a multipart upload outside the pipeline and scratch folders: ${key}"
            return
        }
        try {
            writer.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).build())
        }
        catch (Exception e) {
            log.debug "[FOVUS] Could not abort the multipart upload of ${key}: ${e.class.simpleName}"
        }
    }

    // -- transfers

    /** A stream that uploads to {@code key}; see {@link S3MultipartOutputStream}. */
    S3MultipartOutputStream newOutputStream(String key) throws IOException {
        return new S3MultipartOutputStream(this, writable(key))
    }

    /**
     * Upload a local file: one PutObject up to one part, otherwise parts in parallel on this client's transfer
     * pool, each read from its slice of the file as it is sent. Parts are larger than {@link #partSize} only when
     * the file would otherwise need more than {@link #MAX_PARTS}.
     */
    void uploadFile(Path file, String key) throws IOException {
        writable(key)
        final long size = Files.size(file)
        final long part = uploadPartSize(size, partSize)
        if (size <= part) {
            final request = PutObjectRequest.builder().bucket(bucket).key(key).build()
            call('write', key) { writer.putObject(request, RequestBody.fromFile(file)) }
            return
        }

        final uploadId = createMultipart(key)
        final List<Future<CompletedPart>> futures = []
        boolean completed = false
        try {
            int partNumber = 1
            for (long offset = 0; offset < size; offset += part) {
                final long start = offset
                final long length = Math.min(part, size - offset)
                final int number = partNumber++
                futures.add(transfers.submit({ -> uploadFilePart(key, uploadId, number, file, start, length) } as Callable<CompletedPart>))
            }
            final List<CompletedPart> parts = []
            for (Future<CompletedPart> future : futures) parts.add(await(future))
            completeMultipart(key, uploadId, parts)
            completed = true
        }
        finally {
            cancel(futures)
            if (!completed) abortMultipart(key, uploadId)
        }
    }

    /** The part size for a file upload: {@code partSize}, or more when that would take over {@link #MAX_PARTS} parts. */
    @PackageScope
    static long uploadPartSize(long size, int partSize) {
        return Math.max((long) partSize, Math.floorDiv(size + MAX_PARTS - 1, (long) MAX_PARTS))
    }

    private CompletedPart uploadFilePart(String key, String uploadId, int partNumber, Path file, long start, long length)
            throws IOException {
        writable(key)
        final request = UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber).build()
        final body = new FileSliceProvider(file, start, length)
        try {
            final response = call('write', key) {
                writer.uploadPart(request, RequestBody.fromContentProvider(body, length, 'application/octet-stream'))
            }
            return CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build()
        }
        finally {
            body.close()
        }
    }

    /**
     * Download an object to a local file: one GET up to one part, otherwise parallel ranged GETs. The data
     * goes to a temporary file next to {@code target}, moved into place only on success.
     */
    void downloadFile(String key, Path target) throws IOException {
        final entry = head(key)
        if (entry == null) throw new NoSuchFileException(uri(key))
        final directory = target.toAbsolutePath().parent
        Files.createDirectories(directory)
        // Not createTempFile: that is owner-only on POSIX, and the moved file would keep the mode
        final temp = Files.createFile(directory.resolve(".${target.fileName}.${UUID.randomUUID()}.part".toString()))
        boolean moved = false
        try {
            if (entry.size <= partSize) {
                readingBody(key) {
                    getObject(key).withCloseable { InputStream input -> Files.copy(input, temp, StandardCopyOption.REPLACE_EXISTING) }
                }
            }
            else {
                downloadRanges(key, entry.size, temp)
            }
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING)
            moved = true
        }
        finally {
            if (!moved) deleteQuietly(temp)
        }
    }

    /** Copy inside the pipeline: CopyObject when allowed, otherwise a streamed download and upload. */
    void copy(String sourceKey, String targetKey, long size) throws IOException {
        writable(targetKey)
        if (!readable(sourceKey)) throw new NoSuchFileException(uri(sourceKey))
        if (size <= MAX_COPY_OBJECT_SIZE) {
            final request = CopyObjectRequest.builder()
                    .sourceBucket(bucket).sourceKey(sourceKey)
                    .destinationBucket(bucket).destinationKey(targetKey)
                    .build()
            try {
                call('copy', targetKey) { writer.copyObject(request) }
                return
            }
            catch (AccessDeniedException ignored) {
                log.debug "[FOVUS] CopyObject is not allowed for ${sourceKey}; copying it through a download instead"
            }
        }
        final out = newOutputStream(targetKey)
        try {
            readingBody(sourceKey) {
                getObject(sourceKey).withCloseable { InputStream input -> input.transferTo(out) }
            }
            out.close()
        }
        finally {
            // A no-op once close() has run, whether or not it succeeded; otherwise it discards the partial upload
            out.abort()
        }
    }

    /**
     * Run a read of an object's bytes. The SDK reports a failure while the body streams (a dropped connection, an
     * aborted request) as an unchecked exception; it becomes an I/O error that names only the key and the
     * exception class, like every other S3 failure.
     */
    private static <T> T readingBody(String key, Closure<T> action) throws IOException {
        try {
            return action.call()
        }
        catch (SdkException e) {
            throw new IOException("S3 read failed on ${key}: ${e.class.simpleName}".toString())
        }
    }

    private static void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file)
        }
        catch (IOException e) {
            log.debug "[FOVUS] Could not delete the partial download ${file}: ${e.class.simpleName}"
        }
    }

    private void downloadRanges(String key, long size, Path temp) throws IOException {
        FileChannel.open(temp, StandardOpenOption.WRITE).withCloseable { FileChannel channel ->
            final List<Future<Object>> futures = []
            try {
                for (long start = 0; start < size; start += partSize) {
                    final long from = start
                    final long to = Math.min(start + partSize, size) - 1
                    futures.add(transfers.submit({ -> readRange(key, from, to, channel); return null } as Callable<Object>))
                }
                for (Future<Object> future : futures) await(future)
            }
            finally {
                cancel(futures)
            }
        }
    }

    /** One ranged GET, streamed straight into the file at its offset. */
    private void readRange(String key, long from, long to, FileChannel channel) throws IOException {
        final request = GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=${from}-${to}".toString()).build()
        final body = new S3BodyStream(call('read', key) { reader.getObject(request) }, key)
        body.withCloseable { InputStream input ->
            final buffer = new byte[COPY_BUFFER_SIZE]
            long position = from
            int count
            while ((count = input.read(buffer)) >= 0) {
                final chunk = ByteBuffer.wrap(buffer, 0, count)
                while (chunk.hasRemaining()) position += channel.write(chunk, position)
            }
            if (position != to + 1) {
                throw new IOException("S3 read failed on ${key}: the range ended at byte ${position} instead of ${to + 1}".toString())
            }
        }
    }

    /** Stop the parts of a transfer that is over, most usefully one that failed; a no-op for finished parts. */
    private static void cancel(List<? extends Future> futures) {
        for (Future future : futures) future.cancel(true)
    }

    private static <T> T await(Future<T> future) throws IOException {
        try {
            return future.get()
        }
        catch (ExecutionException e) {
            final cause = e.cause
            if (cause instanceof IOException) throw (IOException) cause
            throw new IOException(cause?.message ?: 'S3 transfer failed', cause)
        }
        catch (InterruptedException ignored) {
            // Keep the flag for the caller, and fail as an I/O error so the transfer's own cleanup still runs
            Thread.currentThread().interrupt()
            throw new InterruptedIOException('Interrupted while waiting for an S3 transfer')
        }
    }

    // -- helpers

    private List<S3Entry> listing(String dirKey, String delimiter) throws IOException {
        final folder = asFolder(dirKey)
        if (!readable(folder)) return []
        final builder = ListObjectsV2Request.builder().bucket(bucket).prefix(folder).maxKeys(listPageSize)
        if (delimiter != null) builder.delimiter(delimiter)
        final request = builder.build()
        return call('list', folder) {
            final List<S3Entry> entries = []
            for (ListObjectsV2Response page : reader.listObjectsV2Paginator(request)) {
                for (S3Object object : page.contents()) {
                    entries.add(new S3Entry(object.key(), object.size() ?: 0L, object.lastModified(), object.key().endsWith('/')))
                }
                for (CommonPrefix common : page.commonPrefixes()) {
                    entries.add(new S3Entry(common.prefix(), 0L, null, true))
                }
            }
            return entries
        }
    }

    /**
     * Run one S3 call. An expired token forces one credentials refresh and one retry; every failure is
     * mapped by {@link #mapError}.
     */
    private <T> T call(String operation, String key, Closure<T> action) throws IOException {
        try {
            return action.call()
        }
        catch (S3Exception e) {
            if (credentials == null || !EXPIRED_TOKEN_CODES.contains(errorCode(e))) throw mapError(operation, key, e)
            credentials.forceRefresh()
            try {
                return action.call()
            }
            catch (Exception retryFailure) {
                throw mapError(operation, key, retryFailure)
            }
        }
        catch (Exception e) {
            throw mapError(operation, key, e)
        }
    }

    private static IOException mapError(String operation, String key, Exception error) {
        final credentialsFailure = credentialsFailure(error)
        if (credentialsFailure != null) return credentialsFailure
        if (error instanceof IOException) return (IOException) error
        if (error instanceof S3Exception) {
            final s3Error = (S3Exception) error
            if (s3Error.statusCode() == 404) return new NoSuchFileException(uri(key))
            if (s3Error.statusCode() == 403) {
                return new AccessDeniedException(uri(key), null,
                        "Fovus storage credentials don't allow ${operation} on ${key} (${tokenFor(operation)} token)".toString())
            }
            return new IOException("S3 ${operation} failed on ${key}: ${errorCode(s3Error)} (HTTP ${s3Error.statusCode()}, request ${s3Error.requestId()})".toString())
        }
        if (error instanceof SdkClientException) {
            return new IOException("S3 ${operation} failed on ${key}: ${error.message}".toString())
        }
        return new IOException("S3 ${operation} failed on ${key}: ${error.class.simpleName}".toString())
    }

    private static StorageCredentialsException credentialsFailure(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.cause) {
            if (cause instanceof StorageCredentialsException) return (StorageCredentialsException) cause
        }
        return null
    }

    private static String errorCode(S3Exception error) {
        return error.awsErrorDetails()?.errorCode() ?: ''
    }

    private static String tokenFor(String operation) {
        return operation in ['read', 'list'] ? 'read' : 'write'
    }

    static String uri(String key) {
        return "fovus:///fovus-storage/${key}".toString()
    }

    /** A folder key always ends with {@code /}, so that listing it cannot reach a sibling such as {@code <pid>2/}. */
    private static String asFolder(String dirKey) {
        return dirKey == null || dirKey.endsWith('/') ? dirKey : dirKey + '/'
    }

    /** Inside the pipeline or a scratch folder, including the folder itself written without its trailing slash. */
    private boolean readable(String key) {
        return inScope(key) || (key != null && !hasDotSegment(key) && allowedFolders.contains(key + '/'))
    }

    private String writable(String key) throws AccessDeniedException {
        if (!inScope(key)) {
            throw new AccessDeniedException(uri(key), null, "Refusing to write outside ${prefix}".toString())
        }
        return key
    }

    /**
     * Under the pipeline prefix or a session scratch folder, with no {@code .} or {@code ..} segment that would
     * lead back out of it.
     */
    private boolean inScope(String key) {
        if (key == null || hasDotSegment(key)) return false
        for (String folder : allowedFolders) {
            if (key.startsWith(folder)) return true
        }
        return false
    }

    private static boolean hasDotSegment(String key) {
        for (String segment : key.split('/')) {
            if (segment == '.' || segment == '..') return true
        }
        return false
    }
}
