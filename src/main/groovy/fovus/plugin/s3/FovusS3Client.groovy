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
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.profiles.ProfileFile
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3BaseClientBuilder
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*

import java.nio.file.AccessDeniedException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Duration

/**
 * S3 access to Fovus storage in direct mode: the work directory, {@code pipelines/<pid>/} in the user's Fovus
 * bucket, and Nextflow's session scratch folders next to it, {@code pipelines/tmp/} and
 * {@code pipelines/collect-file/}. Nextflow writes those under its {@code workDir} itself (collectFile without
 * {@code storeDir}, and the list of collected files kept for {@code -resume}); mount mode writes them to the same
 * keys through the mount. The user's files, {@code files/}, are read and written too: a pipeline names inputs
 * there as {@code fovus://} paths, and {@code publishDir} can publish into it. The jobs' outputs, {@code jobs/},
 * are read only.
 *
 * Reads use the download token and writes the upload token. Neither token is limited to the pipeline: both
 * reach the whole bucket. Every key is therefore checked before any call -- it must be inside one of those
 * folders (for a write, any but {@code jobs/}), with no {@code .} or {@code ..} segment -- and this guard is
 * what keeps the plugin there. Writes into {@code jobs/} fail as read-only, writes elsewhere are refused, and
 * reads elsewhere look like missing files. S3 errors are reported by code, HTTP status, request ID and key
 * only -- never the S3 error body, which can echo the access key ID.
 *
 * Two sync clients serve metadata, listings, reads and small writes; file transfers and streamed writes go
 * through {@link S3Transfers}, the SDK's Transfer Manager. Both are behind the same guard and error mapping.
 */
@Slf4j
@CompileStatic
class FovusS3Client implements Closeable {

    static final int DEFAULT_LIST_PAGE_SIZE = 1000
    static final int MAX_ATTEMPTS = 10
    static final long MAX_COPY_OBJECT_SIZE = 5L * 1024 * 1024 * 1024
    static final Set<String> EXPIRED_TOKEN_CODES =
            ['ExpiredToken', 'ExpiredTokenException', 'InvalidToken', 'TokenRefreshRequired'] as Set<String>
    /** Nextflow's session scratch folders, directly under its {@code workDir} (the pipelines area). */
    static final List<String> SESSION_SCRATCH_FOLDERS = List.of('tmp/', 'collect-file/')
    /** The user's files, and their jobs' outputs: the other areas of Fovus storage, next to {@code pipelines/}. */
    static final String FILES_AREA = 'files/'
    static final String JOBS_AREA = 'jobs/'
    /** Why a write into {@link #JOBS_AREA} is refused. Explicitly public: the NIO provider, in Java, reads it. */
    public static final String JOBS_READ_ONLY = 'Fovus storage jobs/ is read-only'
    private static final List<String> JOBS_FOLDERS = List.of(JOBS_AREA)

    private final S3Client reader
    private final S3Client writer
    private final S3Transfers transfers
    final String bucket
    final String prefix
    private final int listPageSize
    private final RefreshingStorageCredentials credentials
    /** The pipeline folder, the session scratch folders and the files area, each ending with {@code /}: where this client writes. */
    private final List<String> allowedFolders
    /** Those, and the jobs area: where this client reads. */
    private final List<String> readableFolders

    FovusS3Client(S3Client reader, S3Client writer, S3Transfers transfers, String bucket, String prefix,
                  RefreshingStorageCredentials credentials, int listPageSize = DEFAULT_LIST_PAGE_SIZE) {
        // The guard treats the prefix as a folder: without the trailing slash it would also admit sibling pipelines
        if (!prefix || !prefix.endsWith('/')) {
            throw new IllegalArgumentException("The pipeline prefix must end with '/': ${prefix}".toString())
        }
        this.reader = reader
        this.writer = writer
        this.transfers = transfers
        this.bucket = bucket
        this.prefix = prefix
        this.credentials = credentials
        this.listPageSize = listPageSize
        this.allowedFolders = allowedFolders(prefix)
        this.readableFolders = (this.allowedFolders + [JOBS_AREA]).asImmutable()
    }

    /** The pipeline folder, the scratch folders in the pipelines area it belongs to (the prefix's first segment), and the files area. */
    private static List<String> allowedFolders(String prefix) {
        final area = prefix.substring(0, prefix.indexOf('/') + 1)
        final List<String> folders = [prefix]
        for (String scratch : SESSION_SCRATCH_FOLDERS) folders.add(area + scratch)
        folders.add(FILES_AREA)
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
        final transfers = TransferManagerTransfers.create(first.bucket, first.region, credentials.readProvider(),
                                                          credentials.writeProvider(), interceptors)
        return new FovusS3Client(buildClient(first.region, credentials.readProvider(), interceptors),
                                 buildClient(first.region, credentials.writeProvider(), interceptors),
                                 transfers, first.bucket, first.prefix, credentials)
    }

    private static S3Client buildClient(String region, AwsCredentialsProvider provider, List<ExecutionInterceptor> interceptors) {
        return withFovusSettings(S3Client.builder(), region, provider, interceptors)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(30))
                        .socketTimeout(Duration.ofMinutes(5)))
                .build()
    }

    /**
     * The settings of every client of Fovus storage, sync or async, so that they cannot drift apart. The user's own
     * AWS configuration must not reach these clients: an explicit endpoint, so neither {@code AWS_ENDPOINT_URL(_S3)},
     * {@code aws.endpointUrl(S3)} nor a profile's {@code endpoint_url} can send Fovus-signed requests elsewhere, and
     * an empty profile file, so {@code ~/.aws/config} and {@code ~/.aws/credentials} are not read at all. FIPS and
     * dual-stack are switched off explicitly: the SDK refuses either one next to an explicit endpoint, so a user's
     * {@code AWS_USE_FIPS_ENDPOINT} would otherwise fail every request. Standard retries, up to {@link #MAX_ATTEMPTS}
     * attempts, and checksums only where S3 requires them (TLS protects the transfer).
     */
    @PackageScope
    static <B extends S3BaseClientBuilder<B, ?>> B withFovusSettings(B builder, String region, AwsCredentialsProvider provider,
                                                                     List<ExecutionInterceptor> interceptors) {
        final retries = AwsRetryStrategy.standardRetryStrategy().toBuilder().maxAttempts(MAX_ATTEMPTS).build()
        final overrides = ClientOverrideConfiguration.builder()
                .retryStrategy(retries)
                .defaultProfileFile(emptyProfileFile())
        for (ExecutionInterceptor interceptor : interceptors) overrides.addExecutionInterceptor(interceptor)
        return builder
                .region(Region.of(region))
                .endpointOverride(URI.create("https://s3.${region}.amazonaws.com".toString()))
                .fipsEnabled(false)
                .dualstackEnabled(false)
                .credentialsProvider(provider)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .overrideConfiguration(overrides.build())
    }

    private static ProfileFile emptyProfileFile() {
        return ProfileFile.builder()
                .content(new ByteArrayInputStream(new byte[0]))
                .type(ProfileFile.Type.CONFIGURATION)
                .build()
    }

    // -- reads

    /** Size and time of an object, or {@code null} when it does not exist or is outside the readable folders. */
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

    /** The object's body from {@code fromByte}; a failure while it streams is an I/O error, see {@link S3BodyStream}. */
    InputStream getObject(String key, long fromByte) throws IOException {
        if (!readable(key)) throw new NoSuchFileException(uri(key))
        final builder = GetObjectRequest.builder().bucket(bucket).key(key)
        if (fromByte > 0) builder.range("bytes=${fromByte}-".toString())
        final request = builder.build()
        return new S3BodyStream(call('read', key) { reader.getObject(request) }, key)
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

    // -- transfers

    /** A stream whose bytes become the object at {@code key} once it is closed; see {@link S3Transfers#newUploadStream}. */
    S3UploadStream newOutputStream(String key) throws IOException {
        writable(key)
        return new MappedUploadStream(key, call('write', key) { transfers.newUploadStream(key) })
    }

    /**
     * Upload a local file, in parallel parts above the transfers' part size. An expired token refreshes the
     * credentials and uploads the whole file again, once.
     */
    void uploadFile(Path file, String key) throws IOException {
        writable(key)
        call('write', key) { transfers.uploadFile(file, key) }
    }

    /**
     * Download an object to a local file. The data goes to a temporary file next to {@code target}, moved into
     * place only on success, and deleted on any failure.
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
            call('read', key) { transfers.downloadFile(key, temp) }
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING)
            moved = true
        }
        finally {
            if (!moved) deleteQuietly(temp)
        }
    }

    /**
     * Copy a readable key to a writable one, in any area (a task output into {@code files/}, for {@code publishDir}):
     * CopyObject when allowed, otherwise a streamed download and upload.
     */
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
            getObject(sourceKey).withCloseable { InputStream input -> input.transferTo(out) }
            out.close()
        }
        finally {
            // A no-op once close() has run, whether or not it succeeded; otherwise it discards the partial upload
            out.abort()
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

    /** Close the transfers and their clients. For tests: in a run, Nextflow ends the JVM, and the SDK's threads are daemons. */
    @Override
    void close() throws IOException {
        transfers.close()
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

    private IOException mapError(String operation, String key, Exception error) {
        final credentialsFailure = credentialsFailure(error)
        if (credentialsFailure != null) return credentialsFailure
        if (error instanceof IOException) return (IOException) error
        if (error instanceof S3Exception) {
            final s3Error = (S3Exception) error
            if (s3Error.statusCode() == 404 && errorCode(s3Error) == 'NoSuchBucket') {
                return new IOException("Fovus storage bucket ${bucket} was not found".toString())
            }
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

    /** Inside a readable folder, including the folder itself written without its trailing slash. */
    private boolean readable(String key) {
        return inOrIs(key, readableFolders)
    }

    /**
     * Throws, with no S3 call, unless a path with this key may be written to or removed: inside a writable folder,
     * or one of them itself without its trailing slash, which is how a path to the folder is spelled. For callers
     * that must tell the guard's refusal from S3's own denial of the call they make next, such as a delete that is
     * left in place when S3 denies it. The calls that write an object are stricter: an object named as a folder
     * (without the slash) would sit next to it, and is refused.
     */
    void checkWritable(String key) throws AccessDeniedException {
        if (inOrIs(key, allowedFolders)) return
        writable(key)
    }

    private String writable(String key) throws AccessDeniedException {
        if (inScope(key)) return key
        // Dot segments can lead anywhere, whatever the key starts with: they are refused as outside, even in jobs/
        if (inOrIs(key, JOBS_FOLDERS)) {
            throw new AccessDeniedException(uri(key), null, JOBS_READ_ONLY)
        }
        throw new AccessDeniedException(uri(key), null,
                "Refusing to write outside ${prefix}, the session scratch folders and files/".toString())
    }

    /** Under the pipeline prefix, a session scratch folder or the files area: where this client writes objects. */
    private boolean inScope(String key) {
        return within(key, allowedFolders)
    }

    /** Under one of {@code folders}, or one of them itself named without its trailing {@code /}. */
    private static boolean inOrIs(String key, List<String> folders) {
        return within(key, folders) || (key != null && !hasDotSegment(key) && folders.contains(key + '/'))
    }

    /** Under one of {@code folders}, with no {@code .} or {@code ..} segment that would lead back out of it. */
    private static boolean within(String key, List<String> folders) {
        if (key == null || hasDotSegment(key)) return false
        for (String folder : folders) {
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

    /**
     * The upload stream of the transfers, with its failures reported as those of every other call are, by
     * {@link #mapError}: an I/O error with the S3 error code, status and request ID, never the SDK's exception.
     */
    private final class MappedUploadStream extends S3UploadStream {

        private final String key
        private final S3UploadStream upload

        MappedUploadStream(String key, S3UploadStream upload) {
            this.key = key
            this.upload = upload
        }

        @Override
        void write(int b) throws IOException {
            try {
                upload.write(b)
            }
            catch (Exception e) {
                throw mapError('write', key, e)
            }
        }

        @Override
        void write(byte[] bytes, int offset, int length) throws IOException {
            try {
                upload.write(bytes, offset, length)
            }
            catch (Exception e) {
                throw mapError('write', key, e)
            }
        }

        @Override
        void close() throws IOException {
            try {
                upload.close()
            }
            catch (Exception e) {
                throw mapError('write', key, e)
            }
        }

        @Override
        void abort() {
            upload.abort()
        }
    }
}
