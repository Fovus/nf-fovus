package fovus.plugin.s3

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*

import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import java.time.Duration

/**
 * S3 access to the direct-mode work directory: {@code pipelines/<pid>/} in the user's Fovus bucket.
 *
 * Reads use the pipeline-scoped download token and writes the upload token. Every key is checked against
 * the pipeline prefix before any call; the upload token itself can write to all of {@code files/} and
 * {@code pipelines/}, so this guard is what keeps the plugin inside its own pipeline. S3 errors are
 * reported by code, HTTP status, request ID and key only -- never the S3 error body, which can echo the
 * access key ID.
 */
@Slf4j
@CompileStatic
class FovusS3Client {

    static final int DEFAULT_PART_SIZE = 16 * 1024 * 1024
    static final int MIN_PART_SIZE = 5 * 1024 * 1024
    static final int DEFAULT_LIST_PAGE_SIZE = 1000
    static final int MAX_ATTEMPTS = 10
    static final int TRANSFER_THREADS = 4
    static final Set<String> EXPIRED_TOKEN_CODES =
            ['ExpiredToken', 'ExpiredTokenException', 'InvalidToken', 'TokenRefreshRequired'] as Set<String>

    private final S3Client reader
    private final S3Client writer
    final String bucket
    final String prefix
    final int partSize
    private final int listPageSize
    private final RefreshingStorageCredentials credentials

    FovusS3Client(S3Client reader, S3Client writer, String bucket, String prefix, RefreshingStorageCredentials credentials,
                  int partSize = DEFAULT_PART_SIZE, int listPageSize = DEFAULT_LIST_PAGE_SIZE) {
        this.reader = reader
        this.writer = writer
        this.bucket = bucket
        this.prefix = prefix
        this.credentials = credentials
        this.partSize = partSize
        this.listPageSize = listPageSize
    }

    /** Clients for the bucket and region named by the (already initialized) credentials. */
    static FovusS3Client create(RefreshingStorageCredentials credentials) throws StorageCredentialsException {
        final first = credentials.get()
        return new FovusS3Client(buildClient(first.region, credentials.readProvider()),
                                 buildClient(first.region, credentials.writeProvider()),
                                 first.bucket, first.prefix, credentials)
    }

    private static S3Client buildClient(String region, AwsCredentialsProvider provider) {
        final retries = AwsRetryStrategy.standardRetryStrategy().toBuilder().maxAttempts(MAX_ATTEMPTS).build()
        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(provider)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(30))
                        .socketTimeout(Duration.ofMinutes(5)))
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .overrideConfiguration(ClientOverrideConfiguration.builder().retryStrategy(retries).build())
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

    /** True when at least one object starts with {@code dirKey}, which should end with {@code /}. */
    boolean hasChildren(String dirKey) throws IOException {
        if (!readable(dirKey)) return false
        final request = ListObjectsV2Request.builder().bucket(bucket).prefix(dirKey).maxKeys(1).build()
        final response = call('list', dirKey) { reader.listObjectsV2(request) }
        return (response.keyCount() ?: 0) > 0
    }

    /** The objects and sub-folder prefixes directly under {@code dirKey}, which should end with {@code /}. */
    List<S3Entry> list(String dirKey) throws IOException {
        return listing(dirKey, '/')
    }

    /** Every object under {@code dirKey}, at any depth. */
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
        final request = UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber).build()
        final response = call('write', key) { writer.uploadPart(request, RequestBody.fromBytes(bytes)) }
        return CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build()
    }

    void completeMultipart(String key, String uploadId, List<CompletedPart> parts) throws IOException {
        final request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                .build()
        call('write', key) { writer.completeMultipartUpload(request) }
    }

    /** Best effort: an upload that cannot be aborted stays invisible until the bucket's lifecycle rule removes it. */
    void abortMultipart(String key, String uploadId) {
        try {
            writer.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).build())
        }
        catch (Exception e) {
            log.debug "[FOVUS] Could not abort the multipart upload of ${key}: ${e.class.simpleName}"
        }
    }

    // -- helpers

    private List<S3Entry> listing(String dirKey, String delimiter) throws IOException {
        if (!readable(dirKey)) return []
        final builder = ListObjectsV2Request.builder().bucket(bucket).prefix(dirKey).maxKeys(listPageSize)
        if (delimiter != null) builder.delimiter(delimiter)
        final request = builder.build()
        return call('list', dirKey) {
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
            return new IOException("S3 ${operation} failed on ${key}: ${error.message}".toString(), error)
        }
        return new IOException("S3 ${operation} failed on ${key}: ${error.class.simpleName}".toString(), error)
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

    /** Inside this pipeline, including the pipeline folder itself. */
    private boolean readable(String key) {
        return key.startsWith(prefix) || key + '/' == prefix
    }

    private String writable(String key) throws AccessDeniedException {
        if (!key.startsWith(prefix)) {
            throw new AccessDeniedException(uri(key), null, "Refusing to write outside ${prefix}".toString())
        }
        return key
    }
}
