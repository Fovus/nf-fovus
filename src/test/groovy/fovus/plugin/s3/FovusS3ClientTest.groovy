package fovus.plugin.s3

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.exception.AbortedException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

class FovusS3ClientTest extends Specification {

    static final String PREFIX = 'pipelines/p-1-user/'
    static final String KEY = PREFIX + 'fovus-work/ab/cdef/.exitcode'
    static final String OTHER_PIPELINE_KEY = 'pipelines/p-2-user/x'
    /** Keys that start with the pipeline prefix as text but are not inside the pipeline folder. */
    static final List<String> DOT_SEGMENT_KEYS = [PREFIX + '../p-2-user/x', PREFIX + './x', PREFIX + 'a/../../p-2-user/x',
                                                  PREFIX + 'a/..', PREFIX + '..', PREFIX + 'a/./b']
    /** Keys in Nextflow's session scratch folders next to the pipeline folder, which mount mode also uses. */
    static final List<String> SCRATCH_KEYS = ['pipelines/tmp/xx/yyy', 'pipelines/collect-file/abc', 'pipelines/tmp/', 'pipelines/collect-file/']
    /** Keys that look like the scratch folders but are not inside them. */
    static final List<String> NEAR_SCRATCH_KEYS = ['pipelines/tmpx/y', 'pipelines/collect-filex/y', 'pipelines/tmp/../p-2-user/x',
                                                   'pipelines/collect-file/../p-2-user/x', 'pipelines/tmp/./x',
                                                   'tmp/x', 'collect-file/x', 'pipelines/p-1-user/../tmp/x']
    /** Keys in the files/ area of Fovus storage, which direct mode reads and writes. */
    static final List<String> FILES_KEYS = ['files/x', 'files/tmp/x', 'files/data/in.txt', 'files/results/']
    /** Keys in the jobs/ area of Fovus storage, which direct mode reads but does not write. */
    static final List<String> JOBS_KEYS = ['jobs/j-1/x', 'jobs/x', 'jobs/', 'jobs']
    /** The writable folders named without their trailing slash, as a path to the folder itself is: they stand for the folder. */
    static final List<String> BARE_FOLDER_KEYS = ['files', 'pipelines/p-1-user', 'pipelines/tmp', 'pipelines/collect-file']
    /** Keys in the files/ and jobs/ areas, which direct mode reads. */
    static final List<String> AREA_KEYS = ['files/x', 'files/tmp/x', 'files/data/in.txt', 'jobs/j-1/x']
    static final String OUTSIDE_REASON = 'Refusing to write outside pipelines/p-1-user/, the session scratch folders and files/'
    static final String JOBS_REASON = 'Fovus storage jobs/ is read-only'
    /** Keys that look like the files/ and jobs/ areas but are not inside them. */
    static final List<String> NEAR_AREA_KEYS = ['filesx/y', 'jobsx/y', 'files/../pipelines/p-2-user/x', 'files/./x',
                                                'jobs/j-1/../../pipelines/p-2-user/x', 'shared/x']

    S3Client s3 = Mock()
    RefreshingStorageCredentials credentials = Mock()
    FovusS3Client client = new FovusS3Client(s3, s3, 'bucket', PREFIX, credentials)

    /** An S3 error whose body text must never reach a message. */
    static S3Exception s3Error(int status, String code) {
        return (S3Exception) S3Exception.builder()
                .statusCode(status)
                .requestId('req-1')
                .message('raw S3 body text SECRET-BODY')
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).errorMessage('raw S3 body text SECRET-BODY').build())
                .build()
    }

    def 'writes outside the writable folders should be refused before calling S3'() {
        when:
        client.putObject('pipelines/p-2-user/x', new byte[0])

        then:
        def e = thrown(AccessDeniedException)
        e.reason == OUTSIDE_REASON
        0 * s3._
    }

    def 'reads outside the pipeline prefix should look like missing files'() {
        when:
        def head = client.head('pipelines/p-2-user/x')

        then:
        head == null
        0 * s3._

        when:
        client.getObject('pipelines/p-2-user/x')

        then:
        thrown(NoSuchFileException)
        0 * s3._
    }

    def 'the pipeline folder itself should be readable'() {
        when:
        def found = client.hasChildren(PREFIX)

        then:
        1 * s3.listObjectsV2(_ as ListObjectsV2Request) >> ListObjectsV2Response.builder().keyCount(1).build()
        found
    }

    def 'head should return size and time, and null for a missing object'() {
        given:
        def modified = Instant.parse('2026-09-30T12:00:00Z')

        when:
        def found = client.head(KEY)
        def missing = client.head(PREFIX + 'missing')

        then:
        2 * s3.headObject(_ as HeadObjectRequest) >>> [HeadObjectResponse.builder().contentLength(1L).lastModified(modified).build()] >> { throw s3Error(404, 'NotFound') }
        found.size == 1L
        found.lastModified == modified
        !found.directory
        missing == null
    }

    def 'access denied should name the token and hide the S3 body'() {
        given:
        s3.putObject(_ as PutObjectRequest, _ as RequestBody) >> { throw s3Error(403, 'AccessDenied') }

        when:
        client.putObject(KEY, 'x'.bytes)

        then:
        def e = thrown(AccessDeniedException)
        e.reason == "Fovus storage credentials don't allow write on ${KEY} (write token)".toString()
        !e.message.contains('SECRET-BODY')
    }

    def 'other S3 errors should report code, status and request id only'() {
        given:
        s3.getObject(_ as GetObjectRequest) >> { throw s3Error(500, 'InternalError') }

        when:
        client.getObject(KEY)

        then:
        def e = thrown(IOException)
        e.message == "S3 read failed on ${KEY}: InternalError (HTTP 500, request req-1)".toString()
        e.cause == null
    }

    def 'a missing bucket should be reported as such, not as a missing file'() {
        given:
        s3.listObjectsV2(_ as ListObjectsV2Request) >> { throw s3Error(404, 'NoSuchBucket') }

        when:
        client.hasChildren(PREFIX)

        then:
        def e = thrown(IOException)
        !(e instanceof NoSuchFileException)
        e.message == 'Fovus storage bucket bucket was not found'
        e.cause == null
    }

    def 'an expired token should force one refresh and retry once'() {
        when:
        def found = client.head(KEY)

        then:
        1 * s3.headObject(_ as HeadObjectRequest) >> { throw s3Error(400, 'ExpiredToken') }
        1 * credentials.forceRefresh()
        1 * s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(7L).build()
        found.size == 7L
    }

    def 'a credentials failure inside the SDK should surface as that failure'() {
        given:
        def failure = new StorageCredentialsException('Unable to refresh Fovus storage credentials: CLI down', false)
        s3.getObject(_ as GetObjectRequest) >> {
            throw SdkClientException.create('Unable to load credentials', new UncheckedStorageCredentialsException(failure))
        }

        when:
        client.getObject(KEY)

        then:
        def e = thrown(StorageCredentialsException)
        e.is(failure)
    }

    def 'list should return one folder level, files and sub-folders'() {
        given:
        def modified = Instant.parse('2026-09-30T12:00:00Z')
        s3.listObjectsV2Paginator(_ as ListObjectsV2Request) >> { ListObjectsV2Request request -> new ListObjectsV2Iterable(s3, request) }
        s3.listObjectsV2(_ as ListObjectsV2Request) >> ListObjectsV2Response.builder()
                .contents(S3Object.builder().key(PREFIX + 'dir/').size(0L).lastModified(modified).build(),
                          S3Object.builder().key(PREFIX + 'dir/a.txt').size(5L).lastModified(modified).build())
                .commonPrefixes(CommonPrefix.builder().prefix(PREFIX + 'dir/sub/').build())
                .isTruncated(false)
                .build()

        when:
        def entries = client.list(PREFIX + 'dir/')

        then:
        entries*.key == [PREFIX + 'dir/', PREFIX + 'dir/a.txt', PREFIX + 'dir/sub/']
        entries*.directory == [true, false, true]
        entries[1].size == 5L
    }

    def 'a directory marker should be an empty object ending with a slash'() {
        when:
        client.putDirectoryMarker(PREFIX + 'ab/cdef')

        then:
        1 * s3.putObject({ PutObjectRequest r -> r.key() == PREFIX + 'ab/cdef/' && r.bucket() == 'bucket' }, _ as RequestBody)
    }

    def 'a slash-less pipeline folder key should be listed with a trailing slash, never as a sibling prefix'() {
        given:
        ListObjectsV2Request sent = null
        s3.listObjectsV2Paginator(_ as ListObjectsV2Request) >> { ListObjectsV2Request request ->
            sent = request
            new ListObjectsV2Iterable(s3, request)
        }
        s3.listObjectsV2(_ as ListObjectsV2Request) >> ListObjectsV2Response.builder().isTruncated(false).build()

        when:
        client.list('pipelines/p-1-user')

        then:
        sent.prefix() == PREFIX

        when:
        sent = null
        client.listAll('pipelines/p-1-user')

        then:
        sent.prefix() == PREFIX
    }

    def 'hasChildren on a slash-less pipeline folder key should ask S3 for the prefix with a trailing slash'() {
        when:
        def found = client.hasChildren('pipelines/p-1-user')

        then:
        1 * s3.listObjectsV2({ ListObjectsV2Request r -> r.prefix() == PREFIX && r.bucket() == 'bucket' }) >> ListObjectsV2Response.builder().keyCount(1).build()
        found
    }

    @Unroll
    def 'a listing of #dirKey should stay out of S3'() {
        when:
        def listed = client.list(dirKey)
        def listedAll = client.listAll(dirKey)
        def found = client.hasChildren(dirKey)

        then:
        listed == []
        listedAll == []
        !found
        0 * s3._

        where:
        dirKey << ['pipelines/p-1-usery', 'pipelines/p-2-user/', 'pipelines', '', PREFIX + '../p-2-user/', PREFIX + '..',
                   'pipelines/tmpx', 'pipelines/collect-filex/', 'pipelines/tmp/../p-2-user/', 'pipelines/tmp/..',
                   'filesx', 'jobsx/', 'files/..', 'jobs/./j-1']
    }

    @Unroll
    def 'a write of #key outside the writable folders should be refused before calling S3'() {
        when:
        client.putObject(key, new byte[0])

        then:
        def e = thrown(AccessDeniedException)
        e.reason == OUTSIDE_REASON
        0 * s3._

        where:
        key << [OTHER_PIPELINE_KEY, 'pipelines/p-1-user2/x', '', 'pipelines', 'jobs/./x', 'jobs/j-1/../x'] +
                DOT_SEGMENT_KEYS + NEAR_SCRATCH_KEYS + NEAR_AREA_KEYS
    }

    @Unroll
    def 'the writable folder named #key should be writable as the folder it stands for'() {
        when:
        client.checkWritable(key)
        client.putDirectoryMarker(key)

        then:
        1 * s3.putObject({ PutObjectRequest r -> r.key() == key + '/' }, _ as RequestBody)
        noExceptionThrown()

        where:
        key << BARE_FOLDER_KEYS
    }

    @Unroll
    def 'a write of #key into files/ should reach S3, as one into the pipeline folder does'() {
        when:
        client.putObject(key, 'x'.bytes)
        client.delete(key)
        client.copy(PREFIX + 'a', key, 1L)
        client.abortMultipart(key, 'upload-1')
        client.checkWritable(key)

        then:
        1 * s3.putObject({ PutObjectRequest r -> r.key() == key && r.bucket() == 'bucket' }, _ as RequestBody)
        1 * s3.deleteObject({ DeleteObjectRequest r -> r.key() == key })
        1 * s3.copyObject({ CopyObjectRequest r -> r.sourceKey() == PREFIX + 'a' && r.destinationKey() == key })
        1 * s3.abortMultipartUpload({ AbortMultipartUploadRequest r -> r.key() == key })
        noExceptionThrown()

        where:
        key << FILES_KEYS
    }

    @Unroll
    def 'a write of #key into jobs/ should be refused as read-only before calling S3'() {
        when:
        action.call(client, key)

        then:
        def e = thrown(AccessDeniedException)
        e.reason == JOBS_REASON
        0 * s3._

        where:
        [key, action] << [JOBS_KEYS, [
                { FovusS3Client c, String k -> c.putObject(k, new byte[0]) },
                { FovusS3Client c, String k -> c.putDirectoryMarker(k) },
                { FovusS3Client c, String k -> c.delete(k) },
                { FovusS3Client c, String k -> c.createMultipart(k) },
                { FovusS3Client c, String k -> c.uploadPart(k, 'upload-1', 1, new byte[0]) },
                { FovusS3Client c, String k -> c.completeMultipart(k, 'upload-1', []) },
                { FovusS3Client c, String k -> c.newOutputStream(k) },
                { FovusS3Client c, String k -> c.uploadFile(java.nio.file.Path.of('missing'), k) },
                { FovusS3Client c, String k -> c.copy(PREFIX + 'a', k, 1L) },
                { FovusS3Client c, String k -> c.checkWritable(k) },
        ]].combinations()
    }

    def 'a copy out of jobs/ and files/ into the pipeline folder or files/ should reach S3'() {
        when:
        client.copy(source, target, 1L)

        then:
        1 * s3.copyObject({ CopyObjectRequest r -> r.sourceKey() == source && r.destinationKey() == target })

        where:
        source          | target
        'jobs/j-1/x'    | 'files/results/x'
        'files/data/x'  | PREFIX + 'x'
        PREFIX + 'x'    | 'files/results/x'
    }

    def 'a copy from outside the readable folders should look like a missing file'() {
        when:
        client.copy(OTHER_PIPELINE_KEY, 'files/results/x', 1L)

        then:
        thrown(NoSuchFileException)
        0 * s3._
    }

    @Unroll
    def "Nextflow's session scratch key #key should be writable and readable, as it is in mount mode"() {
        when:
        client.putObject(key, new byte[0])
        def found = client.head(key)
        def stream = client.newOutputStream(key)
        client.abortMultipart(key, 'upload-1')

        then:
        1 * s3.putObject({ PutObjectRequest r -> r.key() == key }, _ as RequestBody)
        1 * s3.headObject({ HeadObjectRequest r -> r.key() == key }) >> HeadObjectResponse.builder().contentLength(0L).build()
        1 * s3.abortMultipartUpload({ AbortMultipartUploadRequest r -> r.key() == key })
        found != null
        stream != null

        where:
        key << SCRATCH_KEYS
    }

    @Unroll
    def "Nextflow's session scratch folder #folder should be listable"() {
        given:
        s3.listObjectsV2Paginator(_ as ListObjectsV2Request) >> { ListObjectsV2Request request -> new ListObjectsV2Iterable(s3, request) }

        when:
        def found = client.hasChildren(folder)
        client.list(folder)

        then:
        1 * s3.listObjectsV2({ ListObjectsV2Request r -> r.prefix() == folder + '/' && r.maxKeys() == 1 }) >> ListObjectsV2Response.builder().keyCount(1).build()
        1 * s3.listObjectsV2({ ListObjectsV2Request r -> r.prefix() == folder + '/' && r.delimiter() == '/' }) >> ListObjectsV2Response.builder().isTruncated(false).build()
        found

        where:
        folder << ['pipelines/tmp', 'pipelines/collect-file']
    }

    @Unroll
    def 'a read of #key outside the readable folders should look like a missing file'() {
        when:
        def head = client.head(key)

        then:
        head == null
        0 * s3._

        when:
        client.getObject(key)

        then:
        thrown(NoSuchFileException)
        0 * s3._

        where:
        key << [OTHER_PIPELINE_KEY, 'pipelines/p-1-user2/x', ''] + DOT_SEGMENT_KEYS + NEAR_SCRATCH_KEYS + NEAR_AREA_KEYS
    }

    @Unroll
    def 'a read of #key in Fovus storage files/ or jobs/ should reach S3'() {
        when:
        def found = client.head(key)
        def text = client.getObject(key).text

        then:
        1 * s3.headObject({ HeadObjectRequest r -> r.key() == key && r.bucket() == 'bucket' }) >> HeadObjectResponse.builder().contentLength(1L).build()
        1 * s3.getObject({ GetObjectRequest r -> r.key() == key && r.bucket() == 'bucket' }) >>
                new ResponseInputStream<GetObjectResponse>(GetObjectResponse.builder().build(), AbortableInputStream.create(new ByteArrayInputStream('x'.bytes)))
        found.size == 1L
        text == 'x'

        where:
        key << AREA_KEYS
    }

    @Unroll
    def 'the #area area should be listable from its root'() {
        given:
        s3.listObjectsV2Paginator(_ as ListObjectsV2Request) >> { ListObjectsV2Request request -> new ListObjectsV2Iterable(s3, request) }

        when:
        def found = client.hasChildren(area)
        client.list(area)

        then:
        1 * s3.listObjectsV2({ ListObjectsV2Request r -> r.prefix() == area + '/' && r.maxKeys() == 1 }) >> ListObjectsV2Response.builder().keyCount(1).build()
        1 * s3.listObjectsV2({ ListObjectsV2Request r -> r.prefix() == area + '/' && r.delimiter() == '/' }) >> ListObjectsV2Response.builder().isTruncated(false).build()
        found

        where:
        area << ['files', 'jobs']
    }

    @Unroll
    def '#operation outside the writable folders should be refused before calling S3'() {
        when:
        action.call(client)

        then:
        def e = thrown(AccessDeniedException)
        e.reason == OUTSIDE_REASON
        0 * s3._

        where:
        operation           | action
        'checkWritable'     | { FovusS3Client c -> c.checkWritable(OTHER_PIPELINE_KEY) }
        'delete'            | { FovusS3Client c -> c.delete(OTHER_PIPELINE_KEY) }
        'createMultipart'   | { FovusS3Client c -> c.createMultipart(OTHER_PIPELINE_KEY) }
        'uploadPart'        | { FovusS3Client c -> c.uploadPart(OTHER_PIPELINE_KEY, 'upload-1', 1, new byte[0]) }
        'completeMultipart' | { FovusS3Client c -> c.completeMultipart(OTHER_PIPELINE_KEY, 'upload-1', []) }
        'delete (..)'       | { FovusS3Client c -> c.delete(PREFIX + '../p-2-user/x') }
        'uploadPart (..)'   | { FovusS3Client c -> c.uploadPart(PREFIX + '../p-2-user/x', 'upload-1', 1, new byte[0]) }
    }

    def 'abortMultipart outside the writable folders should do nothing and not throw'() {
        when:
        client.abortMultipart(OTHER_PIPELINE_KEY, 'upload-1')
        client.abortMultipart(PREFIX + '../p-2-user/x', 'upload-1')
        client.abortMultipart('jobs/j-1/x', 'upload-1')

        then:
        noExceptionThrown()
        0 * s3._
    }

    def 'abortMultipart inside the pipeline prefix should call S3 and swallow its failure'() {
        when:
        client.abortMultipart(KEY, 'upload-1')

        then:
        1 * s3.abortMultipartUpload({ AbortMultipartUploadRequest r -> r.key() == KEY && r.uploadId() == 'upload-1' }) >> { throw s3Error(500, 'InternalError') }
        noExceptionThrown()
    }

    @Unroll
    def 'a #failure.class.simpleName while the body streams should be an I/O error naming only the key'() {
        given:
        def error = failure
        def body = new InputStream() {
            int served = 0

            @Override
            int read() {
                throw error
            }

            @Override
            int read(byte[] bytes, int offset, int length) {
                if (served > 0) throw error
                served = 1
                return 1
            }
        }
        s3.getObject(_ as GetObjectRequest) >> new ResponseInputStream<GetObjectResponse>(GetObjectResponse.builder().build(),
                                                                                          AbortableInputStream.create(body))

        when:
        client.getObject(KEY).readAllBytes()

        then:
        def e = thrown(IOException)
        e.message == "S3 read failed on ${KEY}: ${failure.class.simpleName}".toString()
        e.cause == null

        where:
        failure << [SdkClientException.create('Unable to execute HTTP request: SECRET-BODY'),
                    AbortedException.create('Thread was interrupted SECRET-BODY')]
    }

    def 'a client error without a credentials cause should be reported without the SDK exception as its cause'() {
        given:
        s3.getObject(_ as GetObjectRequest) >> { throw SdkClientException.create('Unable to execute HTTP request: connect timed out') }

        when:
        client.getObject(KEY)

        then:
        def e = thrown(IOException)
        e.message == "S3 read failed on ${KEY}: Unable to execute HTTP request: connect timed out".toString()
        e.cause == null
    }

    def 'an unexpected failure should be reported by class name without it as the cause'() {
        given:
        s3.getObject(_ as GetObjectRequest) >> { throw new IllegalStateException('body SECRET-BODY') }

        when:
        client.getObject(KEY)

        then:
        def e = thrown(IOException)
        e.message == "S3 read failed on ${KEY}: IllegalStateException".toString()
        e.cause == null
    }

    @Unroll
    def 'a pipeline prefix of #prefix should be rejected'() {
        when:
        new FovusS3Client(s3, s3, 'bucket', prefix, credentials)

        then:
        thrown(IllegalArgumentException)

        where:
        prefix << [null, '', 'pipelines/p-1-user']
    }

    def 'a real client should ask the credentials provider once and surface its failure as itself'() {
        given:
        def calls = new AtomicInteger()
        def provider = { ->
            calls.incrementAndGet()
            throw new UncheckedStorageCredentialsException(new StorageCredentialsException('down', true))
        } as AwsCredentialsProvider
        def keys = new SessionKeys('AKIA-TEST', 'secret-test', 'token-test', Instant.now().plusSeconds(3600))
        RefreshingStorageCredentials refreshing = Mock()
        refreshing.get() >> new StorageCredentials('bucket', 'us-east-2', PREFIX, keys, keys)
        refreshing.readProvider() >> provider
        refreshing.writeProvider() >> provider
        def real = FovusS3Client.create(refreshing)

        when:
        real.head(PREFIX + 'x')

        then:
        def e = thrown(StorageCredentialsException)
        e.message == 'down'
        calls.get() == 1
    }
}
