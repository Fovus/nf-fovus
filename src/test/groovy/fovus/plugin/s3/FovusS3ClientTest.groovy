package fovus.plugin.s3

import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import spock.lang.Specification

import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import java.time.Instant

class FovusS3ClientTest extends Specification {

    static final String PREFIX = 'pipelines/p-1-user/'
    static final String KEY = PREFIX + 'fovus-work/ab/cdef/.exitcode'

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

    def 'writes outside the pipeline prefix should be refused before calling S3'() {
        when:
        client.putObject('pipelines/p-2-user/x', new byte[0])

        then:
        def e = thrown(AccessDeniedException)
        e.reason == 'Refusing to write outside pipelines/p-1-user/'
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
}
