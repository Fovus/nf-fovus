package fovus.plugin.s3

import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.AccessDeniedException
import java.nio.file.Files
import java.nio.file.Path

/** File transfers and streamed writes: handed to the transfers ({@link S3Transfers}) under the guard, their failures mapped like any S3 call. */
class S3TransferTest extends Specification {

    static final String KEY = 'pipelines/p-1-user/fovus-work/ab/cdef/data.bin'

    @TempDir
    Path tempDir

    S3Client s3 = Mock()
    S3Transfers transfers = Mock()
    RefreshingStorageCredentials credentials = Mock()
    FovusS3Client client = new FovusS3Client(s3, s3, transfers, 'bucket', 'pipelines/p-1-user/', credentials)

    def 'uploadFile should hand the file to the transfer manager under the guard'() {
        given:
        def file = Files.write(tempDir.resolve('data.bin'), 'hello'.bytes)

        when:
        client.uploadFile(file, KEY)

        then:
        1 * transfers.uploadFile(file, KEY)
        0 * s3._
    }

    def 'an expired token during a file upload should refresh and retry the upload once'() {
        given:
        def file = Files.write(tempDir.resolve('data.bin'), 'hello'.bytes)

        when:
        client.uploadFile(file, KEY)

        then:
        1 * transfers.uploadFile(file, KEY) >> { throw FovusS3ClientTest.s3Error(400, 'ExpiredToken') }

        then:
        1 * credentials.forceRefresh()

        then:
        1 * transfers.uploadFile(file, KEY)
    }

    def 'a file upload failing twice with an expired token should fail, and say so by code only'() {
        given:
        def file = Files.write(tempDir.resolve('data.bin'), 'hello'.bytes)

        when:
        client.uploadFile(file, KEY)

        then:
        2 * transfers.uploadFile(file, KEY) >> { throw FovusS3ClientTest.s3Error(400, 'ExpiredToken') }
        1 * credentials.forceRefresh()
        def e = thrown(IOException)
        e.message == "S3 write failed on ${KEY}: ExpiredToken (HTTP 400, request req-1)".toString()
        e.cause == null
    }

    def 'a download should land in a temp file next to the target and move into place'() {
        given:
        def target = tempDir.resolve('out/data.bin')
        Path landed = null
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(5L).build()

        when:
        client.downloadFile(KEY, target)

        then:
        1 * transfers.downloadFile(KEY, _ as Path) >> { String key, Path destination ->
            landed = destination
            Files.write(destination, 'hello'.bytes)
        }
        landed.parent == target.parent
        landed.fileName.toString() ==~ /\.data\.bin\.[0-9a-f-]{36}\.part/
        Files.readAllBytes(target) == 'hello'.bytes
        Files.list(target.parent).withCloseable { it.count() } == 1
    }

    def 'an expired token during a download should refresh and download again into the same temp file'() {
        given:
        def target = tempDir.resolve('out/data.bin')
        List<Path> destinations = []
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(5L).build()

        when:
        client.downloadFile(KEY, target)

        then:
        1 * transfers.downloadFile(KEY, _ as Path) >> { String key, Path destination ->
            destinations << destination
            Files.write(destination, 'he'.bytes)
            throw FovusS3ClientTest.s3Error(400, 'ExpiredToken')
        }

        then:
        1 * credentials.forceRefresh()

        then:
        1 * transfers.downloadFile(KEY, _ as Path) >> { String key, Path destination ->
            destinations << destination
            Files.write(destination, 'hello'.bytes)
        }
        destinations[0] == destinations[1]
        Files.readAllBytes(target) == 'hello'.bytes
    }

    def 'newOutputStream should hand back the upload stream of the transfers under the guard'() {
        given:
        def upload = new RecordingUploadStream()

        when:
        def out = client.newOutputStream(KEY)
        out.write('hel'.bytes)
        out.write((int) ('l' as char))
        out.write('xox'.bytes, 1, 1)
        out.close()

        then:
        1 * transfers.newUploadStream(KEY) >> upload
        0 * s3._
        upload.published
        upload.text == 'hello'
    }

    def 'abort should discard the upload stream of the transfers'() {
        given:
        def upload = new RecordingUploadStream()
        transfers.newUploadStream(KEY) >> upload

        when:
        def out = client.newOutputStream(KEY)
        out.write('partial'.bytes)
        out.abort()
        out.close()

        then:
        upload.aborted
        !upload.published
    }

    def 'a failure of the upload stream on #operation should be reported like any S3 failure: #expected'() {
        given:
        S3UploadStream upload = Mock()
        transfers.newUploadStream(KEY) >> upload
        def out = client.newOutputStream(KEY)

        when:
        action.call(out)

        then:
        1 * upload./write|close/(*_) >> { throw failure }
        def e = thrown(IOException)
        e.class == type
        e.message == expected
        e.cause == null
        !e.message.contains('SECRET-BODY')

        where:
        operation | action                                  | failure                                                               | type                  | expected
        'write'   | { OutputStream o -> o.write(new byte[3]) } | FovusS3ClientTest.s3Error(500, 'InternalError')                     | IOException           | "S3 write failed on ${KEY}: InternalError (HTTP 500, request req-1)".toString()
        'write'   | { OutputStream o -> o.write(1) }           | FovusS3ClientTest.s3Error(500, 'InternalError')                     | IOException           | "S3 write failed on ${KEY}: InternalError (HTTP 500, request req-1)".toString()
        'close'   | { OutputStream o -> o.close() }            | FovusS3ClientTest.s3Error(403, 'AccessDenied')                      | AccessDeniedException | "fovus:///fovus-storage/${KEY}: Fovus storage credentials don't allow write on ${KEY} (write token)".toString()
        'close'   | { OutputStream o -> o.close() }            | SdkClientException.create('Unable to execute HTTP request: timed out') | IOException        | "S3 write failed on ${KEY}: Unable to execute HTTP request: timed out".toString()
        'close'   | { OutputStream o -> o.close() }            | new IOException('S3 write failed on x: CancellationException')        | IOException          | 'S3 write failed on x: CancellationException'
    }

    def 'a credentials failure surfacing when the upload stream closes should be that failure'() {
        given:
        def failure = new StorageCredentialsException('Unable to refresh Fovus storage credentials: CLI down', false)
        S3UploadStream upload = Mock()
        transfers.newUploadStream(KEY) >> upload
        def out = client.newOutputStream(KEY)

        when:
        out.close()

        then:
        1 * upload.close() >> { throw SdkClientException.create('Unable to load credentials', new UncheckedStorageCredentialsException(failure)) }
        def e = thrown(StorageCredentialsException)
        e.is(failure)
    }

    def 'closing the client should close the transfers'() {
        when:
        client.close()

        then:
        1 * transfers.close()
    }
}
