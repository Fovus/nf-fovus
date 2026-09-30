package fovus.plugin

import fovus.plugin.nio.PipelinesTestSupport
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.S3Entry
import fovus.plugin.s3.StorageCredentialsException
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path

class ExitStatusReaderTest extends Specification {

    static final String TASK_KEY = 'pipelines/p-1-user/fovus-work/ab/cdef'

    @TempDir
    Path tempDir

    def reader = new ExitStatusReader()

    /** A direct-mode task folder whose .exitcode reads return or throw as {@code response} says. */
    private Path directTaskDir(Closure response) {
        final client = Stub(FovusS3Client) {
            list(_) >> [new S3Entry(TASK_KEY + '/', 0L, null, true)]
            // a closure literal is what makes Spock run it per call; a closure held in a variable would be returned as the value
            getObject(*_) >> { response() }
        }
        return PipelinesTestSupport.fileSystem(client).getPath('/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef')
    }

    def 'a local exit file should be read as before'() {
        given:
        def exitFile = Files.writeString(tempDir.resolve('.exitcode'), '0\n')

        expect:
        reader.read(exitFile, tempDir) == 0
        reader.read(tempDir.resolve('missing'), tempDir) == Integer.MAX_VALUE
        reader.read(Files.writeString(tempDir.resolve('empty'), ''), tempDir) == Integer.MAX_VALUE
        reader.failure == null
    }

    def 'a direct-mode exit file should be read from Fovus storage'() {
        given:
        def taskDir = directTaskDir { new ByteArrayInputStream('1\n'.bytes) }

        expect:
        reader.read(taskDir.resolve('.exitcode'), taskDir) == 1
    }

    def 'a temporary storage error should defer until the limit, then give up'() {
        given:
        def taskDir = directTaskDir { throw new IOException('connection reset') }
        def exitFile = taskDir.resolve('.exitcode')

        expect:
        (1..<ExitStatusReader.MAX_CONSECUTIVE_STORAGE_FAILURES).every { reader.read(exitFile, taskDir) == null }
        reader.read(exitFile, taskDir) == Integer.MAX_VALUE
        reader.failure.message == 'connection reset'
    }

    def 'an S3 failure while the exit file streams should defer, not count as a missing exit status'() {
        given: 'a real client, whose GetObject body breaks off part way'
        def s3 = Mock(S3Client)
        s3.listObjectsV2Paginator(_ as ListObjectsV2Request) >> { ListObjectsV2Request request -> new ListObjectsV2Iterable(s3, request) }
        s3.listObjectsV2(_ as ListObjectsV2Request) >> ListObjectsV2Response.builder()
                .contents(S3Object.builder().key(TASK_KEY + '/.exitcode').size(2L).build()).isTruncated(false).build()
        s3.getObject(_ as GetObjectRequest) >> {
            final body = new InputStream() {
                @Override
                int read() { throw SdkClientException.create('Unable to execute HTTP request: Connection reset') }
            }
            new ResponseInputStream<GetObjectResponse>(GetObjectResponse.builder().build(), AbortableInputStream.create(body))
        }
        def client = new FovusS3Client(s3, s3, 'bucket', 'pipelines/p-1-user/', null)
        def taskDir = PipelinesTestSupport.fileSystem(client).getPath('/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef')

        expect:
        reader.read(taskDir.resolve('.exitcode'), taskDir) == null
        reader.failure == null
    }

    def 'credentials that can no longer be refreshed should fail at once'() {
        given:
        def taskDir = directTaskDir { throw new StorageCredentialsException('not signed in', false) }

        expect:
        reader.read(taskDir.resolve('.exitcode'), taskDir) == Integer.MAX_VALUE
        reader.failure.message == 'not signed in'
    }

    def 'a missing direct-mode exit file should be read as missing'() {
        given:
        def taskDir = directTaskDir { throw new NoSuchFileException('.exitcode') }

        expect:
        reader.read(taskDir.resolve('.exitcode'), taskDir) == Integer.MAX_VALUE
        reader.failure == null
    }
}
