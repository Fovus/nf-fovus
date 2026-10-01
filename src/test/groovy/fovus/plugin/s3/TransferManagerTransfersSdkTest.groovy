package fovus.plugin.s3

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.AccessDeniedException
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong

/**
 * {@link TransferManagerTransfers} on the real SDK clients, built as {@code create} builds them, against a local fake
 * S3 ({@link FakeS3Server}): retries of transient failures, what an abandoned stream leaves, and the edge sizes. Parts
 * are 1 MiB rather than S3's minimum of 5 MiB, which the fake does not enforce, to keep the test fast.
 */
class TransferManagerTransfersSdkTest extends Specification {

    static final long PART = 1024 * 1024
    static final String KEY = 'pipelines/p-1-user/fovus-work/ab/cdef/data.bin'

    @Shared FakeS3Server s3 = new FakeS3Server()
    @Shared TransferManagerTransfers transfers

    @TempDir
    Path tempDir

    def setupSpec() {
        transfers = TransferManagerTransfers.fromBuilders(clientBuilder(), clientBuilder(), 'bucket', PART)
    }

    def cleanupSpec() {
        transfers?.close()
        s3.close()
    }

    def setup() {
        s3.reset()
    }

    /** The Fovus client settings, pointed at the fake. */
    private S3AsyncClientBuilder clientBuilder() {
        final credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create('AKIA-TEST', 'secret-test'))
        return FovusS3Client.withFovusSettings(S3AsyncClient.builder(), 'us-east-1', credentials, [])
                .endpointOverride(s3.endpoint)
                .forcePathStyle(true)
    }

    private static byte[] content(long size) {
        final bytes = new byte[(int) size]
        new Random(size).nextBytes(bytes)
        return bytes
    }

    /** Write {@code data} through an upload stream in 64 KiB writes, then close it. */
    private void stream(byte[] data) {
        final out = transfers.newUploadStream(KEY)
        for (int offset = 0; offset < data.length; offset += 65536) out.write(data, offset, Math.min(65536, data.length - offset))
        out.close()
    }

    def 'a small stream whose PutObject fails once should be sent again'() {
        given:
        s3.failOnce("PUT ${KEY}")

        when:
        stream('hello'.bytes)

        then:
        s3.requests == ["PUT ${KEY}", "PUT ${KEY}"]
        s3.objects[KEY] == 'hello'.bytes
    }

    def 'a stream of several parts whose second part fails once should send that part again'() {
        given:
        def data = content(2 * PART + PART.intdiv(2))
        s3.failOnce("PART 2 ${KEY}")

        when:
        stream(data)

        then:
        s3.requests.count { it == "PART 2 ${KEY}" } == 2
        s3.requests.count { it.startsWith('PART ') } == 4
        s3.requests.first() == "CREATE ${KEY}"
        s3.requests.last() == "COMPLETE ${KEY}"
        s3.objects[KEY] == data
    }

    def 'a file of several parts whose second part fails once should send that part again'() {
        given:
        def data = content(2 * PART + PART.intdiv(2))
        def file = Files.write(tempDir.resolve('data.bin'), data)
        s3.failOnce("PART 2 ${KEY}")

        when:
        transfers.uploadFile(file, KEY)

        then:
        s3.requests.count { it == "PART 2 ${KEY}" } == 2
        s3.requests.last() == "COMPLETE ${KEY}"
        s3.objects[KEY] == data
    }

    def 'a small file whose PutObject fails once should be sent again'() {
        given:
        def file = Files.write(tempDir.resolve('data.bin'), 'hello'.bytes)
        s3.failOnce("PUT ${KEY}")

        when:
        transfers.uploadFile(file, KEY)

        then:
        s3.requests == ["PUT ${KEY}", "PUT ${KEY}"]
        s3.objects[KEY] == 'hello'.bytes
    }

    def 'a download whose GET fails once should be fetched again, whole, with one GET'() {
        given:
        def data = content(2 * PART + 10)
        s3.objects[KEY] = data
        s3.failOnce("GET ${KEY}")
        def destination = Files.createFile(tempDir.resolve('.data.bin.part'))

        when:
        transfers.downloadFile(KEY, destination)

        then:
        s3.requests == ["GET ${KEY}", "GET ${KEY}"]
        Files.readAllBytes(destination) == data
    }

    @Requires({ FileSystems.default.supportedFileAttributeViews().contains('posix') })
    def 'a download through the client should land whole with the default permissions, and leave no temp file'() {
        given:
        def data = content(PART + 10)
        s3.objects[KEY] = data
        def directory = Files.createDirectories(tempDir.resolve('downloads'))
        def reference = Files.createFile(tempDir.resolve('reference.bin'))
        def target = directory.resolve('data.bin')

        when:
        clientOver(data.length).downloadFile(KEY, target)

        then:
        Files.readAllBytes(target) == data
        Files.getPosixFilePermissions(target) == Files.getPosixFilePermissions(reference)
        Files.list(directory).withCloseable { it.count() } == 1
    }

    def 'a download through the client that S3 denies should leave no file and no temp file'() {
        given:
        s3.objects[KEY] = 'hello'.bytes
        s3.deny("GET ${KEY}")
        def directory = Files.createDirectories(tempDir.resolve('downloads'))

        when:
        clientOver(5).downloadFile(KEY, directory.resolve('data.bin'))

        then:
        def e = thrown(AccessDeniedException)
        e.reason == "Fovus storage credentials don't allow read on ${KEY} (read token)".toString()
        !e.message.contains('SECRET-BODY')
        Files.list(directory).withCloseable { it.count() } == 0
    }

    /** A client on these transfers, whose sync client finds every object {@code size} bytes long. */
    private FovusS3Client clientOver(long size) {
        S3Client sync = Stub()
        sync.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(size).build()
        return new FovusS3Client(sync, sync, transfers, 'bucket', 'pipelines/p-1-user/', null)
    }

    def 'a stream aborted after #written bytes should publish nothing'() {
        given:
        def out = transfers.newUploadStream(KEY)
        out.write(content(written))

        when:
        out.abort()
        out.close()
        // the SDK may still be sending the parts it had; give it the time to finish what it would
        Thread.sleep(500)

        then:
        !s3.objects.containsKey(KEY)
        !s3.requests.contains("COMPLETE ${KEY}".toString())
        !s3.requests.contains("PUT ${KEY}".toString())

        where:
        written << [5L, 2 * PART + 10]
    }

    def 'an empty stream should become one empty object'() {
        when:
        stream(new byte[0])

        then:
        s3.requests == ["PUT ${KEY}"]
        s3.objects[KEY].length == 0
    }

    def 'a stream of exactly one part should become one PutObject'() {
        given:
        def data = content(PART)

        when:
        stream(data)

        then: 'SDK 2.55 sends one PutObject; 2.31 made it a multipart upload whose second part was empty'
        s3.requests == ["PUT ${KEY}"]
        s3.objects[KEY] == data
    }

    def 'a stream should hold about two parts in memory while S3 is slow to take them'() {
        given:
        s3.holdParts()
        def out = transfers.newUploadStream(KEY)
        def chunk = new byte[64 * 1024]
        def written = new AtomicLong()
        Throwable failure = null
        def writer = Thread.start {
            try {
                while (written.get() < 20 * PART) {
                    out.write(chunk)
                    written.addAndGet(chunk.length)
                }
            }
            catch (Throwable t) {
                failure = t
            }
        }

        when: 'the writer blocks once the parts in flight fill the buffer'
        long before = -1
        for (int i = 0; i < 100 && written.get() != before; i++) {
            before = written.get()
            Thread.sleep(200)
        }

        then:
        writer.alive
        written.get() <= 2 * PART + chunk.length
        written.get() >= PART

        when: 'abort releases the blocked writer'
        out.abort()
        writer.join(10000)

        then:
        !writer.alive
        failure != null

        cleanup:
        s3.releaseParts()
    }

    def 'a write after S3 denied the upload should fail as access denied, naming the write token and not the S3 body'() {
        given:
        def client = new FovusS3Client(Mock(S3Client), Mock(S3Client), transfers, 'bucket', 'pipelines/p-1-user/', null)
        s3.deny("PART 1 ${KEY}")
        def out = client.newOutputStream(KEY)
        def chunk = new byte[64 * 1024]

        when: 'the writer keeps writing until the denied part fails the upload'
        for (int i = 0; i < 1000; i++) out.write(chunk)

        then:
        def e = thrown(AccessDeniedException)
        e.reason == "Fovus storage credentials don't allow write on ${KEY} (write token)".toString()
        !e.message.contains('SECRET-BODY')
        e.cause == null

        when: 'the failed upload was discarded: closing does not publish'
        out.close()

        then:
        noExceptionThrown()
        !s3.objects.containsKey(KEY)
    }
}
