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
 * S3 ({@link FakeS3Server}): retries of transient failures, what an abandoned stream leaves, the edge sizes, and how
 * many requests wait for what. Parts are 1 MiB rather than S3's minimum of 5 MiB, which the fake does not enforce,
 * to keep the test fast. Each feature writes its own key, so a request the SDK sends late cannot reach the next one.
 */
class TransferManagerTransfersSdkTest extends Specification {

    static final long PART = 1024 * 1024

    @Shared FakeS3Server s3 = new FakeS3Server()
    @Shared TransferManagerTransfers transfers

    @TempDir
    Path tempDir

    String key

    def setupSpec() {
        transfers = TransferManagerTransfers.fromBuilders(clientBuilder(), clientBuilder(), 'bucket', PART)
    }

    def cleanupSpec() {
        transfers?.close()
        s3.close()
    }

    def setup() {
        s3.reset()
        key = "pipelines/p-1-user/fovus-work/${UUID.randomUUID()}/data.bin".toString()
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

    /** Write {@code data} through an upload stream of {@code via} in 64 KiB writes, then close it. */
    private void stream(byte[] data, TransferManagerTransfers via = transfers) {
        final out = via.newUploadStream(key)
        for (int offset = 0; offset < data.length; offset += 65536) out.write(data, offset, Math.min(65536, data.length - offset))
        out.close()
    }

    /** True once {@code condition} holds, polled for up to 10 seconds. */
    private static boolean eventually(Closure<Boolean> condition) {
        final deadline = System.nanoTime() + 10_000_000_000L
        while (!condition.call()) {
            if (System.nanoTime() > deadline) return false
            Thread.sleep(20)
        }
        return true
    }

    /** True when {@code condition} holds throughout the next {@code millis}: a short window for a wrong request to show. */
    private static boolean staysTrue(long millis, Closure<Boolean> condition) {
        final deadline = System.nanoTime() + millis * 1_000_000L
        while (System.nanoTime() < deadline) {
            if (!condition.call()) return false
            Thread.sleep(20)
        }
        return condition.call()
    }

    private int parts() {
        return s3.requestsFor(key).count { String request -> request.startsWith('PART ') }
    }

    def 'a small stream whose PutObject fails once should be sent again'() {
        given:
        s3.failOnce("PUT ${key}")

        when:
        stream('hello'.bytes)

        then:
        s3.requestsFor(key) == ["PUT ${key}", "PUT ${key}"]
        s3.objects[key] == 'hello'.bytes
    }

    def 'a stream of several parts whose second part fails once should send that part again'() {
        given:
        def data = content(2 * PART + PART.intdiv(2))
        s3.failOnce("PART 2 ${key}")

        when:
        stream(data)

        then:
        s3.requestsFor(key).count { it == "PART 2 ${key}" } == 2
        parts() == 4
        s3.requestsFor(key).first() == "CREATE ${key}"
        s3.requestsFor(key).last() == "COMPLETE ${key}"
        s3.objects[key] == data
    }

    def 'a file of several parts whose second part fails once should send that part again'() {
        given:
        def data = content(2 * PART + PART.intdiv(2))
        def file = Files.write(tempDir.resolve('data.bin'), data)
        s3.failOnce("PART 2 ${key}")

        when:
        transfers.uploadFile(file, key)

        then:
        s3.requestsFor(key).count { it == "PART 2 ${key}" } == 2
        s3.requestsFor(key).last() == "COMPLETE ${key}"
        s3.objects[key] == data
    }

    def 'a small file whose PutObject fails once should be sent again'() {
        given:
        def file = Files.write(tempDir.resolve('data.bin'), 'hello'.bytes)
        s3.failOnce("PUT ${key}")

        when:
        transfers.uploadFile(file, key)

        then:
        s3.requestsFor(key) == ["PUT ${key}", "PUT ${key}"]
        s3.objects[key] == 'hello'.bytes
    }

    def 'a file upload should have at most four parts in flight'() {
        given:
        s3.holdParts()
        def data = content(10 * PART)
        def file = Files.write(tempDir.resolve('data.bin'), data)
        Throwable failure = null
        def uploader = Thread.start {
            try {
                transfers.uploadFile(file, key)
            }
            catch (Throwable t) {
                failure = t
            }
        }

        expect: 'four parts reach S3 and wait there; no fifth one is sent meanwhile'
        eventually { parts() == TransferManagerTransfers.MAX_IN_FLIGHT_PARTS }
        staysTrue(300) { parts() == TransferManagerTransfers.MAX_IN_FLIGHT_PARTS }

        when:
        s3.releaseParts()
        uploader.join(10000)

        then:
        !uploader.alive
        failure == null
        parts() == 10
        s3.objects[key] == data
    }

    def 'a request waiting for a free connection should wait past the SDK default of 10 s, then go through'() {
        given: 'one connection, taken by a part that S3 holds'
        def narrow = TransferManagerTransfers.fromBuilders(clientBuilder(), clientBuilder(), 'bucket', PART, 1)
        s3.holdParts()
        def data = content(2 * PART)
        def file = Files.write(tempDir.resolve('data.bin'), data)
        def failures = Collections.synchronizedList([])
        def uploader = Thread.start {
            try {
                narrow.uploadFile(file, key)
            }
            catch (Throwable t) {
                failures << t
            }
        }
        assert eventually { parts() == 1 }
        def smallKey = key + '.small'

        when: 'a small write waits for the connection longer than a 10 s connection acquire would allow'
        def writer = Thread.start {
            try {
                final out = narrow.newUploadStream(smallKey)
                out.write('hello'.bytes)
                out.close()
            }
            catch (Throwable t) {
                failures << t
            }
        }
        Thread.sleep(11_000)

        then:
        failures.isEmpty()
        uploader.alive
        writer.alive

        when:
        s3.releaseParts()
        uploader.join(10000)
        writer.join(10000)

        then:
        failures.isEmpty()
        s3.objects[key] == data
        s3.objects[smallKey] == 'hello'.bytes

        cleanup:
        s3.releaseParts()
        narrow?.close()
    }

    def 'a download whose GET fails once should be fetched again, whole, with one GET'() {
        given:
        def data = content(2 * PART + 10)
        s3.objects[key] = data
        s3.failOnce("GET ${key}")
        def destination = Files.createFile(tempDir.resolve('.data.bin.part'))

        when:
        transfers.downloadFile(key, destination)

        then:
        s3.requestsFor(key) == ["GET ${key}", "GET ${key}"]
        Files.readAllBytes(destination) == data
    }

    def 'a download should replace a longer content of the destination entirely'() {
        given:
        s3.objects[key] = 'hello'.bytes
        def destination = Files.write(tempDir.resolve('.data.bin.part'), 'a longer leftover'.bytes)

        when:
        transfers.downloadFile(key, destination)

        then:
        Files.readAllBytes(destination) == 'hello'.bytes
    }

    @Requires({ FileSystems.default.supportedFileAttributeViews().contains('posix') })
    def 'a download through the client should land whole with the default permissions, and leave no temp file'() {
        given:
        def data = content(PART + 10)
        s3.objects[key] = data
        def directory = Files.createDirectories(tempDir.resolve('downloads'))
        def reference = Files.createFile(tempDir.resolve('reference.bin'))
        def target = directory.resolve('data.bin')

        when:
        clientOver(data.length).downloadFile(key, target)

        then:
        Files.readAllBytes(target) == data
        Files.getPosixFilePermissions(target) == Files.getPosixFilePermissions(reference)
        Files.list(directory).withCloseable { it.count() } == 1
    }

    def 'a download through the client that S3 denies should leave no file and no temp file'() {
        given:
        s3.objects[key] = 'hello'.bytes
        s3.deny("GET ${key}")
        def directory = Files.createDirectories(tempDir.resolve('downloads'))

        when:
        clientOver(5).downloadFile(key, directory.resolve('data.bin'))

        then:
        def e = thrown(AccessDeniedException)
        e.reason == "Fovus storage credentials don't allow read on ${key} (read token)".toString()
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
        def out = transfers.newUploadStream(key)
        out.write(content(written))

        when:
        out.abort()
        out.close()

        then: 'nothing completes, even once the SDK is done with the parts it had'
        staysTrue(500) { !s3.objects.containsKey(key) && !s3.requestsFor(key).any { it.startsWith('COMPLETE ') || it.startsWith('PUT ') } }

        where:
        written << [5L, 2 * PART + 10]
    }

    def 'an empty stream should become one empty object'() {
        when:
        stream(new byte[0])

        then:
        s3.requestsFor(key) == ["PUT ${key}"]
        s3.objects[key].length == 0
    }

    def 'a stream of exactly one part should become one PutObject'() {
        given:
        def data = content(PART)

        when:
        stream(data)

        then: 'SDK 2.55 sends one PutObject; 2.31 made it a multipart upload whose second part was empty'
        s3.requestsFor(key) == ["PUT ${key}"]
        s3.objects[key] == data
    }

    def 'a stream should hold at most two parts in memory while S3 is slow to take them'() {
        given:
        s3.holdParts()
        def out = transfers.newUploadStream(key)
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

        when: 'the two parts the buffer holds reach S3, which keeps them, and the writer waits for room'
        def blocked = eventually { parts() == 2 && writer.state == Thread.State.WAITING }

        then:
        blocked
        written.get() <= 2 * PART + chunk.length
        written.get() >= 2 * PART

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
        s3.deny("PART 1 ${key}")
        def out = client.newOutputStream(key)
        def chunk = new byte[64 * 1024]

        when: 'the writer keeps writing until the denied part fails the upload'
        for (int i = 0; i < 1000; i++) out.write(chunk)

        then:
        def e = thrown(AccessDeniedException)
        e.reason == "Fovus storage credentials don't allow write on ${key} (write token)".toString()
        !e.message.contains('SECRET-BODY')
        e.cause == null

        when: 'the failed upload was discarded: closing does not publish'
        out.close()

        then:
        noExceptionThrown()
        !s3.objects.containsKey(key)
    }
}
