package fovus.plugin.s3

import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Parallel transfers: parts read from and written to the file directly, on one bounded pool per client. */
class S3TransferTest extends Specification {

    static final String KEY = 'pipelines/p-1-user/fovus-work/ab/cdef/data.bin'
    static final int PART = 1024

    @TempDir
    Path tempDir

    S3Client s3 = Mock()
    FovusS3Client client = new FovusS3Client(s3, s3, 'bucket', 'pipelines/p-1-user/', null, PART)

    def cleanup() {
        client.shutdownTransfers()
    }

    private static byte[] content(int size) {
        final bytes = new byte[size]
        for (int i = 0; i < size; i++) bytes[i] = (byte) (i * 31 + 7)
        return bytes
    }

    private static int[] range(GetObjectRequest request) {
        final bounds = request.range().replace('bytes=', '').split('-')
        return [Integer.parseInt(bounds[0]), Integer.parseInt(bounds[1])] as int[]
    }

    private static ResponseInputStream<GetObjectResponse> responseStream(byte[] bytes) {
        return new ResponseInputStream<GetObjectResponse>(GetObjectResponse.builder().build(),
                AbortableInputStream.create(new ByteArrayInputStream(bytes)))
    }

    def 'a file upload should stream each part from its slice of the file, again on a retry'() {
        given:
        def bytes = content(2 * PART + 100)
        def file = Files.write(tempDir.resolve('data.bin'), bytes)
        def bodies = new ConcurrentHashMap<Integer, List<byte[]>>()

        when:
        client.uploadFile(file, KEY)

        then:
        1 * s3.createMultipartUpload(_ as CreateMultipartUploadRequest) >> CreateMultipartUploadResponse.builder().uploadId('u-1').build()
        3 * s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> { UploadPartRequest request, RequestBody body ->
            final provider = body.contentStreamProvider()
            bodies[request.partNumber()] = [provider.newStream().bytes, provider.newStream().bytes, body.optionalContentLength().get()]
            UploadPartResponse.builder().eTag("e${request.partNumber()}").build()
        }
        1 * s3.completeMultipartUpload({ CompleteMultipartUploadRequest r -> r.multipartUpload().parts()*.partNumber() == [1, 2, 3] })

        and:
        bodies.keySet() == [1, 2, 3] as Set
        (1..3).every { int number ->
            final expected = Arrays.copyOfRange(bytes, (number - 1) * PART, Math.min(number * PART, bytes.length))
            bodies[number][0] == expected && bodies[number][1] == expected && bodies[number][2] == (long) expected.length
        }
    }

    def 'a part body should be read from the file when it is sent, not held in memory beforehand'() {
        given:
        def file = Files.write(tempDir.resolve('data.bin'), new byte[2 * PART])
        def sent = new CopyOnWriteArrayList<byte[]>()

        when:
        client.uploadFile(file, KEY)

        then:
        1 * s3.createMultipartUpload(_) >> CreateMultipartUploadResponse.builder().uploadId('u-1').build()
        2 * s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> { UploadPartRequest request, RequestBody body ->
            // the file changes after the part was handed over, before the SDK reads the body
            synchronized (S3TransferTest) {
                Files.write(file, content(2 * PART))
                sent << body.contentStreamProvider().newStream().bytes
            }
            UploadPartResponse.builder().eTag('e').build()
        }
        1 * s3.completeMultipartUpload(_)
        sent.every { byte[] part -> part.any { it != 0 } }
    }

    def 'a ranged download should stream each range straight into its place in the file'() {
        given:
        def bytes = content(2 * PART + 100)
        def target = tempDir.resolve('out/data.bin')
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength((long) bytes.length).build()

        when:
        client.downloadFile(KEY, target)

        then:
        3 * s3.getObject(_ as GetObjectRequest) >> { GetObjectRequest request ->
            final bounds = range(request)
            responseStream(Arrays.copyOfRange(bytes, bounds[0], bounds[1] + 1))
        }
        0 * s3.getObjectAsBytes(_)
        Files.readAllBytes(target) == bytes
    }

    def 'a range that ends early should fail the download and leave no file behind'() {
        given:
        def target = tempDir.resolve('out/data.bin')
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(3L * PART).build()
        s3.getObject(_ as GetObjectRequest) >> { GetObjectRequest request -> responseStream(new byte[10]) }

        when:
        client.downloadFile(KEY, target)

        then:
        def e = thrown(IOException)
        e.message.startsWith("S3 read failed on ${KEY}")
        Files.list(tempDir.resolve('out')).withCloseable { it.count() } == 0
    }

    def "one client's transfers should share one bounded pool of daemon threads"() {
        given:
        def files = (1..4).collect { Files.write(tempDir.resolve("in-${it}.bin"), content(3 * PART)) }
        def running = new AtomicInteger()
        def maxRunning = new AtomicInteger()
        def threads = ConcurrentHashMap.<Thread> newKeySet()
        def part = { ->
            threads << Thread.currentThread()
            final now = running.incrementAndGet()
            synchronized (maxRunning) { if (now > maxRunning.get()) maxRunning.set(now) }
            Thread.sleep(20)
            running.decrementAndGet()
        }
        s3.createMultipartUpload(_) >> CreateMultipartUploadResponse.builder().uploadId('u-1').build()
        s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> { part(); UploadPartResponse.builder().eTag('e').build() }
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(3L * PART).build()
        s3.getObject(_ as GetObjectRequest) >> { GetObjectRequest request ->
            part()
            final bounds = range(request)
            responseStream(new byte[bounds[1] - bounds[0] + 1])
        }
        def callers = Executors.newFixedThreadPool(files.size() + 1)

        when: 'four uploads and a download run at once, from five caller threads'
        def work = files.collect { Path file -> callers.submit({ client.uploadFile(file, KEY) } as Runnable) }
        work << callers.submit({ client.downloadFile(KEY, tempDir.resolve('out.bin')) } as Runnable)
        work.each { it.get(30, TimeUnit.SECONDS) }

        then:
        maxRunning.get() <= FovusS3Client.TRANSFER_THREADS
        threads.size() <= FovusS3Client.TRANSFER_THREADS
        threads.every { Thread thread -> thread.daemon && thread.name.startsWith('fovus-s3-transfer-') }

        cleanup:
        callers.shutdownNow()
    }

    def 'shutting the transfers down should stop the pool'() {
        given:
        def file = Files.write(tempDir.resolve('data.bin'), content(2 * PART))
        s3.createMultipartUpload(_) >> CreateMultipartUploadResponse.builder().uploadId('u-1').build()
        s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> UploadPartResponse.builder().eTag('e').build()
        client.uploadFile(file, KEY)

        when:
        client.shutdownTransfers()

        then:
        client.@transfers.awaitTermination(5, TimeUnit.SECONDS)
    }

    def 'the part size should grow only when a file would need more than 10,000 parts'() {
        given:
        final int part = FovusS3Client.DEFAULT_PART_SIZE

        expect:
        FovusS3Client.uploadPartSize(size, part) == expected
        Math.ceil(size / (double) FovusS3Client.uploadPartSize(size, part)) <= FovusS3Client.MAX_PARTS

        where:
        size                                           | expected
        100L * 1024 * 1024                             | (long) FovusS3Client.DEFAULT_PART_SIZE
        10_000L * FovusS3Client.DEFAULT_PART_SIZE      | (long) FovusS3Client.DEFAULT_PART_SIZE
        10_000L * FovusS3Client.DEFAULT_PART_SIZE + 1  | FovusS3Client.DEFAULT_PART_SIZE + 1L
        200L * 1024 * 1024 * 1024                      | 21_474_837L
        5L * 1024 * 1024 * 1024 * 1024                 | 549_755_814L
    }
}
