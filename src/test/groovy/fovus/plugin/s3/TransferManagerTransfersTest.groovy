package fovus.plugin.s3

import io.netty.channel.nio.NioEventLoopGroup
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.BufferedSplittableAsyncRequestBody
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.CompletedDownload
import software.amazon.awssdk.transfer.s3.model.Download
import software.amazon.awssdk.transfer.s3.model.DownloadRequest
import software.amazon.awssdk.transfer.s3.model.FileUpload
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.TimeUnit

/** {@link TransferManagerTransfers} with the SDK mocked: requests, waiting, and how failures come out. */
class TransferManagerTransfersTest extends Specification {

    static final String KEY = 'pipelines/p-1-user/fovus-work/ab/cdef/data.bin'

    @TempDir
    Path tempDir

    S3TransferManager reader = Mock()
    S3TransferManager writer = Mock()
    S3AsyncClient writerClient = Mock()
    FileUpload fileUpload = Mock()
    Download<GetObjectResponse> download = Mock()
    TransferManagerTransfers transfers = new TransferManagerTransfers(reader, writer, writerClient, 'bucket')

    def 'uploadFile should send the file to the bucket and key with the writer'() {
        given:
        def file = Files.write(tempDir.resolve('data.bin'), 'hello'.bytes)

        when:
        transfers.uploadFile(file, KEY)

        then:
        1 * writer.uploadFile({ UploadFileRequest r ->
            r.putObjectRequest().bucket() == 'bucket' && r.putObjectRequest().key() == KEY && r.source() == file
        }) >> fileUpload
        fileUpload.completionFuture() >> CompletableFuture.completedFuture(null)
        0 * reader._
    }

    /** A finished download whose response says the object is {@code length} bytes long. */
    private static CompletableFuture<CompletedDownload<GetObjectResponse>> downloaded(long length) {
        return CompletableFuture.completedFuture(
                CompletedDownload.builder().result(GetObjectResponse.builder().contentLength(length).build()).build())
    }

    def 'downloadFile should fetch the bucket and key with the reader'() {
        given:
        def destination = Files.write(tempDir.resolve('.data.bin.part'), 'hello'.bytes)

        when:
        transfers.downloadFile(KEY, destination)

        then:
        1 * reader.download({ DownloadRequest r -> r.getObjectRequest().bucket() == 'bucket' && r.getObjectRequest().key() == KEY }) >> download
        download.completionFuture() >> downloaded(5)
        0 * writer._
    }

    def "downloadFile should write into the destination, never create it: the file is the caller's, and may be gone"() {
        given:
        def destination = tempDir.resolve('.data.bin.part')
        DownloadRequest request = null
        reader.download(_) >> { DownloadRequest r -> request = r; download }
        download.completionFuture() >> downloaded(5)
        Files.write(destination, 'hello'.bytes)
        transfers.downloadFile(KEY, destination)

        when: 'its transformer gets a response after the caller deleted the file'
        Files.delete(destination)
        def transformer = request.responseTransformer()
        def result = transformer.prepare()
        transformer.onResponse(GetObjectResponse.builder().contentLength(5L).build())
        transformer.onStream(SdkPublisher.adapt(AsyncRequestBody.fromBytes('hello'.bytes)))
        result.handle { r, t -> null }.get()

        then:
        result.isCompletedExceptionally()
        !Files.exists(destination)
    }

    def 'downloadFile should cut the destination to the length of the object'() {
        given: 'an attempt that wrote more than the object holds now'
        def destination = Files.write(tempDir.resolve('.data.bin.part'), 'hello, and more'.bytes)
        reader.download(_) >> download
        download.completionFuture() >> downloaded(5)

        when:
        transfers.downloadFile(KEY, destination)

        then:
        Files.readAllBytes(destination) == 'hello'.bytes
    }

    def 'a download that wrote less than the length of the object should fail'() {
        given: 'a response that declared 5 bytes, of which only 3 reached the destination'
        def destination = Files.write(tempDir.resolve('.data.bin.part'), 'hel'.bytes)
        reader.download(_) >> download
        download.completionFuture() >> downloaded(5)

        when:
        transfers.downloadFile(KEY, destination)

        then:
        def e = thrown(IOException)
        e.message == "S3 read failed on ${KEY}: the body ended after 3 of 5 bytes".toString()
        e.cause == null
    }

    def 'a transfer failing with #wrapped should surface the failure unwrapped, or else as an I/O error naming its class'() {
        given:
        def future = new CompletableFuture()
        future.completeExceptionally(failure)
        writer.uploadFile(_) >> fileUpload
        fileUpload.completionFuture() >> future
        reader.download(_) >> download
        download.completionFuture() >> future

        when:
        transfers.uploadFile(tempDir.resolve('in.bin'), KEY)

        then:
        def upload = thrown(Exception)
        rethrown(upload, expected, 'write')

        when:
        transfers.downloadFile(KEY, tempDir.resolve('out.bin'))

        then:
        def download = thrown(Exception)
        rethrown(download, expected, 'read')

        where:
        wrapped                              | failure                                                         | expected
        'an S3Exception'                     | s3Error                                                         | s3Error
        'CompletionException(S3Exception)'   | new CompletionException(s3Error)                                | s3Error
        'two CompletionExceptions'           | new CompletionException(new CompletionException(s3Error))       | s3Error
        'an SdkClientException'              | clientError                                                     | clientError
        'an IOException'                     | ioError                                                         | ioError
        'an IllegalStateException'           | new IllegalStateException('SECRET-BODY')                        | 'an I/O error naming the class only'
    }

    static final S3Exception s3Error = FovusS3ClientTest.s3Error(500, 'InternalError')
    static final SdkClientException clientError = SdkClientException.create('Unable to execute HTTP request')
    static final IOException ioError = new IOException('disk full')

    /** The failure itself, or for anything else an I/O error that names the class only. */
    private static boolean rethrown(Exception e, Object expected, String operation) {
        if (expected instanceof Exception) return e.is(expected)
        return e.class == IOException && e.cause == null && e.message == "S3 ${operation} failed on ${KEY}: IllegalStateException".toString()
    }

    def 'an interrupted wait should cancel the transfer, keep the interrupt and throw InterruptedIOException'() {
        given:
        def future = new CompletableFuture()
        reader.download(_) >> download
        download.completionFuture() >> future

        when:
        Thread.currentThread().interrupt()
        transfers.downloadFile(KEY, tempDir.resolve('out.bin'))

        then:
        thrown(InterruptedIOException)
        Thread.interrupted()
        future.isCancelled()
    }

    // -- the upload stream

    /** What the SDK does with the body: subscribe at once, take every byte, and finish the upload when the body ends. */
    private static class ConsumingUpload {
        final CompletableFuture<PutObjectResponse> future = new CompletableFuture<>()
        final ByteArrayOutputStream received = new ByteArrayOutputStream()
        AsyncRequestBody body
        Subscription subscription
        Throwable bodyError
        /** When set, the upload fails with it at the first chunk, and then cancels the body, as the SDK does. */
        Throwable failAtFirstChunk
        /** When set, the upload fails with it once the body is complete. */
        Throwable failAtEnd

        CompletableFuture<PutObjectResponse> start(AsyncRequestBody body) {
            this.body = body
            body.subscribe(new Subscriber<ByteBuffer>() {
                @Override
                void onSubscribe(Subscription s) {
                    subscription = s
                    s.request(Long.MAX_VALUE)
                }

                @Override
                void onNext(ByteBuffer buffer) {
                    if (failAtFirstChunk != null) {
                        future.completeExceptionally(failAtFirstChunk)
                        subscription.cancel()
                        return
                    }
                    final bytes = new byte[buffer.remaining()]
                    buffer.get(bytes)
                    received.write(bytes)
                }

                @Override
                void onError(Throwable t) {
                    bodyError = t
                    future.completeExceptionally(t)
                }

                @Override
                void onComplete() {
                    if (failAtEnd != null) future.completeExceptionally(failAtEnd)
                    else future.complete(PutObjectResponse.builder().build())
                }
            })
            return future
        }
    }

    def 'the upload stream should send its bytes as a retryable body of unknown length, and publish them on close'() {
        given:
        def sdk = new ConsumingUpload()

        when:
        def out = transfers.newUploadStream(KEY, null)
        out.write('hel'.bytes)
        out.write((int) ('l' as char))
        out.write('xox'.bytes, 1, 1)
        out.close()

        then:
        1 * writerClient.putObject({ PutObjectRequest r -> r.bucket() == 'bucket' && r.key() == KEY }, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        sdk.body instanceof BufferedSplittableAsyncRequestBody
        !sdk.body.contentLength().isPresent()
        sdk.received.toString() == 'hello'
        sdk.future.isDone() && !sdk.future.isCompletedExceptionally()
    }

    static final long PART = TransferManagerTransfers.DEFAULT_PART_SIZE
    /** The largest stream of known size: 10,000 parts of the buffer's 2 parts, 312.5 GiB at 16 MiB parts. */
    static final long LARGEST_KNOWN = 10_000L * TransferManagerTransfers.STREAM_BUFFER_PARTS * PART

    def 'an upload stream of known size above one part should declare its size, so the SDK picks parts that fit in 10,000'() {
        given:
        def sdk = new ConsumingUpload()

        when:
        def out = transfers.newUploadStream(KEY, size)
        out.abort()

        then:
        1 * writerClient.putObject({ PutObjectRequest r -> r.key() == KEY }, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        sdk.body instanceof BufferedSplittableAsyncRequestBody
        sdk.body.contentLength() == Optional.of(size)

        where:
        size << [PART + 1, 3 * PART, 10_000L * PART + 1, LARGEST_KNOWN]
    }

    def 'an upload stream of known size within one part should not declare it, so its single PutObject stays buffered and retryable'() {
        given:
        def sdk = new ConsumingUpload()

        when:
        def out = transfers.newUploadStream(KEY, size)
        out.abort()

        then:
        1 * writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        !sdk.body.contentLength().isPresent()

        where:
        size << [0L, 5L, PART]
    }

    def 'an upload stream of known size above what a stream can take should fail before any upload starts'() {
        when:
        transfers.newUploadStream(KEY, size)

        then:
        def e = thrown(IOException)
        e.message == "Cannot upload ${KEY}: ${size} bytes (${expected}) is more than the 312.5 GiB a streamed upload supports".toString()
        0 * writerClient._

        where:
        size              | expected
        LARGEST_KNOWN + 1 | '312.5 GiB'
        400L << 30        | '400.0 GiB'
    }

    def 'an upload stream of known size should publish exactly that many bytes'() {
        given:
        def sdk = new ConsumingUpload()
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }

        when:
        def out = transfers.newUploadStream(KEY, 5L)
        out.write('hell'.bytes)
        out.write((int) ('o' as char))
        out.close()

        then:
        sdk.received.toString() == 'hello'
        sdk.future.isDone() && !sdk.future.isCompletedExceptionally()
    }

    def 'an upload stream of known size written past it with #how should fail and discard the upload'() {
        given:
        def sdk = new ConsumingUpload()
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        def out = transfers.newUploadStream(KEY, 5L)
        out.write('hell'.bytes)

        when:
        write.call(out)

        then:
        def e = thrown(IOException)
        e.message == "S3 write failed on ${KEY}: the stream is longer than its declared 5 bytes".toString()
        e.cause == null
        sdk.future.isCompletedExceptionally()
        !sdk.received.toString().contains('!')

        when: 'the discarded upload cannot be published'
        out.close()

        then:
        noExceptionThrown()
        sdk.future.isCompletedExceptionally()

        where:
        how             | write
        'write(int)'    | { OutputStream o -> o.write((int) ('o' as char)); o.write((int) ('!' as char)) }
        'write(byte[])' | { OutputStream o -> o.write('o!'.bytes) }
    }

    def 'an upload stream of known size closed before it was reached should fail and discard the upload'() {
        given:
        def sdk = new ConsumingUpload()
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        def out = transfers.newUploadStream(KEY, 10L)
        out.write('hello'.bytes)

        when:
        out.close()

        then:
        def e = thrown(IOException)
        e.message == "S3 write failed on ${KEY}: the stream ended after 5 of its declared 10 bytes".toString()
        e.cause == null
        sdk.future.isCompletedExceptionally()

        when:
        out.close()

        then:
        noExceptionThrown()
    }

    def 'closing an empty upload stream should still finish the upload'() {
        given:
        def sdk = new ConsumingUpload()
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }

        when:
        transfers.newUploadStream(KEY, null).close()

        then:
        sdk.received.size() == 0
        sdk.future.isDone() && !sdk.future.isCompletedExceptionally()
    }

    def 'close should wait for the upload and rethrow its failure'() {
        given:
        def sdk = new ConsumingUpload(failAtEnd: new CompletionException(s3Error))
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        def out = transfers.newUploadStream(KEY, null)
        out.write('hello'.bytes)

        when:
        out.close()

        then:
        def e = thrown(S3Exception)
        e.is(s3Error)

        when: 'closing again'
        out.close()

        then:
        noExceptionThrown()
    }

    def 'abort should cancel the upload, and a later close should do nothing'() {
        given:
        def sdk = new ConsumingUpload()
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        def out = transfers.newUploadStream(KEY, null)
        out.write('partial'.bytes)

        when:
        out.abort()
        out.abort()
        out.close()

        then:
        sdk.future.isCancelled() || sdk.bodyError != null
        sdk.future.isCompletedExceptionally()

        when:
        out.write('more'.bytes)

        then:
        def e = thrown(IOException)
        e.message == "The upload to ${KEY} is closed".toString()
    }

    def 'abort before anything was written should cancel the upload'() {
        given:
        def sdk = new ConsumingUpload()
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }

        when:
        def out = transfers.newUploadStream(KEY, null)
        out.abort()

        then:
        sdk.future.isCompletedExceptionally()
    }

    def 'abort after close should do nothing'() {
        given:
        def sdk = new ConsumingUpload()
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        def out = transfers.newUploadStream(KEY, null)
        out.write('hello'.bytes)
        out.close()

        when:
        out.abort()

        then:
        !sdk.future.isCompletedExceptionally()
        sdk.received.toString() == 'hello'
    }

    def 'a write after the upload failed should throw the upload failure, not the cancelled body'() {
        given:
        def sdk = new ConsumingUpload(failAtFirstChunk: new CompletionException(s3Error))
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        def out = transfers.newUploadStream(KEY, null)

        when: 'the first chunk fails the upload, so the SDK cancels the body; one of the next writes notices'
        10.times { out.write(new byte[8192]) }

        then:
        def e = thrown(S3Exception)
        e.is(s3Error)

        when: 'the failed upload was discarded'
        out.close()

        then:
        noExceptionThrown()
    }

    def 'an upload that failed before the SDK read the body should fail the first write with that failure'() {
        given:
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> CompletableFuture.failedFuture(clientError)
        def out = transfers.newUploadStream(KEY, null)

        when:
        out.write('hello'.bytes)

        then:
        def e = thrown(SdkClientException)
        e.is(clientError)
    }

    def 'close should close both transfer managers, the clients built for them, and their event loops'() {
        given:
        S3AsyncClient readerClient = Mock()
        def eventLoops = SdkEventLoopGroup.builder().numberOfThreads(1).build()
        eventLoops.eventLoopGroup().submit({ -> } as Runnable).get()
        def built = new TransferManagerTransfers(reader, writer, writerClient, 'bucket', [readerClient, writerClient], eventLoops)

        when:
        built.close()

        then:
        1 * reader.close()
        1 * writer.close()
        1 * readerClient.close()
        1 * writerClient.close()
        eventLoops.eventLoopGroup().isTerminated()
    }

    def 'close should give the event loops a short quiet period, for the connections that close with the clients'() {
        given:
        NioEventLoopGroup group = Spy(constructorArgs: [1])
        def built = new TransferManagerTransfers(reader, writer, writerClient, 'bucket', [writerClient], SdkEventLoopGroup.create(group))

        when:
        built.close()

        then: 'not none: a connection that closes as the loops stop hands its last task to a loop that already stopped, and Netty warns'
        1 * group.shutdownGracefully({ long quiet -> quiet > 0 && quiet <= 500 }, { long timeout -> timeout >= 1000 }, TimeUnit.MILLISECONDS)
        group.isTerminated()
    }

    def 'when the writer client cannot be built, the reader client and the event loops should be closed'() {
        given:
        S3AsyncClient readerClient = Mock()
        S3AsyncClientBuilder readerBuilder = Mock()
        readerBuilder.httpClientBuilder(_) >> readerBuilder
        readerBuilder.build() >> readerClient
        S3AsyncClientBuilder writerBuilder = Mock()
        writerBuilder.httpClientBuilder(_) >> writerBuilder
        writerBuilder.multipartEnabled(_) >> writerBuilder
        writerBuilder.multipartConfiguration(_ as MultipartConfiguration) >> writerBuilder
        writerBuilder.build() >> { throw new IllegalStateException('no writer client') }
        def eventLoops = SdkEventLoopGroup.builder().numberOfThreads(1).build()

        when:
        TransferManagerTransfers.fromBuilders(readerBuilder, writerBuilder, 'bucket', TransferManagerTransfers.MIN_PART_SIZE,
                                              TransferManagerTransfers.MAX_CONNECTIONS, eventLoops)

        then:
        def e = thrown(IllegalStateException)
        e.message == 'no writer client'
        1 * readerClient.close()
        eventLoops.eventLoopGroup().isTerminated()
    }

    def 'close should close everything and shut the event loops down even when a close fails, then throw the first failure'() {
        given:
        S3AsyncClient readerClient = Mock()
        def eventLoops = SdkEventLoopGroup.builder().numberOfThreads(1).build()
        def built = new TransferManagerTransfers(reader, writer, writerClient, 'bucket', [readerClient, writerClient], eventLoops)
        def failure = new IllegalStateException('the reader did not close')

        when:
        built.close()

        then:
        1 * reader.close() >> { throw failure }
        1 * writer.close()
        1 * readerClient.close() >> { throw new IllegalStateException('the reader client did not close') }
        1 * writerClient.close()
        def e = thrown(IllegalStateException)
        e.is(failure)
        e.suppressed*.message == ['the reader client did not close']
        eventLoops.eventLoopGroup().isTerminated()
    }
}
