package fovus.plugin.s3

import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.BufferedSplittableAsyncRequestBody
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest
import software.amazon.awssdk.transfer.s3.model.FileDownload
import software.amazon.awssdk.transfer.s3.model.FileUpload
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException

/** {@link TransferManagerTransfers} with the SDK mocked: requests, waiting, and how failures come out. */
class TransferManagerTransfersTest extends Specification {

    static final String KEY = 'pipelines/p-1-user/fovus-work/ab/cdef/data.bin'

    @TempDir
    Path tempDir

    S3TransferManager reader = Mock()
    S3TransferManager writer = Mock()
    S3AsyncClient writerClient = Mock()
    FileUpload fileUpload = Mock()
    FileDownload fileDownload = Mock()
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

    def 'downloadFile should fetch the bucket and key into the destination with the reader'() {
        given:
        def destination = tempDir.resolve('.data.bin.part')

        when:
        transfers.downloadFile(KEY, destination)

        then:
        1 * reader.downloadFile({ DownloadFileRequest r ->
            r.getObjectRequest().bucket() == 'bucket' && r.getObjectRequest().key() == KEY && r.destination() == destination
        }) >> fileDownload
        fileDownload.completionFuture() >> CompletableFuture.completedFuture(null)
        0 * writer._
    }

    def 'a transfer failing with #wrapped should surface the failure unwrapped, or else as an I/O error naming its class'() {
        given:
        def future = new CompletableFuture()
        future.completeExceptionally(failure)
        writer.uploadFile(_) >> fileUpload
        fileUpload.completionFuture() >> future
        reader.downloadFile(_) >> fileDownload
        fileDownload.completionFuture() >> future

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
        reader.downloadFile(_) >> fileDownload
        fileDownload.completionFuture() >> future

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
        def out = transfers.newUploadStream(KEY)
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

    def 'closing an empty upload stream should still finish the upload'() {
        given:
        def sdk = new ConsumingUpload()
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }

        when:
        transfers.newUploadStream(KEY).close()

        then:
        sdk.received.size() == 0
        sdk.future.isDone() && !sdk.future.isCompletedExceptionally()
    }

    def 'close should wait for the upload and rethrow its failure'() {
        given:
        def sdk = new ConsumingUpload(failAtEnd: new CompletionException(s3Error))
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        def out = transfers.newUploadStream(KEY)
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
        def out = transfers.newUploadStream(KEY)
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
        def out = transfers.newUploadStream(KEY)
        out.abort()

        then:
        sdk.future.isCompletedExceptionally()
    }

    def 'abort after close should do nothing'() {
        given:
        def sdk = new ConsumingUpload()
        writerClient.putObject(_ as PutObjectRequest, _ as AsyncRequestBody) >> { PutObjectRequest r, AsyncRequestBody body -> sdk.start(body) }
        def out = transfers.newUploadStream(KEY)
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
        def out = transfers.newUploadStream(KEY)

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
        def out = transfers.newUploadStream(KEY)

        when:
        out.write('hello'.bytes)

        then:
        def e = thrown(SdkClientException)
        e.is(clientError)
    }

    def 'close should close both transfer managers and the clients built for them'() {
        given:
        S3AsyncClient readerClient = Mock()
        def built = new TransferManagerTransfers(reader, writer, writerClient, 'bucket', [readerClient, writerClient])

        when:
        built.close()

        then:
        1 * reader.close()
        1 * writer.close()
        1 * readerClient.close()
        1 * writerClient.close()
    }
}
