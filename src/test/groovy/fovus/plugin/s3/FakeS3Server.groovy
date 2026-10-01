package fovus.plugin.s3

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

/**
 * A local S3 endpoint for testing the real SDK clients without Docker: path-style requests over plain HTTP to one
 * bucket, with single and multipart uploads, whole-object GETs, and failures injected per request. Every request is
 * recorded as a short line, such as {@code PUT <key>}, {@code CREATE <key>}, {@code PART 2 <key>},
 * {@code COMPLETE <key>}, {@code ABORT <key>} or {@code GET <key>}.
 */
class FakeS3Server implements Closeable {

    /** The objects, by key. */
    final Map<String, byte[]> objects = new ConcurrentHashMap<>()
    /** The requests in the order they arrived. */
    final List<String> requests = new CopyOnWriteArrayList<>()
    private final Map<String, Map<Integer, byte[]>> uploads = new ConcurrentHashMap<>()
    private final Set<String> failingOnce = ConcurrentHashMap.newKeySet()
    private final Set<String> denied = ConcurrentHashMap.newKeySet()
    private volatile CountDownLatch partsHeld = new CountDownLatch(0)
    private final ExecutorService executor
    private final HttpServer server

    FakeS3Server() {
        executor = Executors.newCachedThreadPool({ Runnable task ->
            final thread = new Thread(task, 'fake-s3')
            thread.daemon = true
            return thread
        } as ThreadFactory)
        server = HttpServer.create(new InetSocketAddress(InetAddress.loopbackAddress, 0), 0)
        server.createContext('/') { HttpExchange exchange -> handle(exchange) }
        server.executor = executor
        server.start()
    }

    /** The requests for {@code key}, in the order they arrived. */
    List<String> requestsFor(String key) {
        return requests.findAll { String request -> request.endsWith(' ' + key) }
    }

    URI getEndpoint() {
        return URI.create("http://${server.address.hostString}:${server.address.port}")
    }

    /** Answer the next request recorded as {@code request} (e.g. {@code PART 2 <key>}) with a 500, once. */
    void failOnce(String request) {
        failingOnce.add(request)
    }

    /** Answer every request recorded as {@code request} with a 403. */
    void deny(String request) {
        denied.add(request)
    }

    /** Keep every UploadPart request waiting, unanswered, until {@link #releaseParts()}. */
    void holdParts() {
        partsHeld = new CountDownLatch(1)
    }

    void releaseParts() {
        partsHeld.countDown()
    }

    void reset() {
        releaseParts()
        objects.clear()
        requests.clear()
        uploads.clear()
        failingOnce.clear()
        denied.clear()
    }

    @Override
    void close() {
        releaseParts()
        server.stop(0)
        executor.shutdownNow()
        executor.awaitTermination(5, TimeUnit.SECONDS)
    }

    private void handle(HttpExchange exchange) {
        try {
            respondTo(exchange)
        }
        finally {
            exchange.close()
        }
    }

    private void respondTo(HttpExchange exchange) {
        final key = exchange.requestURI.path.substring('/bucket/'.length())
        final query = query(exchange)
        final method = exchange.requestMethod
        final String request
        if (method == 'PUT' && query.partNumber) request = "PART ${query.partNumber} ${key}"
        else if (method == 'PUT') request = "PUT ${key}"
        else if (method == 'POST' && query.containsKey('uploads')) request = "CREATE ${key}"
        else if (method == 'POST' && query.uploadId) request = "COMPLETE ${key}"
        else if (method == 'DELETE' && query.uploadId) request = "ABORT ${key}"
        else if (method == 'GET') request = "GET ${key}"
        else request = "${method} ${exchange.requestURI}"
        requests.add(request)
        final body = body(exchange)

        if (request.startsWith('PART ')) partsHeld.await()
        if (denied.contains(request)) {
            error(exchange, 403, 'AccessDenied')
            return
        }
        if (failingOnce.remove(request)) {
            error(exchange, 500, 'InternalError')
            return
        }
        if (request.startsWith('PART ')) {
            uploads[query.uploadId][query.partNumber as int] = body
            respond(exchange, 200, '', [ETag: "\"part-${query.partNumber}\"".toString()])
        }
        else if (request.startsWith('PUT ')) {
            objects[key] = body
            respond(exchange, 200, '', [ETag: '"object"'])
        }
        else if (request.startsWith('CREATE ')) {
            final uploadId = UUID.randomUUID().toString()
            uploads[uploadId] = new ConcurrentHashMap<Integer, byte[]>()
            respond(exchange, 200, "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>${key}</Key><UploadId>${uploadId}</UploadId></InitiateMultipartUploadResult>")
        }
        else if (request.startsWith('COMPLETE ')) {
            final parts = uploads.remove(query.uploadId)
            final whole = new ByteArrayOutputStream()
            parts.keySet().sort().each { Integer number -> whole.write(parts[number]) }
            objects[key] = whole.toByteArray()
            respond(exchange, 200, "<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>${key}</Key><ETag>\"object\"</ETag></CompleteMultipartUploadResult>")
        }
        else if (request.startsWith('ABORT ')) {
            uploads.remove(query.uploadId)
            respond(exchange, 204, '')
        }
        else if (request.startsWith('GET ')) {
            final data = objects[key]
            if (data == null) {
                error(exchange, 404, 'NoSuchKey')
                return
            }
            exchange.responseHeaders.add('ETag', '"object"')
            exchange.responseHeaders.add('Last-Modified', 'Thu, 01 Oct 2026 12:00:00 GMT')
            exchange.sendResponseHeaders(200, data.length == 0 ? -1 : data.length)
            if (data.length > 0) exchange.responseBody.write(data)
        }
        else {
            error(exchange, 400, 'NotImplemented')
        }
    }

    private static Map<String, String> query(HttpExchange exchange) {
        final raw = exchange.requestURI.rawQuery
        if (!raw) return [:]
        return raw.split('&').collectEntries { String pair ->
            final parts = pair.split('=', 2)
            [(parts[0]): parts.length > 1 ? URLDecoder.decode(parts[1], 'UTF-8') : '']
        }
    }

    /** The request body; an {@code aws-chunked} one (signed streaming) is decoded. */
    private static byte[] body(HttpExchange exchange) {
        final raw = exchange.requestBody.readAllBytes()
        final sha = exchange.requestHeaders.getFirst('x-amz-content-sha256')
        if (sha == null || !sha.startsWith('STREAMING')) return raw
        final decoded = new ByteArrayOutputStream()
        int position = 0
        while (position < raw.length) {
            int end = position
            while (!(raw[end] == (byte) '\r' && raw[end + 1] == (byte) '\n')) end++
            final size = Integer.parseInt(new String(raw, position, end - position).split(';')[0], 16)
            position = end + 2
            if (size == 0) break
            decoded.write(raw, position, size)
            position += size + 2
        }
        return decoded.toByteArray()
    }

    private static void error(HttpExchange exchange, int status, String code) {
        respond(exchange, status, "<Error><Code>${code}</Code><Message>raw S3 body text SECRET-BODY</Message><RequestId>req-1</RequestId></Error>",
                ['Content-Type': 'application/xml'])
    }

    private static void respond(HttpExchange exchange, int status, String text, Map<String, String> headers = [:]) {
        final bytes = text.getBytes('UTF-8')
        headers.each { String name, String value -> exchange.responseHeaders.add(name, value) }
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length)
        if (bytes.length > 0) exchange.responseBody.write(bytes)
    }
}
