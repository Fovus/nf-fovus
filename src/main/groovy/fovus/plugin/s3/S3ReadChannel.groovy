package fovus.plugin.s3

import groovy.transform.CompileStatic

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonWritableChannelException
import java.nio.channels.SeekableByteChannel

/** A read-only channel over one object; seeking reopens the object from the new offset with a ranged GET. */
@CompileStatic
class S3ReadChannel implements SeekableByteChannel {

    private final FovusS3Client client
    private final String key
    private final long objectSize
    private long currentOffset = 0
    private InputStream stream
    private long streamOffset = -1
    private boolean channelOpen = true

    S3ReadChannel(FovusS3Client client, String key, long size) {
        this.client = client
        this.key = key
        this.objectSize = size
    }

    @Override
    int read(ByteBuffer destination) throws IOException {
        ensureOpen()
        if (currentOffset >= objectSize) return -1
        if (stream == null || streamOffset != currentOffset) {
            stream?.close()
            stream = client.getObject(key, currentOffset)
            streamOffset = currentOffset
        }
        final chunk = new byte[Math.min(destination.remaining(), 64 * 1024)]
        final count = stream.read(chunk)
        if (count < 0) return -1
        destination.put(chunk, 0, count)
        currentOffset += count
        streamOffset += count
        return count
    }

    @Override
    int write(ByteBuffer source) {
        throw new NonWritableChannelException()
    }

    @Override
    long position() {
        return currentOffset
    }

    @Override
    SeekableByteChannel position(long newPosition) throws IOException {
        ensureOpen()
        currentOffset = newPosition
        return this
    }

    @Override
    long size() {
        return objectSize
    }

    @Override
    SeekableByteChannel truncate(long size) {
        throw new NonWritableChannelException()
    }

    @Override
    boolean isOpen() {
        return channelOpen
    }

    @Override
    void close() throws IOException {
        channelOpen = false
        stream?.close()
    }

    private void ensureOpen() throws ClosedChannelException {
        if (!channelOpen) throw new ClosedChannelException()
    }
}
