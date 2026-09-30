package fovus.plugin.s3

import groovy.transform.CompileStatic

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonWritableChannelException
import java.nio.channels.SeekableByteChannel

/** A read-only channel over one object; seeking reopens the object from the new offset with a ranged GET. */
@CompileStatic
class S3ReadChannel implements SeekableByteChannel {

    static final int BLOCK_SIZE = 64 * 1024

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

    /**
     * Reads a full block -- {@code destination}'s free space, up to {@link #BLOCK_SIZE} -- unless the object ends
     * first. A network stream returns short reads, and callers such as Nextflow's {@code FilesEx.tail} expect
     * a full block.
     */
    @Override
    int read(ByteBuffer destination) throws IOException {
        ensureOpen()
        if (!destination.hasRemaining()) return 0
        if (currentOffset >= objectSize) return -1
        if (stream == null || streamOffset != currentOffset) {
            stream?.close()
            stream = client.getObject(key, currentOffset)
            streamOffset = currentOffset
        }
        final chunk = new byte[Math.min(destination.remaining(), BLOCK_SIZE)]
        int filled = 0
        while (filled < chunk.length) {
            final count = stream.read(chunk, filled, chunk.length - filled)
            if (count < 0) break
            filled += count
        }
        if (filled == 0) return -1
        destination.put(chunk, 0, filled)
        currentOffset += filled
        streamOffset += filled
        return filled
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
