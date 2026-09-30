package fovus.plugin.s3

import groovy.transform.CompileStatic

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonReadableChannelException
import java.nio.channels.SeekableByteChannel

/** A write-only, sequential channel over an {@link S3MultipartOutputStream}. */
@CompileStatic
class S3WriteChannel implements SeekableByteChannel {

    private final OutputStream output
    private long written = 0
    private boolean channelOpen = true

    S3WriteChannel(OutputStream output) {
        this.output = output
    }

    @Override
    int write(ByteBuffer source) throws IOException {
        if (!channelOpen) throw new ClosedChannelException()
        final count = source.remaining()
        final bytes = new byte[count]
        source.get(bytes)
        output.write(bytes)
        written += count
        return count
    }

    @Override
    int read(ByteBuffer destination) {
        throw new NonReadableChannelException()
    }

    @Override
    long position() {
        return written
    }

    @Override
    SeekableByteChannel position(long newPosition) {
        if (newPosition != written) throw new UnsupportedOperationException('Fovus storage objects are written sequentially')
        return this
    }

    @Override
    long size() {
        return written
    }

    @Override
    SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException('Fovus storage objects cannot be truncated')
    }

    @Override
    boolean isOpen() {
        return channelOpen
    }

    @Override
    void close() throws IOException {
        if (!channelOpen) return
        channelOpen = false
        output.close()
    }
}
