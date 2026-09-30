package fovus.plugin.s3

import groovy.transform.CompileStatic
import software.amazon.awssdk.http.ContentStreamProvider

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * The body of one upload part: {@code length} bytes of a local file from {@code start}, read from the file as
 * the SDK sends them rather than copied into memory first. Each {@link #newStream()} (the SDK asks again on a
 * retry) starts over and closes the previous stream; {@link #close()} closes the last one.
 */
@CompileStatic
class FileSliceProvider implements ContentStreamProvider, Closeable {

    private final Path file
    private final long start
    private final long length
    private SliceStream current

    FileSliceProvider(Path file, long start, long length) {
        this.file = file
        this.start = start
        this.length = length
    }

    @Override
    synchronized InputStream newStream() {
        current?.close()
        current = new SliceStream(file, start, length)
        return current
    }

    @Override
    synchronized void close() throws IOException {
        current?.close()
        current = null
    }

    @CompileStatic
    private static class SliceStream extends InputStream {

        private final Path file
        private final FileChannel channel
        private final long end
        private long position

        SliceStream(Path file, long start, long length) {
            this.file = file
            this.channel = FileChannel.open(file, StandardOpenOption.READ)
            this.position = start
            this.end = start + length
        }

        @Override
        int read() throws IOException {
            final one = new byte[1]
            return read(one, 0, 1) < 0 ? -1 : (one[0] & 0xff)
        }

        @Override
        int read(byte[] bytes, int offset, int count) throws IOException {
            if (count == 0) return 0
            if (position >= end) return -1
            final int wanted = (int) Math.min((long) count, end - position)
            final int read = channel.read(ByteBuffer.wrap(bytes, offset, wanted), position)
            if (read < 0) throw new EOFException("${file} ended early".toString())
            position += read
            return read
        }

        @Override
        void close() throws IOException {
            channel.close()
        }
    }
}
