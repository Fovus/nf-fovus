package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.S3Entry
import fovus.plugin.s3.S3ReadChannel
import fovus.plugin.s3.S3WriteChannel
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileHelper

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessDeniedException
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileVisitOption
import java.nio.file.FileSystems
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.NotDirectoryException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

/**
 * NIO operations for the {@code pipelines/} area of Fovus storage in direct mode, on top of
 * {@link FovusS3Client}. A path's S3 key is its {@link FovusPath#toRemoteFilePath()}, e.g.
 * {@code pipelines/<pid>/fovus-work/ab/cdef/.command.run}. Folders are key prefixes, with a zero-byte
 * {@code <key>/} marker once created. Deleting always succeeds, as for the rest of the provider.
 */
@Slf4j
@CompileStatic
class S3Storage {

    private final FovusS3Client s3
    private final AtomicBoolean deniedDeleteWarned = new AtomicBoolean()

    S3Storage(FovusS3Client s3) {
        this.s3 = s3
    }

    static String keyOf(FovusPath path) {
        return path.toRemoteFilePath()
    }

    InputStream newInputStream(FovusPath path) throws IOException {
        return s3.getObject(keyOf(path))
    }

    OutputStream newOutputStream(FovusPath path, OpenOption... options) throws IOException {
        final opts = Arrays.asList(options)
        if (opts.contains(StandardOpenOption.APPEND)) {
            throw new UnsupportedOperationException('Appending to a file in Fovus storage is not supported')
        }
        if (opts.contains(StandardOpenOption.CREATE_NEW) && exists(path)) {
            throw new FileAlreadyExistsException(path.toString())
        }
        return s3.newOutputStream(keyOf(path))
    }

    SeekableByteChannel newByteChannel(FovusPath path, Set<? extends OpenOption> options) throws IOException {
        if (options.contains(StandardOpenOption.WRITE) || options.contains(StandardOpenOption.APPEND)) {
            return new S3WriteChannel(newOutputStream(path, options.toArray(new OpenOption[0])))
        }
        final attributes = readAttributes(path)
        if (attributes.isDirectory()) throw new IOException("${path} is a directory".toString())
        return new S3ReadChannel(s3, keyOf(path), attributes.size())
    }

    DirectoryStream<Path> newDirectoryStream(FovusPath dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        final dirKey = keyOf(dir) + '/'
        final entries = s3.list(dirKey)
        if (entries.isEmpty()) {
            if (s3.head(keyOf(dir)) != null) throw new NotDirectoryException(dir.toString())
            throw new NoSuchFileException(dir.toUri().toString())
        }

        final List<Path> children = []
        for (S3Entry entry : entries) {
            // skip the folder's own marker object
            if (entry.key == dirKey) continue
            final name = entry.key.substring(dirKey.length()).replaceFirst('/$', '')
            // a key such as <dir>//x has an empty name; resolving it would return the folder itself
            if (name.isEmpty()) continue
            final child = (FovusPath) dir.resolve(name)
            // cache the listing's size and time so walking a folder needs no HeadObject per entry
            child.setFileMetadata(new FovusFileMetadata(
                    entry.directory ? child.getKey() + '/' : child.getKey(),
                    entry.lastModified == null ? null : Date.from(entry.lastModified),
                    null,
                    entry.size))
            if (filter == null || filter.accept(child)) children.add(child)
        }
        return new ListedDirectoryStream(children)
    }

    FovusFileAttributes readAttributes(FovusPath path) throws IOException {
        final cached = path.getFileMetadata()
        if (cached != null) {
            return cached.key.endsWith('/')
                    ? directory(cached.key)
                    : file(cached.key, cached.size, cached.lastModified?.toInstant())
        }

        final key = keyOf(path)
        final entry = s3.head(key)
        if (entry != null) return file(key, entry.size, entry.lastModified)
        if (s3.hasChildren(key + '/')) return directory(key + '/')
        throw new NoSuchFileException(path.toUri().toString())
    }

    boolean exists(FovusPath path) throws IOException {
        try {
            readAttributes(path)
            return true
        }
        catch (NoSuchFileException ignored) {
            return false
        }
    }

    void createDirectory(FovusPath dir) throws IOException {
        s3.putDirectoryMarker(keyOf(dir))
    }

    void delete(FovusPath path) throws IOException {
        final key = keyOf(path)
        try {
            if (s3.head(key) != null) s3.delete(key)
            // a folder: remove its marker, if it has one
            else s3.delete(key + '/')
        }
        catch (AccessDeniedException e) {
            // Every delete is denied the same way (the write credentials cannot delete): say so once, not per file
            if (deniedDeleteWarned.compareAndSet(false, true)) {
                log.warn "[FOVUS] ${e.reason ?: e.message} -- ${path} was left in place (further files left in place are logged at debug level)"
            }
            else {
                log.debug "[FOVUS] ${e.reason ?: e.message} -- ${path} was left in place"
            }
        }
    }

    void copy(FovusPath source, FovusPath target, CopyOption... options) throws IOException {
        if (!Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING) && exists(target)) {
            throw new FileAlreadyExistsException(target.toString())
        }
        final attributes = readAttributes(source)
        // Nextflow copies a folder's content itself, one file at a time
        if (attributes.isDirectory()) {
            createDirectory(target)
            return
        }
        s3.copy(keyOf(source), keyOf(target), attributes.size())
    }

    void move(FovusPath source, FovusPath target, CopyOption... options) throws IOException {
        if (readAttributes(source).isDirectory()) {
            throw new IOException("Moving a folder within Fovus storage is not supported: ${source}".toString())
        }
        copy(source, target, options)
        delete(source)
    }

    /** Upload a file or folder, from the local disk or from another file system such as https:// or s3://. */
    void upload(Path local, FovusPath target, CopyOption... options) throws IOException {
        if (!Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING) && exists(target)) {
            throw new FileAlreadyExistsException(target.toString())
        }
        if (!Files.isDirectory(local)) {
            uploadFile(local, keyOf(target))
            return
        }
        final FovusS3Client client = s3
        final S3Storage storage = this
        // Follow links, like Files.isDirectory above: a symlinked folder, or a symlinked sub-folder, is uploaded as a folder
        Files.walkFileTree(local, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
            @Override
            FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                client.putDirectoryMarker(keyOf(within(target, local, dir)))
                return FileVisitResult.CONTINUE
            }

            @Override
            FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                storage.uploadFile(file, keyOf(within(target, local, file)))
                return FileVisitResult.CONTINUE
            }
        })
    }

    /**
     * A local file is uploaded in parallel parts. A file on another file system is streamed, and published only
     * once it was read in full: any failure while reading it, or fewer bytes than its known size, discards the
     * upload, so FilePorter never finds a truncated input to reuse.
     */
    private void uploadFile(Path source, String key) throws IOException {
        if (source.getFileSystem() == FileSystems.getDefault()) {
            s3.uploadFile(source, key)
            return
        }
        final long expected = knownSize(source)
        final out = s3.newOutputStream(key)
        boolean complete = false
        try {
            final long copied = Files.newInputStream(source).withCloseable { InputStream input -> input.transferTo(out) }
            if (expected > 0 && copied != expected) {
                throw new IOException("Read ${copied} of ${expected} bytes for ${FovusS3Client.uri(key)}: the source ended early, so nothing was uploaded".toString())
            }
            out.close()
            complete = true
        }
        finally {
            // A no-op once close() has run, whether or not it succeeded; otherwise it discards the partial upload
            if (!complete) out.abort()
        }
    }

    /** The size a file system reports for a file, or -1 when it has none (e.g. an http response without a length). */
    private static long knownSize(Path source) {
        try {
            return Files.size(source)
        }
        catch (IOException | UnsupportedOperationException ignored) {
            return -1L
        }
    }

    void download(FovusPath source, Path local, CopyOption... options) throws IOException {
        if (Files.exists(local)) {
            if (!Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING)) {
                throw new FileAlreadyExistsException(local.toString())
            }
            FileHelper.deletePath(local)
        }
        if (!readAttributes(source).isDirectory()) {
            s3.downloadFile(keyOf(source), local)
            return
        }
        final dirKey = keyOf(source) + '/'
        final root = local.toAbsolutePath().normalize()
        Files.createDirectories(local)
        for (S3Entry entry : s3.listAll(dirKey)) {
            final relative = entry.key.substring(dirKey.length())
            // the folder's own marker
            if (relative.isEmpty()) continue
            // S3 keys are arbitrary strings: one such as <dir>//tmp/x or <dir>/../x must not reach outside the local folder
            if (!isPlainRelativePath(relative)) {
                log.debug "[FOVUS] Not downloading ${entry.key}: it is not a plain path below ${dirKey}"
                continue
            }
            final target = local.resolve(relative)
            if (!target.toAbsolutePath().normalize().startsWith(root)) {
                log.debug "[FOVUS] Not downloading ${entry.key}: it would be written outside ${local}"
                continue
            }
            if (entry.key.endsWith('/')) {
                Files.createDirectories(target)
                continue
            }
            Files.createDirectories(target.parent)
            s3.downloadFile(entry.key, target)
        }
    }

    /** A folder marker's trailing {@code /} aside, every segment must be a real name: not empty, {@code .} or {@code ..}. */
    private static boolean isPlainRelativePath(String relative) {
        final name = relative.endsWith('/') ? relative.substring(0, relative.length() - 1) : relative
        if (name.isEmpty()) return false
        for (String segment : name.split('/', -1)) {
            if (segment.isEmpty() || segment == '.' || segment == '..') return false
        }
        return true
    }

    private static FovusPath within(FovusPath target, Path localRoot, Path local) {
        final relative = localRoot.relativize(local).toString()
        return relative.isEmpty() ? target : (FovusPath) target.resolve(relative)
    }

    private static FovusFileAttributes directory(String key) {
        return new FovusFileAttributes(key, null, 0L, true, false)
    }

    private static FovusFileAttributes file(String key, long size, Instant lastModified) {
        return new FovusFileAttributes(key, lastModified == null ? null : FileTime.from(lastModified), size, false, true)
    }

    @CompileStatic
    private static class ListedDirectoryStream implements DirectoryStream<Path> {
        private final List<Path> children

        ListedDirectoryStream(List<Path> children) {
            this.children = children
        }

        @Override
        Iterator<Path> iterator() {
            return children.iterator()
        }

        @Override
        void close() {
        }
    }
}
