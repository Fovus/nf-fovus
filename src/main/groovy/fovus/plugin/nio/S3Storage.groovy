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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * NIO operations for every area of Fovus storage in direct mode ({@code files/}, {@code jobs/} and
 * {@code pipelines/}), on top of {@link FovusS3Client}, whose guard decides what each area allows: {@code jobs/}
 * is read-only. A path's S3 key is {@link #keyOf}, e.g. {@code pipelines/<pid>/fovus-work/ab/cdef/.command.run}.
 * Folders are key prefixes, with a zero-byte {@code <key>/} marker once created. A copy or move may cross areas
 * (a task output published into {@code files/}). Deleting succeeds whether or not the key existed, as for the rest
 * of the provider, with two exceptions: a delete the guard refuses fails, and one S3 denies (the write credentials
 * cannot delete) is warned about and left in place, and the next replace of that key is allowed; see
 * {@link #deniedDeletes}.
 */
@Slf4j
@CompileStatic
class S3Storage {

    private final FovusS3Client s3
    private final AtomicBoolean deniedDeleteWarned = new AtomicBoolean()
    /**
     * The keys whose delete S3 denied, so the object is still there. Nextflow's {@code publishDir} overwrites a
     * published file by deleting it and copying again; with a delete that cannot happen, that second copy would
     * find the target and fail. The write replaces an object atomically anyway, so a target in this set may be
     * replaced once, without {@code REPLACE_EXISTING}, which gives the caller the effect of the delete it asked
     * for. Only the writes need this: {@link #exists} and the listings keep showing the object.
     */
    private final Set<String> deniedDeletes = ConcurrentHashMap.newKeySet()

    S3Storage(FovusS3Client s3) {
        this.s3 = s3
    }

    /** The S3 key of a path: {@code <area>} for an area root, else {@code <area>/<key>}, the object the mount shows at {@code /fovus-storage/<area>/<key>}. */
    static String keyOf(FovusPath path) {
        final key = path.getKey()
        return key.isEmpty() ? path.getFileType() : path.getFileType() + '/' + key
    }

    InputStream newInputStream(FovusPath path) throws IOException {
        return s3.getObject(keyOf(path))
    }

    OutputStream newOutputStream(FovusPath path, OpenOption... options) throws IOException {
        final opts = Arrays.asList(options)
        if (opts.contains(StandardOpenOption.APPEND)) {
            throw new UnsupportedOperationException('Appending to a file in Fovus storage is not supported')
        }
        // Before the existence check below, which would call S3 for a path that cannot be written anyway
        s3.checkWritable(keyOf(path))
        if (opts.contains(StandardOpenOption.CREATE_NEW)) checkAbsent(path, false)
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
            // An area root always exists, as a folder, even while nothing is under it
            if (dir.isAreaRoot()) return new ListedDirectoryStream([])
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
        final key = keyOf(dir)
        s3.putDirectoryMarker(key)
        // The marker was written again: the denied delete of the old one, if any, is used up
        deniedDeletes.remove(key + '/')
    }

    void delete(FovusPath path) throws IOException {
        final key = keyOf(path)
        // A refusal by the guard (jobs/, outside the writable folders) is an error. Only S3's denial of the delete is
        // left in place with a warning, so this runs before the try, and before any S3 call
        s3.checkWritable(key)
        try {
            // a folder: remove its marker, if it has one
            deleteRemembering(s3.head(key) != null ? key : key + '/')
        }
        catch (AccessDeniedException e) {
            leftInPlace(e, path.toString())
        }
    }

    /** Delete one object; when S3 denies it, the object is still there, and that is remembered: see {@link #deniedDeletes}. */
    private void deleteRemembering(String key) throws IOException {
        try {
            s3.delete(key)
        }
        catch (AccessDeniedException e) {
            deniedDeletes.add(key)
            throw e
        }
    }

    /** Every delete is denied the same way (the write credentials cannot delete): say so once, not per file. */
    private void leftInPlace(AccessDeniedException denied, String what) {
        if (deniedDeleteWarned.compareAndSet(false, true)) {
            log.warn "[FOVUS] ${denied.reason ?: denied.message} -- ${what} was left in place (further files left in place are logged at debug level)"
        }
        else {
            log.debug "[FOVUS] ${denied.reason ?: denied.message} -- ${what} was left in place"
        }
    }

    /** Copy a file. A folder is only created at the target: Nextflow copies a folder's content itself, one file at a time. */
    void copy(FovusPath source, FovusPath target, CopyOption... options) throws IOException {
        // The target first: a jobs/ target is refused without a call, and the existence check below is one
        s3.checkWritable(keyOf(target))
        if (!replaces(options)) checkAbsent(target, false)
        final attributes = readAttributes(source)
        if (attributes.isDirectory()) {
            createDirectory(target)
            return
        }
        s3.copy(keyOf(source), keyOf(target), attributes.size())
    }

    /**
     * A file is copied, then deleted. A folder is copied object by object, with the relative keys and markers it
     * has, and only once every object is copied are the source objects deleted. Either way the source is left in
     * place, with a warning, when S3 denies the delete.
     */
    void move(FovusPath source, FovusPath target, CopyOption... options) throws IOException {
        // The source is deleted at the end, so one that cannot be is refused before anything is copied
        s3.checkWritable(keyOf(source))
        s3.checkWritable(keyOf(target))
        if (!readAttributes(source).isDirectory()) {
            copy(source, target, options)
            delete(source)
            return
        }
        // With REPLACE_EXISTING the objects are merged into an existing target folder: S3 has no folders to replace,
        // and Nextflow's own folder copy merges the same way
        if (!replaces(options)) checkAbsent(target, true)
        final sourceKey = keyOf(source) + '/'
        final targetKey = keyOf(target) + '/'
        final entries = s3.listAll(sourceKey)
        for (S3Entry entry : entries) {
            final destination = targetKey + entry.key.substring(sourceKey.length())
            if (entry.directory) s3.putDirectoryMarker(destination)
            else s3.copy(entry.key, destination, entry.size)
        }
        for (S3Entry entry : entries) {
            try {
                deleteRemembering(entry.key)
            }
            catch (AccessDeniedException e) {
                // S3 denies every delete the same way: report the folder once rather than ask for each object
                leftInPlace(e, source.toString())
                return
            }
        }
    }

    private static boolean replaces(CopyOption... options) {
        return Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING)
    }

    /**
     * Throw {@link FileAlreadyExistsException} when {@code target} exists, unless its delete was denied by S3 and
     * it is replaced now (once; see {@link #deniedDeletes}). A folder was deleted through its marker.
     */
    private void checkAbsent(FovusPath target, boolean folder) throws IOException {
        if (!exists(target)) return
        final key = folder ? keyOf(target) + '/' : keyOf(target)
        if (deniedDeletes.remove(key)) {
            // The folder's files are replaced one by one: their own denied deletes are used up with it
            if (folder) deniedDeletes.removeIf { String denied -> denied.startsWith(key) }
            return
        }
        throw new FileAlreadyExistsException(target.toString())
    }

    /** Upload a file or folder, from the local disk or from another file system such as https:// or s3://. */
    void upload(Path local, FovusPath target, CopyOption... options) throws IOException {
        s3.checkWritable(keyOf(target))
        final folder = Files.isDirectory(local)
        if (!replaces(options)) checkAbsent(target, folder)
        if (!folder) {
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
     * A local file is uploaded by the Transfer Manager, in parallel parts when large. A file on another file system
     * is streamed, and published only once it was read in full: any failure while reading it, or fewer bytes than
     * its known size, discards the upload, so FilePorter never finds a truncated input to reuse.
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
