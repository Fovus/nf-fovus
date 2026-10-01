/*
 * Copyright 2020-2022, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package fovus.plugin.nio;

import com.google.common.base.Preconditions;
import fovus.plugin.s3.FovusS3Client;
import nextflow.extension.FilesEx;
import nextflow.file.FileSystemTransferAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;

import static java.lang.String.format;

/**
 * File system provider for Fovus storage. The implementation was adapted from S3FileSystemProvider.
 *
 * Spec:
 * <p>
 * URI: fovus://fovus-storage/{fileType}/{filePath}
 * <p>
 * FileSystem roots: /fovus-storage/{fileType}/
 * <p>
 * Treatment of Fovus objects: - If a key ends in "/" it's considered a directory
 * *and* a regular file. Otherwise, it's just a regular file. - It is legal for
 * a key "xyz" and "xyz/" to exist at the same time. The latter is treated as a
 * directory. - If a file "a/b/c" exists but there's no "a" or "a/b/", these are
 * considered "implicit" directories. They can be listed, traversed and deleted.
 * <p>
 * Deviations from FileSystem provider API: - Deleting a file or directory
 * always succeeds, regardless of whether the file/directory existed before the
 * operation was issued i.e. Files.delete() and Files.deleteIfExists() are
 * equivalent.
 * <p>
 * Direct mode only: every area ({@code files}, {@code jobs}, {@code pipelines}) is read and written through one
 * {@link S3Storage} with the AWS S3 SDK, once the executor has attached an S3 client to this provider. The client's
 * guard decides what each area allows. Until then, only the area roots can be used: they always exist.
 */
public class FovusFileSystemProvider extends FileSystemProvider implements FileSystemTransferAware {

    private static final Logger log = LoggerFactory.getLogger(FovusFileSystemProvider.class);

    public static final String NOT_ATTACHED_MESSAGE =
            "Fovus storage paths (fovus://) can only be used in direct mode (workDir = 'fovus:///fovus-storage/pipelines'), "
            + "after the Fovus executor has started. With a Fovus storage mount, use the mounted path instead.";

    final Map<String, FovusFileSystem> fileSystems = new HashMap<>();

    /** Direct mode only: set once storage credentials exist, and shared by every area. */
    private volatile S3Storage storage;

    @Override
    public String getScheme() {
        return "fovus";
    }

    /** Direct mode: give every area its S3 client once storage credentials exist. */
    public void attachS3Client(FovusS3Client client) {
        this.storage = new S3Storage(client);
    }

    /** Direct mode: whether an S3 client is already attached (by the trace observer, or the executor). */
    public boolean hasS3Client() {
        return storage != null;
    }

    S3Storage storage() {
        final S3Storage attached = storage;
        if (attached == null) {
            throw new IllegalStateException(NOT_ATTACHED_MESSAGE);
        }
        return attached;
    }

    /**
     * Nextflow creates the file systems while it parses {@code fovus://} paths (the {@code workDir} first), before
     * the pipeline or any credentials exist: they need nothing until the S3 client is attached.
     */
    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        Preconditions.checkNotNull(uri, "uri is null");
        Preconditions.checkArgument(uri.getScheme().equals("fovus"), "uri scheme must be 'fovus': '%s'", uri);

        final String fileType = FovusPath.getFileTypeOfUri(uri);
        synchronized (fileSystems) {
            if (fileSystems.containsKey(fileType))
                throw new FileSystemAlreadyExistsException("Fovus filesystem already exists. Use getFileSystem() instead");
            final FovusFileSystem result = new FovusFileSystem(this, uri);
            fileSystems.put(fileType, result);
            return result;
        }
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        final String fileType = FovusPath.getFileTypeOfUri(uri);

        final FileSystem fileSystem = this.fileSystems.get(fileType);

        if (fileSystem == null) {
            throw new FileSystemNotFoundException("Fovus filesystem not yet created. Use newFileSystem() instead");
        }

        return fileSystem;
    }

    /**
     * Deviation from spec: throws FileSystemNotFoundException if FileSystem
     * hasn't yet been initialized.
     *
     * In this case, initialize the file system with newFileSystem() within the Executor's getWorkdir or getStageDir.
     */
    @Override
    public Path getPath(URI uri) {
        Preconditions.checkArgument(uri.getScheme().equals(getScheme()), "URI scheme must be %s", getScheme());
        return getFileSystem(uri).getPath(uri.getPath());
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        return storage().newDirectoryStream(fovus(dir), filter);
    }


    /**
     * The pipelines area takes a source from any file system: {@link S3Storage#upload} streams a remote one
     * (https://, s3://) and publishes it only once it was read in full. Nextflow's own fallback copies through
     * {@code newOutputStream}, which would publish whatever was read before a failure.
     */
    @Override
    public boolean canUpload(Path source, Path target) {
        if (isPipelines(target)) {
            return true;
        }
        return FileSystems.getDefault().equals(source.getFileSystem()) && target instanceof FovusPath;
    }

    @Override
    public boolean canDownload(Path source, Path target) {
        return source instanceof FovusPath && FileSystems.getDefault().equals(target.getFileSystem());
    }

    @Override
    public void download(Path remoteFile, Path localDestination, CopyOption... options) throws IOException {
        storage().download(fovus(remoteFile), localDestination, options);
    }

    @Override
    public void upload(Path localFile, Path remoteDestination, CopyOption... options) throws IOException {
        storage().upload(localFile, fovus(remoteDestination), options);
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        return storage().newInputStream(fovus(path));
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        return storage().newOutputStream(fovus(path), options);
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path,
                                              Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException {
        return storage().newByteChannel(fovus(path), options);
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs)
            throws IOException {
        // An area root, such as the direct-mode work directory, already exists, before any credentials do
        if (fovus(dir).isAreaRoot()) return;
        storage().createDirectory(fovus(dir));
    }

    @Override
    public void delete(Path path) throws IOException {
        storage().delete(fovus(path));
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options)
            throws IOException {
        if (!sameArea(source, target)) {
            throw new UnsupportedOperationException("Fovus Storage is read-only. copy is not supported");
        }
        storage().copy(fovus(source), fovus(target), options);
    }


    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        if (!sameArea(source, target)) {
            throw new UnsupportedOperationException("Fovus Storage is read-only. move is not supported");
        }
        storage().move(fovus(source), fovus(target), options);
    }

    @Override
    public boolean isSameFile(Path path1, Path path2) throws IOException {
        return path1.isAbsolute() && path2.isAbsolute() && path1.equals(path2);
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        // TODO: When required, add permission check for shared file
        FovusPath fovusPath = fovus(path);
        Preconditions.checkArgument(fovusPath.isAbsolute(),
                "path must be absolute: %s", fovusPath);
        if (!fovusPath.isAreaRoot()) {
            // throws NoSuchFileException when the path does not exist
            storage().readAttributes(fovusPath);
        }
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        FovusPath fovusPath = fovus(path);
        if (type.isAssignableFrom(BasicFileAttributeView.class)) {
            try {
                return (V) new FovusFileAttributesView(readAttributesOf(fovusPath));
            } catch (IOException e) {
                throw new RuntimeException("Unable read attributes for file: " + FilesEx.toUriString(fovusPath), e);
            }
        }
        throw new UnsupportedOperationException("Not a valid Fovus file system provider file attribute view: " + type.getName());
    }


    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        FovusPath fovusPath = fovus(path);

        if (type.isAssignableFrom(BasicFileAttributes.class)) {
            A attributes = (A) readAttributesOf(fovusPath);
            log.trace("+++ Attributes for path {}: {}", path, attributes);
            return attributes;
        }
        // not support attribute class
        throw new UnsupportedOperationException(format("only %s supported", BasicFileAttributes.class));
    }

    private FovusFileAttributes readAttributesOf(FovusPath fovusPath) throws IOException {
        if (fovusPath.isAreaRoot()) {
            return new FovusFileAttributes(fovusPath.getFileType() + "/", null, 0, true, false);
        }
        return storage().readAttributes(fovusPath);
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value,
                             LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * check that the paths exists or not
     *
     * @param path FovusFovusPath
     * @return true if exists
     */
    @Override
    public boolean exists(Path path, LinkOption... options) {
        final FovusPath fovusPath = fovus(path);
        if (fovusPath.isAreaRoot()) {
            return true;
        }
        // Before the S3 client is attached this throws, with the reason, rather than report a missing file
        try {
            return storage().exists(fovusPath);
        } catch (IOException e) {
            return false;
        }
    }

    private static FovusPath fovus(Path path) {
        Preconditions.checkArgument(path instanceof FovusPath, "path must be an instance of %s", FovusPath.class.getName());
        return (FovusPath) path;
    }

    private static boolean isPipelines(Path path) {
        return path instanceof FovusPath && FovusPath.PIPELINES.equals(((FovusPath) path).getFileType());
    }

    private static boolean sameArea(Path source, Path target) {
        return Objects.equals(fovus(source).getFileType(), fovus(target).getFileType());
    }
}
