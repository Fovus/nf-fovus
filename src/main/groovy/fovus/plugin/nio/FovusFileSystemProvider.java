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
import fovus.plugin.job.FovusJobClient;
import fovus.plugin.FovusConfig;
import fovus.plugin.util.FovusFileMetadataLookup;
import nextflow.extension.FilesEx;
import nextflow.file.CopyOptions;
import nextflow.file.FileHelper;
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
import java.util.concurrent.TimeUnit;

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
 * The {@code files} and {@code jobs} areas are read-only and served through the Fovus CLI. The
 * {@code pipelines} area is the work directory of direct mode: it is read and written through
 * {@link S3Storage} with the AWS S3 SDK, once the executor has attached an S3 client.
 */
public class FovusFileSystemProvider extends FileSystemProvider implements FileSystemTransferAware {

    private static final Logger log = LoggerFactory.getLogger(FovusFileSystemProvider.class);

    final Map<String, FovusFileSystem> fileSystems = new HashMap<>();

    private final FovusFileMetadataLookup fovusFileMetadataLookup = new FovusFileMetadataLookup();

    @Override
    public String getScheme() {
        return "fovus";
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        Preconditions.checkNotNull(uri, "uri is null");
        Preconditions.checkArgument(uri.getScheme().equals("fovus"), "uri scheme must be 'fovus': '%s'", uri);

        final String fileType = FovusPath.getFileTypeOfUri(uri);
        synchronized (fileSystems) {
            if (fileSystems.containsKey(fileType))
                throw new FileSystemAlreadyExistsException("Fovus filesystem already exists. Use getFileSystem() instead");
            // The pipelines area needs no CLI client: Nextflow creates it while parsing workDir, before the
            // pipeline or any credentials exist, and the executor attaches an S3 client later.
            final FovusFileSystem result = FovusPath.PIPELINES.equals(fileType)
                    ? new FovusFileSystem(this, null, uri)
                    : createFileSystem(uri, new FovusConfig(env));
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

        Preconditions.checkArgument(dir instanceof FovusPath, "path must be an instance of %s", FovusPath.class.getName());
        final FovusPath fovusPath = (FovusPath) dir;
        if (isPipelines(fovusPath)) {
            return pipelines(fovusPath).newDirectoryStream(fovusPath, filter);
        }

        return new DirectoryStream<Path>() {
            @Override
            public void close() throws IOException {
                // nothing to do here
            }

            @Override
            public Iterator<Path> iterator() {
                return new FovusPathIterator(fovusPath.getKey() + "/", fovusPath);
            }
        };
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
        final FovusPath source = (FovusPath) remoteFile;
        if (isPipelines(source)) {
            pipelines(source).download(source, localDestination, options);
            return;
        }

        final CopyOptions opts = CopyOptions.parse(options);
        // delete target if it exists and REPLACE_EXISTING is specified
        if (opts.replaceExisting()) {
            FileHelper.deletePath(localDestination);
        } else if (Files.exists(localDestination))
            throw new FileAlreadyExistsException(localDestination.toString());

        final Optional<FovusFileAttributes> attrs = readAttr1(source);
        final boolean isDir = attrs.isPresent() && attrs.get().isDirectory();
        final String type = isDir ? "directory" : "file";
        final FovusJobClient fovusJobClient = source.getFileSystem().getJobClient();
        log.debug("Fovus download {} from={} to={}", type, FilesEx.toUriString(source), localDestination);

        if (isDir) {
            fovusJobClient.downloadFile(source.getKey() + "/", localDestination.toAbsolutePath().toString(), source.getFileType());
        } else {
            // Need to use getParent because the download destination is expected to be a directory
            fovusJobClient.downloadFile(source.getKey(), localDestination.getParent().toAbsolutePath().toString(), source.getFileType());
        }
    }

    @Override
    public void upload(Path localFile, Path remoteDestination, CopyOption... options) throws IOException {
        if (isPipelines(remoteDestination)) {
            pipelines(remoteDestination).upload(localFile, (FovusPath) remoteDestination, options);
            return;
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. upload is not supported");
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        if (isPipelines(path)) {
            return pipelines(path).newInputStream((FovusPath) path);
        }
        return super.newInputStream(path, options);
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        if (isPipelines(path)) {
            return pipelines(path).newOutputStream((FovusPath) path, options);
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. newOutputStream is not supported");
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path,
                                              Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException {
        if (isPipelines(path)) {
            return pipelines(path).newByteChannel((FovusPath) path, options);
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. newByteChannel is not supported");
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs)
            throws IOException {
        // The direct-mode work directory itself: nothing to create, and no credentials exist yet
        if (isPipelinesAreaRoot(dir)) return;
        if (isPipelines(dir)) {
            pipelines(dir).createDirectory((FovusPath) dir);
            return;
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. createDirectory is not supported");
    }

    @Override
    public void delete(Path path) throws IOException {
        if (isPipelines(path)) {
            pipelines(path).delete((FovusPath) path);
            return;
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. delete is not supported");
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options)
            throws IOException {
        if (isPipelines(source) && isPipelines(target)) {
            pipelines(target).copy((FovusPath) source, (FovusPath) target, options);
            return;
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. copy is not supported");
    }


    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        if (isPipelines(source) && isPipelines(target)) {
            pipelines(target).move((FovusPath) source, (FovusPath) target, options);
            return;
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. move is not supported");
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
        FovusPath fovusPath = (FovusPath) path;
        Preconditions.checkArgument(fovusPath.isAbsolute(),
                "path must be absolute: %s", fovusPath);
        if (isPipelines(fovusPath) && !fovusPath.isPipelinesAreaRoot()) {
            // throws NoSuchFileException when the path does not exist
            pipelines(fovusPath).readAttributes(fovusPath);
        }
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        Preconditions.checkArgument(path instanceof FovusPath,
                "path must be an instance of %s", FovusPath.class.getName());
        FovusPath fovusPath = (FovusPath) path;
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
        Preconditions.checkArgument(path instanceof FovusPath,
                "path must be an instance of %s", FovusPath.class.getName());
        FovusPath fovusPath = (FovusPath) path;

        if (type.isAssignableFrom(BasicFileAttributes.class)) {
            A attributes = (A) readAttributesOf(fovusPath);
            log.trace("+++ Attributes for path {}: {}", path, attributes);
            return attributes;
        }
        // not support attribute class
        throw new UnsupportedOperationException(format("only %s supported", BasicFileAttributes.class));
    }

    private FovusFileAttributes readAttributesOf(FovusPath fovusPath) throws IOException {
        if (fovusPath.isPipelinesAreaRoot()) {
            return new FovusFileAttributes(FovusPath.PIPELINES + "/", null, 0, true, false);
        }
        if (isPipelines(fovusPath)) {
            return pipelines(fovusPath).readAttributes(fovusPath);
        }
        return "".equals(fovusPath.getKey())
                ? new FovusFileAttributes("/", null, 0, true, false)
                // read the target path attributes
                : readAttr0(fovusPath);
    }

    private Optional<FovusFileAttributes> readAttr1(FovusPath fovusPath) throws IOException {
        try {
            return Optional.of(readAttr0(fovusPath));
        } catch (NoSuchFileException e) {
            return Optional.<FovusFileAttributes>empty();
        }
    }

    private FovusFileAttributes readAttr0(FovusPath fovusPath) throws IOException {
        FovusFileMetadata fileMetadata = fovusFileMetadataLookup.lookup(fovusPath);

        // parse the data to BasicFileAttributes.
        FileTime lastModifiedTime = null;
        if (fileMetadata.getLastModified() != null) {
            lastModifiedTime = FileTime.from(fileMetadata.getLastModified().getTime(), TimeUnit.MILLISECONDS);
        }

        long size = fileMetadata.getSize();
        boolean directory = false;
        boolean regularFile = false;
        String key = fileMetadata.getKey();
        // Check if this is a directory on Fovus Storage and the key explicitly exists (i.e, an empty directory object was created)
        if (fileMetadata.getKey().equals(fovusPath.getKey() + "/") && fileMetadata.getKey().endsWith("/")) {
            directory = true;
        }
        // Here it is a directory, but the key doest not explicitly exist
        else if ((!fileMetadata.getKey().equals(fovusPath.getKey()) || "".equals(fovusPath.getKey())) && fileMetadata.getKey().startsWith(fovusPath.getKey())) {
            directory = true;
            // no metadata, we fake one
            size = 0;
            // delete extra part
            key = fovusPath.getKey() + "/";
        }
        // is a file:
        else {
            regularFile = true;
        }

        return new FovusFileAttributes(key, lastModifiedTime, size, directory, regularFile);
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

    protected FovusFileSystem createFileSystem(URI uri, FovusConfig fovusConfig) {
        FovusJobClient fovusJobClient = new FovusJobClient(fovusConfig);
        return new FovusFileSystem(this, fovusJobClient, uri);
    }


    /**
     * check that the paths exists or not
     *
     * @param path FovusFovusPath
     * @return true if exists
     */
    @Override
    public boolean exists(Path path, LinkOption... options) {
        if (path instanceof FovusPath fovusPath) { // Java 16+ pattern matching for instanceof
            if (fovusPath.isPipelinesAreaRoot()) {
                return true;
            }
            if (isPipelines(fovusPath)) {
                try {
                    return pipelines(fovusPath).exists(fovusPath);
                } catch (IOException e) {
                    return false;
                }
            }
            try {
                fovusFileMetadataLookup.lookup(fovusPath);
                return true;
            } catch (NoSuchFileException e) { // <-- more specific exception preferred
                return false;
            } catch (IOException e) { // fallback if lookup does I/O
                return false;
            }
        }
        return super.exists(path, options); // no else needed — early return above
    }

    private static boolean isPipelines(Path path) {
        return path instanceof FovusPath && FovusPath.PIPELINES.equals(((FovusPath) path).getFileType());
    }

    private static boolean isPipelinesAreaRoot(Path path) {
        return path instanceof FovusPath && ((FovusPath) path).isPipelinesAreaRoot();
    }

    private static S3Storage pipelines(Path path) {
        return ((FovusPath) path).getFileSystem().pipelinesStorage();
    }
}
