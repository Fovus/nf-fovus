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

package nextflow.fovus.nio;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import nextflow.fovus.job.FovusJobClient;
import nextflow.fovus.FovusConfig;
import nextflow.fovus.nio.util.S3MultipartOptions;
import nextflow.fovus.nio.util.S3ObjectSummaryLookup;
import nextflow.extension.FilesEx;
import nextflow.file.CopyOptions;
import nextflow.file.FileHelper;
import nextflow.file.FileSystemTransferAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;

/**
 * Spec:
 * <p>
 * URI: s3://[endpoint]/{bucket}/{key} If endpoint is missing, it's assumed to
 * be the default S3 endpoint (s3.amazonaws.com)
 * <p>
 * FileSystem roots: /{bucket}/
 * <p>
 * Treatment of S3 objects: - If a key ends in "/" it's considered a directory
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
 * <p>
 * Future versions of this provider might allow for a strict mode that mimics
 * the semantics of the FileSystem provider API on a best effort basis, at an
 * increased processing cost.
 *
 *
 */
public class FovusFileSystemProvider extends FileSystemProvider implements FileSystemTransferAware {

    private static final Logger log = LoggerFactory.getLogger(FovusFileSystemProvider.class);

    final Map<String, FovusFileSystem> fileSystems = new HashMap<>();

    private final S3ObjectSummaryLookup s3ObjectSummaryLookup = new S3ObjectSummaryLookup();

    @Override
    public String getScheme() {
        return "fovus";
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        Preconditions.checkNotNull(uri, "uri is null");
        Preconditions.checkArgument(uri.getScheme().equals("fovus"), "uri scheme must be 'fovus': '%s'", uri);

        final String bucketName = FovusPath.bucketName(uri);
        synchronized (fileSystems) {
            if (fileSystems.containsKey(bucketName))
                throw new FileSystemAlreadyExistsException("S3 filesystem already exists. Use getFileSystem() instead");
            final FovusConfig fovusConfig = new FovusConfig(env);
            final FovusFileSystem result = createFileSystem(uri, fovusConfig);
            fileSystems.put(bucketName, result);
            return result;
        }
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        final String bucketName = FovusPath.bucketName(uri);

        final FileSystem fileSystem = this.fileSystems.get(bucketName);

        if (fileSystem == null) {
            throw new FileSystemNotFoundException("Fovus filesystem not yet created. Use newFileSystem() instead");
        }

        return fileSystem;
    }

    /**
     * Deviation from spec: throws FileSystemNotFoundException if FileSystem
     * hasn't yet been initialized. Call newFileSystem() first.
     * Need credentials. Maybe set credentials after? how?
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

        return new DirectoryStream<Path>() {
            @Override
            public void close() throws IOException {
                // nothing to do here
            }

            @Override
            public Iterator<Path> iterator() {
                return new FovusS3Iterator(fovusPath.getKey() + "/", fovusPath);
            }
        };
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options)
            throws IOException {
        log.trace("Inside newInputStream");
        log.trace("path: {}", path);

        Preconditions.checkArgument(options.length == 0,
                "OpenOptions not yet supported: %s",
                ImmutableList.copyOf(options)); // TODO

        Preconditions.checkArgument(path instanceof FovusPath,
                "path must be an instance of %s", FovusPath.class.getName());
        FovusPath fovusPath = (FovusPath) path;

        Preconditions.checkArgument(!fovusPath.getKey().equals(""),
                "cannot create InputStream for root directory: %s", FilesEx.toUriString(fovusPath));

        InputStream result;
        Path tmpDir = Files.createTempDirectory(Path.of("/tmp"), "fovus-");
        try {

            String s3Key = fovusPath.getKey();
            String[] parts = s3Key.split("/");
            fovusPath
                    .getFileSystem()
                    .getClient()
                    .downloadJobFile(fovusPath.getFileJobId(), tmpDir.toAbsolutePath().toString(), fovusPath.getKey());
            Path tempFilePath = tmpDir.resolve(String.join("/", Arrays.copyOfRange(parts, 2, parts.length)));
            result = new FileInputStream(tempFilePath.toFile());
        } catch (Exception e) {
            throw new IOException(String.format("Cannot access file: %s", FilesEx.toUriString(fovusPath)), e);
        }

        return result;
    }

    @Override
    public OutputStream newOutputStream(final Path path, final OpenOption... options) throws IOException {
        Preconditions.checkArgument(path instanceof FovusPath, "path must be an instance of %s", FovusPath.class.getName());
        FovusPath fovusPath = (FovusPath) path;

        // validate options
        if (options.length > 0) {
            Set<OpenOption> opts = new LinkedHashSet<>(Arrays.asList(options));

            // cannot handle APPEND here -> use newByteChannel() implementation
            if (opts.contains(StandardOpenOption.APPEND)) {
                return super.newOutputStream(path, options);
            }

            if (opts.contains(StandardOpenOption.READ)) {
                throw new IllegalArgumentException("READ not allowed");
            }

            boolean create = opts.remove(StandardOpenOption.CREATE);
            boolean createNew = opts.remove(StandardOpenOption.CREATE_NEW);
            boolean truncateExisting = opts.remove(StandardOpenOption.TRUNCATE_EXISTING);

            // remove irrelevant/ignored options
            opts.remove(StandardOpenOption.WRITE);
            opts.remove(StandardOpenOption.SPARSE);

            if (!opts.isEmpty()) {
                throw new UnsupportedOperationException(opts.iterator().next() + " not supported");
            }

            if (!(create && truncateExisting)) {
                if (exists(fovusPath)) {
                    if (createNew || !truncateExisting) {
                        throw new FileAlreadyExistsException(FilesEx.toUriString(fovusPath));
                    }
                } else {
                    if (!createNew && !create) {
                        throw new NoSuchFileException(FilesEx.toUriString(fovusPath));
                    }
                }
            }
        }

        return createUploaderOutputStream(fovusPath);
    }


    @Override
    public boolean canUpload(Path source, Path target) {
        return FileSystems.getDefault().equals(source.getFileSystem()) && target instanceof FovusPath;
    }

    @Override
    public boolean canDownload(Path source, Path target) {
        return source instanceof FovusPath && FileSystems.getDefault().equals(target.getFileSystem());
    }

    @Override
    public void download(Path remoteFile, Path localDestination, CopyOption... options) throws IOException {
        final FovusPath source = (FovusPath) remoteFile;

        final CopyOptions opts = CopyOptions.parse(options);
        // delete target if it exists and REPLACE_EXISTING is specified
        if (opts.replaceExisting()) {
            FileHelper.deletePath(localDestination);
        } else if (Files.exists(localDestination))
            throw new FileAlreadyExistsException(localDestination.toString());

        final Optional<FovusS3FileAttributes> attrs = readAttr1(source);
        final boolean isDir = attrs.isPresent() && attrs.get().isDirectory();
        final String type = isDir ? "directory" : "file";
        final FovusJobClient fovusS3Client = source.getFileSystem().getClient();
        log.debug("Fovus download {} from={} to={}", type, FilesEx.toUriString(source), localDestination);


        fovusS3Client.downloadJobFile(source.getFileJobId(), localDestination.toAbsolutePath().toString(), source.getKey());
    }

    @Override
    public void upload(Path localFile, Path remoteDestination, CopyOption... options) throws IOException {
        final FovusPath target = (FovusPath) remoteDestination;

        CopyOptions opts = CopyOptions.parse(options);
        LinkOption[] linkOptions = (opts.followLinks()) ? new LinkOption[0] : new LinkOption[]{LinkOption.NOFOLLOW_LINKS};

        // attributes of source file
        if (Files.readAttributes(localFile, BasicFileAttributes.class, linkOptions).isSymbolicLink())
            throw new IOException("Uploading of symbolic links not supported - offending path: " + localFile);

        final Optional<FovusS3FileAttributes> attrs = readAttr1(target);
        final boolean exits = attrs.isPresent();

        // delete target if it exists and REPLACE_EXISTING is specified
        if (opts.replaceExisting()) {
            FileHelper.deletePath(target);
        } else if (exits)
            throw new FileAlreadyExistsException(target.toString());

        final boolean isDir = Files.isDirectory(localFile);
        final String type = isDir ? "directory" : "file";
        log.debug("S3 upload {} from={} to={}", type, localFile, FilesEx.toUriString(target));
        final FovusJobClient s3Client = target.getFileSystem().getClient();
        //            TODO: Replace
        s3Client.uploadJobFile(localFile.toFile().toString(), target.getKey(), target.getFileJobId());

//		if( isDir ) {
//			s3Client.uploadDirectory(localFile.toFile(), target);
//		}
//		else {
//			s3Client.uploadFile(localFile.toFile(), target);
//		}
    }

    private FovusS3OutputStream createUploaderOutputStream(FovusPath fileToUpload) {
        FovusJobClient fovusJobClient = fileToUpload.getFileSystem().getClient();
        Properties props = fileToUpload.getFileSystem().properties();

        final String storageClass = fileToUpload.getStorageClass() != null ? fileToUpload.getStorageClass() : props.getProperty("upload_storage_class");
        final S3MultipartOptions opts = props != null ? new S3MultipartOptions(props) : new S3MultipartOptions();
//		final S3ObjectId objectId = fileToUpload.toS3ObjectId();
        FovusS3OutputStream stream = new FovusS3OutputStream(fovusJobClient, fileToUpload, opts);
        return stream;
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path,
                                              Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException {
        Preconditions.checkArgument(path instanceof FovusPath,
                "path must be an instance of %s", FovusPath.class.getName());
        final FovusPath fovusPath = (FovusPath) path;
        // we resolve to a file inside the temp folder with the fovusPath name
        final Path tempDir = createTempDir();
//        final Path tempFile = createTempDir().resolve(path.getFileName().toString());

        String downloadedPath = "";
        try {
//			InputStream is = fovusPath.getFileSystem().getClient()
//					.getObject(fovusPath.getBucket(), fovusPath.getKey())
//					.getObjectContent();
//            InputStream is = null;
//            if (is == null)
//                throw new IOException(String.format("The specified path is a directory: %s", path));
//
//            Files.write(tempFile, IOUtils.toByteArray(is));
            downloadedPath = fovusPath
                    .getFileSystem()
                    .getClient()
                    .downloadJobFile(fovusPath.getFileJobId(), tempDir.toAbsolutePath().toString(), fovusPath.getKey());

            log.trace("[FOVUS] In newByteChannel - downloadedPath: {}", downloadedPath);
            if (downloadedPath.isEmpty()) {
                throw new IOException(String.format("Cannot access file: %s", path));
            }
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 404)
                throw new IOException(String.format("Cannot access file: %s", path), e);
        }

        // and we can use the File SeekableByteChannel implementation
        final Path tempFile = Path.of(downloadedPath);
        final SeekableByteChannel seekable = Files.newByteChannel(tempFile, options);
        final List<Tag> tags = ((FovusPath) path).getTagsList();
        final String contentType = ((FovusPath) path).getContentType();

        return new SeekableByteChannel() {
            @Override
            public boolean isOpen() {
                return seekable.isOpen();
            }

            @Override
            public void close() throws IOException {

                if (!seekable.isOpen()) {
                    return;
                }
                seekable.close();
                // upload the content where the seekable ends (close)
                if (Files.exists(tempFile)) {
                    ObjectMetadata metadata = new ObjectMetadata();
                    metadata.setContentLength(Files.size(tempFile));
                    // FIXME: #20 ServiceLoader can't load com.upplication.s3fs.util.FileTypeDetector when this library is used inside a ear :(
                    metadata.setContentType(Files.probeContentType(tempFile));

                    try (InputStream stream = Files.newInputStream(tempFile)) {
                        /*
                         FIXME: if the stream is {@link InputStream#markSupported()} i can reuse the same stream
                         and evict the close and open methods of probeContentType. By this way:
                         metadata.setContentType(new Tika().detect(stream, tempFile.getFileName().toString()));
                        */
                        fovusPath.getFileSystem()
                                .getClient()
                                .uploadJobFile(tempFile.toFile().toString(), fovusPath.getKey(), fovusPath.getFileJobId());
                    }
                } else {
                    // delete: check option delete_on_close
//                    fovusPath.getFileSystem().
//                        getClient().deleteObject(fovusPath.getBucket(), fovusPath.getKey());
                }
                // and delete the temp dir
                Files.deleteIfExists(tempFile);
                Files.deleteIfExists(tempFile.getParent());
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                return seekable.write(src);
            }

            @Override
            public SeekableByteChannel truncate(long size) throws IOException {
                return seekable.truncate(size);
            }

            @Override
            public long size() throws IOException {
                return seekable.size();
            }

            @Override
            public int read(ByteBuffer dst) throws IOException {
                return seekable.read(dst);
            }

            @Override
            public SeekableByteChannel position(long newPosition)
                    throws IOException {
                return seekable.position(newPosition);
            }

            @Override
            public long position() throws IOException {
                return seekable.position();
            }
        };
    }

    /**
     * Deviations from spec: Does not perform atomic check-and-create. Since a
     * directory is just an S3 object, all directories in the hierarchy are
     * created or it already existed.
     */
    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs)
            throws IOException {

        // FIXME: throw exception if the same key already exists at amazon s3

        FovusPath fovusPath = (FovusPath) dir;

        Preconditions.checkArgument(attrs.length == 0,
                "attrs not yet supported: %s", ImmutableList.copyOf(attrs)); // TODO

        List<Tag> tags = fovusPath.getTagsList();
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        String keyName = fovusPath.getKey()
                + (fovusPath.getKey().endsWith("/") ? "" : "/");

        //TODO Update
        fovusPath.getFileSystem()
                .getClient()
                .uploadEmptyDirectory(keyName, fovusPath.getFileJobId());
    }

    @Override
    public void delete(Path path) throws IOException {
        Preconditions.checkArgument(path instanceof FovusPath,
                "path must be an instance of %s", FovusPath.class.getName());

        FovusPath fovusPath = (FovusPath) path;

        if (Files.notExists(path)) {
            throw new NoSuchFileException("the path: " + FilesEx.toUriString(fovusPath) + " does not exist");
        }

        if (Files.isDirectory(path)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                if (stream.iterator().hasNext()) {
                    throw new DirectoryNotEmptyException("the path: " + FilesEx.toUriString(fovusPath) + " is a directory and is not empty");
                }
            }
        }

        // we delete the two objects (sometimes exists the key '/' and sometimes not)
        //TODO update
//		fovusPath.getFileSystem().getClient()
//			.deleteObject(fovusPath.getBucket(), fovusPath.getKey());
//		fovusPath.getFileSystem().getClient()
//			.deleteObject(fovusPath.getBucket(), fovusPath.getKey() + "/");
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options)
            throws IOException {
        Preconditions.checkArgument(source instanceof FovusPath,
                "source must be an instance of %s", FovusPath.class.getName());
        Preconditions.checkArgument(target instanceof FovusPath,
                "target must be an instance of %s", FovusPath.class.getName());

        if (isSameFile(source, target)) {
            // TODO add copy object
            return;
        }

        FovusPath s3Source = (FovusPath) source;
        FovusPath s3Target = (FovusPath) target;
        /*
         * Preconditions.checkArgument(!s3Source.isDirectory(),
         * "copying directories is not yet supported: %s", source); // TODO
         * Preconditions.checkArgument(!s3Target.isDirectory(),
         * "copying directories is not yet supported: %s", target); // TODO
         */
        ImmutableSet<CopyOption> actualOptions = ImmutableSet.copyOf(options);
        verifySupportedOptions(EnumSet.of(StandardCopyOption.REPLACE_EXISTING),
                actualOptions);

        if (!actualOptions.contains(StandardCopyOption.REPLACE_EXISTING)) {
            if (exists(s3Target)) {
                throw new FileAlreadyExistsException(format(
                        "target already exists: %s", FilesEx.toUriString(s3Target)));
            }
        }

        FovusJobClient client = s3Source.getFileSystem().getClient();
        Properties props = s3Target.getFileSystem().properties();

        // TODO add copy object

        //		final ObjectMetadata sourceObjMetadata = s3Source.getFileSystem().getClient().getObjectMetadata(s3Source.getBucket(), s3Source.getKey());
        final S3MultipartOptions opts = props != null ? new S3MultipartOptions(props) : new S3MultipartOptions();
        final long maxSize = opts.getMaxCopySize();
//		final long length = sourceObjMetadata.getContentLength();
        final List<Tag> tags = ((FovusPath) target).getTagsList();
        final String contentType = ((FovusPath) target).getContentType();
        final String storageClass = ((FovusPath) target).getStorageClass();

//		if( length <= maxSize ) {
//			CopyObjectRequest copyObjRequest = new CopyObjectRequest(s3Source.getBucket(), s3Source.getKey(),s3Target.getBucket(), s3Target.getKey());
//			log.trace("Copy file via copy object - source: source={}, target={}, tags={}, storageClass={}", s3Source, s3Target, tags, storageClass);
//			client.copyObject(copyObjRequest, tags, contentType, storageClass);
//		}
//		else {
//			log.trace("Copy file via multipart upload - source: source={}, target={}, tags={}, storageClass={}", s3Source, s3Target, tags, storageClass);
//			client.multipartCopyObject(s3Source, s3Target, length, opts, tags, contentType, storageClass);
//		}
    }


    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        for (CopyOption it : options) {
            if (it == StandardCopyOption.ATOMIC_MOVE)
                throw new IllegalArgumentException("Atomic move not supported by S3 file system provider");
        }
        copy(source, target, options);
        delete(source);
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
        FovusPath fovusPath = (FovusPath) path;
        Preconditions.checkArgument(fovusPath.isAbsolute(),
                "path must be absolute: %s", fovusPath);

        FovusJobClient client = fovusPath.getFileSystem().getClient();

        // TODO
//		if( modes==null || modes.length==0 ) {
//			// when no modes are given, the method is invoked
//			// by `Files.exists` method, therefore just use summary lookup
//			s3ObjectSummaryLookup.lookup((FovusFovusPath)path);
//			return;
//		}
//
//		// get ACL and check if the file exists as a side-effect
//		AccessControlList acl = getAccessControl(fovusPath);
//
//		for (AccessMode accessMode : modes) {
//			switch (accessMode) {
//			case EXECUTE:
//				throw new AccessDeniedException(fovusPath.toString(), null,
//						"file is not executable");
//			case READ:
//				if (!hasPermissions(acl, client.getS3AccountOwner(),
//						EnumSet.of(Permission.FullControl, Permission.Read))) {
//					throw new AccessDeniedException(fovusPath.toString(), null,
//							"file is not readable");
//				}
//				break;
//			case WRITE:
//				if (!hasPermissions(acl, client.getS3AccountOwner(),
//						EnumSet.of(Permission.FullControl, Permission.Write))) {
//					throw new AccessDeniedException(fovusPath.toString(), null,
//							format("bucket '%s' is not writable",
//									fovusPath.getBucket()));
//				}
//				break;
//			}
//		}
    }

//    /**
//     * check if the param acl has the same owner than the parameter owner and
//     * have almost one of the permission set in the parameter permissions
//     * @param acl
//     * @param owner
//     * @param permissions almost one
//     * @return
//     */
//	private boolean hasPermissions(AccessControlList acl, Owner owner,
//			EnumSet<Permission> permissions) {
//		boolean result = false;
//		for (Grant grant : acl.getGrants()) {
//			if (grant.getGrantee().getIdentifier().equals(owner.getId())
//					&& permissions.contains(grant.getPermission())) {
//				result = true;
//				break;
//			}
//		}
//		return result;
//	}

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        Preconditions.checkArgument(path instanceof FovusPath,
                "path must be an instance of %s", FovusPath.class.getName());
        FovusPath fovusPath = (FovusPath) path;
        if (type.isAssignableFrom(BasicFileAttributeView.class)) {
            try {
                return (V) new FovusS3FileAttributesView(readAttr0(fovusPath));
            } catch (IOException e) {
                throw new RuntimeException("Unable read attributes for file: " + FilesEx.toUriString(fovusPath), e);
            }
        }
        throw new UnsupportedOperationException("Not a valid S3 file system provider file attribute view: " + type.getName());
    }


    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        Preconditions.checkArgument(path instanceof FovusPath,
                "path must be an instance of %s", FovusPath.class.getName());
        FovusPath fovusPath = (FovusPath) path;

        if (type.isAssignableFrom(BasicFileAttributes.class)) {
            A attributes = (A) ("".equals(fovusPath.getKey())
                    // the root bucket is implicitly a directory
                    ? new FovusS3FileAttributes("/", null, 0, true, false)
                    // read the target path attributes
                    : readAttr0(fovusPath));
            log.trace("+++ Attributes for path {}: {}", path, attributes);
            return attributes;
        }
        // not support attribute class
        throw new UnsupportedOperationException(format("only %s supported", BasicFileAttributes.class));
    }

    private Optional<FovusS3FileAttributes> readAttr1(FovusPath fovusPath) throws IOException {
        try {
            return Optional.of(readAttr0(fovusPath));
        } catch (NoSuchFileException e) {
            return Optional.<FovusS3FileAttributes>empty();
        }
    }

    private FovusS3FileAttributes readAttr0(FovusPath fovusPath) throws IOException {
        S3ObjectSummary objectSummary = s3ObjectSummaryLookup.lookup(fovusPath);
        log.trace("+++ Object summary for path {}: {}", fovusPath, objectSummary);

        // parse the data to BasicFileAttributes.
        FileTime lastModifiedTime = null;
        if (objectSummary.getLastModified() != null) {
            lastModifiedTime = FileTime.from(objectSummary.getLastModified().getTime(), TimeUnit.MILLISECONDS);
        }

        long size = objectSummary.getSize();
        boolean directory = false;
        boolean regularFile = false;
        String key = objectSummary.getKey();
        // check if is a directory and the key of this directory exists in amazon s3
        if (objectSummary.getKey().equals(fovusPath.getKey() + "/") && objectSummary.getKey().endsWith("/")) {
            directory = true;
        }
        // is a directory but does not exist in amazon s3
        else if ((!objectSummary.getKey().equals(fovusPath.getKey()) || "".equals(fovusPath.getKey())) && objectSummary.getKey().startsWith(fovusPath.getKey())) {
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

        return new FovusS3FileAttributes(key, lastModifiedTime, size, directory, regularFile);
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

    protected ClientConfiguration createClientConfig(Properties props) {
        ClientConfiguration config = new ClientConfiguration();

        if (props == null)
            return config;

        if (props.containsKey("connection_timeout")) {
            log.trace("AWS client config - connection_timeout: {}", props.getProperty("connection_timeout"));
            config.setConnectionTimeout(Integer.parseInt(props.getProperty("connection_timeout")));
        }

        if (props.containsKey("max_connections")) {
            log.trace("AWS client config - max_connections: {}", props.getProperty("max_connections"));
            config.setMaxConnections(Integer.parseInt(props.getProperty("max_connections")));
        }

        if (props.containsKey("max_error_retry")) {
            log.trace("AWS client config - max_error_retry: {}", props.getProperty("max_error_retry"));
            config.setMaxErrorRetry(Integer.parseInt(props.getProperty("max_error_retry")));
        }

        if (props.containsKey("protocol")) {
            log.trace("AWS client config - protocol: {}", props.getProperty("protocol"));
            config.setProtocol(Protocol.valueOf(props.getProperty("protocol").toUpperCase()));
        }

        if (props.containsKey("proxy_domain")) {
            log.trace("AWS client config - proxy_domain: {}", props.getProperty("proxy_domain"));
            config.setProxyDomain(props.getProperty("proxy_domain"));
        }

        if (props.containsKey("proxy_host")) {
            log.trace("AWS client config - proxy_host: {}", props.getProperty("proxy_host"));
            config.setProxyHost(props.getProperty("proxy_host"));
        }

        if (props.containsKey("proxy_port")) {
            log.trace("AWS client config - proxy_port: {}", props.getProperty("proxy_port"));
            config.setProxyPort(Integer.parseInt(props.getProperty("proxy_port")));
        }

        if (props.containsKey("proxy_username")) {
            log.trace("AWS client config - proxy_username: {}", props.getProperty("proxy_username"));
            config.setProxyUsername(props.getProperty("proxy_username"));
        }

        if (props.containsKey("proxy_password")) {
            log.trace("AWS client config - proxy_password: {}", props.getProperty("proxy_password"));
            config.setProxyPassword(props.getProperty("proxy_password"));
        }

        if (props.containsKey("proxy_workstation")) {
            log.trace("AWS client config - proxy_workstation: {}", props.getProperty("proxy_workstation"));
            config.setProxyWorkstation(props.getProperty("proxy_workstation"));
        }

        if (props.containsKey("signer_override")) {
            log.debug("AWS client config - signerOverride: {}", props.getProperty("signer_override"));
            config.setSignerOverride(props.getProperty("signer_override"));
        }

        if (props.containsKey("socket_send_buffer_size_hints") || props.containsKey("socket_recv_buffer_size_hints")) {
            log.trace("AWS client config - socket_send_buffer_size_hints: {}, socket_recv_buffer_size_hints: {}", props.getProperty("socket_send_buffer_size_hints", "0"), props.getProperty("socket_recv_buffer_size_hints", "0"));
            int send = Integer.parseInt(props.getProperty("socket_send_buffer_size_hints", "0"));
            int recv = Integer.parseInt(props.getProperty("socket_recv_buffer_size_hints", "0"));
            config.setSocketBufferSizeHints(send, recv);
        }

        if (props.containsKey("socket_timeout")) {
            log.trace("AWS client config - socket_timeout: {}", props.getProperty("socket_timeout"));
            config.setSocketTimeout(Integer.parseInt(props.getProperty("socket_timeout")));
        }

        if (props.containsKey("user_agent")) {
            log.trace("AWS client config - user_agent: {}", props.getProperty("user_agent"));
            config.setUserAgent(props.getProperty("user_agent"));
        }

        return config;
    }

    // ~~

    protected FovusFileSystem createFileSystem(URI uri, FovusConfig fovusConfig) {
        // try to load amazon props
        Properties props = loadAmazonProperties();
        // add properties for legacy compatibility

        FovusJobClient fovusJobClient;

        final String bucketName = FovusPath.bucketName(uri);
        // do not use `global` flag for custom endpoint because
        // when enabling that flag, it overrides S3 endpoints with AWS global endpoint
        // see https://github.com/nextflow-io/nextflow/pull/5779
        final boolean global = bucketName != null;
//		final FovusJobClientFactory factory = new FovusJobClientFactory();
        fovusJobClient = new FovusJobClient(fovusConfig);

        if (props.getProperty("glacier_auto_retrieval") != null)
            log.warn("Glacier auto-retrieval is no longer supported, config option `aws.client.glacierAutoRetrieval` will be ignored");

        return new FovusFileSystem(this, fovusJobClient, uri, props);
    }

    protected String getProp(Properties props, String... keys) {
        for (String k : keys) {
            if (props.containsKey(k)) {
                return props.getProperty(k);
            }
        }
        return null;
    }

    /**
     * find /amazon.properties in the classpath
     *
     * @return Properties amazon.properties
     */
    protected Properties loadAmazonProperties() {
        Properties props = new Properties();
        // http://www.javaworld.com/javaworld/javaqa/2003-06/01-qa-0606-load.html
        // http://www.javaworld.com/javaqa/2003-08/01-qa-0808-property.html
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("amazon.properties")) {
            if (in != null) {
                props.load(in);
            }

        } catch (IOException e) {
        }

        return props;
    }

    // ~~~

    private <T> void verifySupportedOptions(Set<? extends T> allowedOptions,
                                            Set<? extends T> actualOptions) {
        Sets.SetView<? extends T> unsupported = difference(actualOptions,
                allowedOptions);
        Preconditions.checkArgument(unsupported.isEmpty(),
                "the following options are not supported: %s", unsupported);
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
            try {
                log.trace("exists Checking file: {}", path.toString());
                s3ObjectSummaryLookup.lookup(fovusPath);
                return true;
            } catch (NoSuchFileException e) { // <-- more specific exception preferred
                return false;
            } catch (IOException e) { // fallback if lookup does I/O
                return false;
            }
        }
        return super.exists(path, options); // no else needed — early return above
    }


//	/**
//	 * Get the Control List, if the path does not exist
//     * (because the path is a directory and this key isn't created at amazon s3)
//     * then return the ACL of the first child.
//     *
//	 * @param path {@link FovusFovusPath}
//	 * @return AccessControlList
//	 * @throws NoSuchFileException if not found the path and any child
//	 */
//	private AccessControlList getAccessControl(FovusFovusPath path) throws NoSuchFileException{
//		S3ObjectSummary obj = s3ObjectSummaryLookup.lookup(path);
//		// check first for file:
//        return path.getFileSystem().getClient().getObjectAcl(obj.getBucketName(), obj.getKey());
//	}

    /**
     * create a temporal directory to create streams
     *
     * @return Path temporal folder
     * @throws IOException
     */
    protected Path createTempDir() throws IOException {
        return Files.createTempDirectory("temp-s3-");
    }

}
