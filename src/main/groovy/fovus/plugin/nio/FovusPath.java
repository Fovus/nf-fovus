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

import com.google.common.base.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.util.Objects;

import static com.google.common.collect.Iterables.*;
import static java.lang.String.format;

public class FovusPath implements Path {

    public static final String PATH_SEPARATOR = "/";

    public static final String FOVUS_PATH_PREFIX = "/fovus-storage";

    /**
     * Parts without fovus-storage prefix and fileType name.
     */
    private final List<String> parts;

    /**
     * actual filesystem
     */
    private final FovusFileSystem fileSystem;

    private FovusFileMetadata fileMetadata;

    private String fileType;


    public List<String> getParts() {
        return new ArrayList<>(parts);
    }

    /**
     * Get the file type of this Fovus Path
     *
     * @return files or jobs.
     */
    public String getFileType() {
        return fileType;
    }

    /**
     * path must be a string of the form "/fovus-storages/{fileType}/", "/fovus-storages/{fileType}/{key}", or just "{key}".
     * Examples:
     * <ul>
     * <li> "/fovus-storages/files/input.txt" good </li>
     * <li> "folder1/input.txt" good </li>
     * <li> "input.txt" good, use case would include getting the file name </li>
     * <li> "//input.txt" error, missing fovus-storage and file type</li>
     * <li> "/files/input.txt" error, missing fovus-storage </li>
     * </ul>
     *
     */
    public FovusPath(FovusFileSystem fileSystem, String path) {
        this(fileSystem, path, "");
    }

    /**
     * Build an FovusPath from path segments. '/' are stripped from each segment.
     *
     * @param first If it is an absolute path, it should star with a '/' and the first element is 'fovus-storage' prefix, followed by {fileType}/.
     * @param more  directories and files
     */
    public FovusPath(FovusFileSystem fileSystem, String first,
                     String... more) {
        this.fileType = null;

        List<String> parts = Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(first));
        if (first.endsWith(PATH_SEPARATOR)) {
            parts.remove(parts.size() - 1);
        }

        if (first.startsWith(PATH_SEPARATOR)) { // absolute path
            Preconditions.checkArgument(parts.size() >= 2 &&
                            parts.get(1).equals("fovus-storage") &&
                            (parts.get(2).equals("jobs") || parts.get(2).equals("files") || parts.get(2).equals("shared")),
                    "Invalid Fovus file path. Path must start with fovus-storage prefix and followed by 'files' or 'jobs' or 'shared");

            fileType = parts.get(2);
            if (fileType.equals("shared")) {
                throw new UnsupportedOperationException("Shared files are not currently supported");
            }

            // Get the remaining parts after /fovus-storage/{fileType}/
            parts = parts.subList(3, parts.size());
        }

        List<String> moreSplitted = Lists.newArrayList();

        for (String part : more) {
            moreSplitted.addAll(Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(part)));
        }

        parts.addAll(moreSplitted);

        this.parts = KeyParts.parse(parts);
        this.fileSystem = fileSystem;
    }

    private FovusPath(FovusFileSystem fileSystem, String fileType,
                      Iterable<String> keys) {
        this.fileType = fileType;
        this.parts = KeyParts.parse(keys);
        this.fileSystem = fileSystem;
    }

    /**
     * The Fovus path file key that is relative to /fovus-storage/{fileType}
     * <b>note:</b> the final slash need to be added to save a directory
     */
    public String getKey() {
        if (parts.isEmpty()) {
            return "";
        }

        List<String> mutableParts = new ArrayList<>(parts);

        ImmutableList.Builder<String> builder = ImmutableList
                .<String>builder().addAll(mutableParts);
        return Joiner.on(PATH_SEPARATOR).join(builder.build());
    }

    /**
     * Get the corresponding remote file path of this {@link FovusPath} object relatively to /fovus-storage/
     */
    public String toRemoteFilePath() {
        return getFileType() + PATH_SEPARATOR + getKey();
    }
    
    @Override
    public FovusFileSystem getFileSystem() {
        return this.fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return fileType != null;
    }

    @Override
    public Path getRoot() {
        if (isAbsolute()) {
            return new FovusPath(fileSystem, FOVUS_PATH_PREFIX + PATH_SEPARATOR + getFileType());
        }

        return null;
    }

    @Override
    public Path getFileName() {
        if (parts.isEmpty()) {
            return null;
        }

        return new FovusPath(
                fileSystem,
                null,
                parts.subList(parts.size() - 1, parts.size())
        );
    }

    @Override
    public Path getParent() {
        if (parts.isEmpty()) {
            return null;
        }

        // Here, we only know the file name, so we can't get the parent
        if (parts.size() == 1 && fileType == null) {
            return null;
        }

        return new FovusPath(fileSystem, fileType,
                parts.subList(0, parts.size() - 1));
    }

    @Override
    public int getNameCount() {
        return parts.size();
    }

    @Override
    public Path getName(int index) {
        return new FovusPath(fileSystem, null, parts.subList(index, index + 1));
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        return new FovusPath(fileSystem, null, parts.subList(beginIndex, endIndex));
    }

    @Override
    public boolean startsWith(Path other) {

        if (other.getNameCount() > this.getNameCount()) {
            return false;
        }

        if (!(other instanceof FovusPath)) {
            return false;
        }

        FovusPath path = (FovusPath) other;

        // Here, we don't know the fileType of the other path
        if (path.parts.isEmpty() && path.fileType == null &&
                (!this.parts.isEmpty() || this.fileType != null)) {
            return false;
        }

        if ((path.getFileType() != null && !path.getFileType().equals(this.getFileType())) ||
                (path.getFileType() == null && this.getFileType() != null)) {
            return false;
        }

        for (int i = 0; i < path.parts.size(); i++) {
            if (!path.parts.get(i).equals(this.parts.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean startsWith(String path) {
        FovusPath other = new FovusPath(this.fileSystem, path);
        return this.startsWith(other);
    }

    @Override
    public boolean endsWith(Path other) {
        if (other.getNameCount() > this.getNameCount()) {
            return false;
        }
        // empty
        if (other.getNameCount() == 0 &&
                this.getNameCount() != 0) {
            return false;
        }

        if (!(other instanceof FovusPath)) {
            return false;
        }

        FovusPath path = (FovusPath) other;

        if ((path.getFileType() != null && !path.getFileType().equals(this.getFileType())) ||
                (path.getFileType() != null && this.getFileType() == null)) {
            return false;
        }

        // check subkeys

        int i = path.parts.size() - 1;
        int j = this.parts.size() - 1;
        for (; i >= 0 && j >= 0; ) {

            if (!path.parts.get(i).equals(this.parts.get(j))) {
                return false;
            }
            i--;
            j--;
        }
        return true;
    }

    @Override
    public boolean endsWith(String other) {
        return this.endsWith(new FovusPath(this.fileSystem, other));
    }

    @Override
    public Path normalize() {
        if (parts == null || parts.size() == 0)
            return this;

        return new FovusPath(fileSystem, fileType, normalize0(parts));
    }

    private Iterable<String> normalize0(List<String> parts) {
        final String s0 = Path.of(String.join(PATH_SEPARATOR, parts)).normalize().toString();
        return Lists.newArrayList(Splitter.on(PATH_SEPARATOR).split(s0));
    }

    @Override
    public Path resolve(Path other) {
        Preconditions.checkArgument(other instanceof FovusPath,
                "other must be an instance of %s", FovusPath.class.getName());

        FovusPath fovusPath = (FovusPath) other;

        if (fovusPath.isAbsolute()) {
            return fovusPath;
        }

        if (fovusPath.parts.isEmpty()) { // other is relative and empty
            return this;
        }

        return new FovusPath(fileSystem, fileType, concat(parts, fovusPath.parts));
    }

    @Override
    public Path resolve(String other) {
        return resolve(new FovusPath(this.getFileSystem(), other));
    }

    @Override
    public Path resolveSibling(Path other) {
        Preconditions.checkArgument(other instanceof FovusPath,
                "other must be an instance of %s", FovusPath.class.getName());

        FovusPath fovusPath = (FovusPath) other;

        Path parent = getParent();

        if (parent == null || fovusPath.isAbsolute()) {
            return fovusPath;
        }

        if (fovusPath.parts.isEmpty()) { // other is relative and empty
            return parent;
        }

        return new FovusPath(fileSystem, fileType, concat(
                parts.subList(0, parts.size() - 1), fovusPath.parts));
    }

    @Override
    public Path resolveSibling(String other) {
        return resolveSibling(new FovusPath(this.getFileSystem(), other));
    }

    @Override
    public Path relativize(Path other) {
        Preconditions.checkArgument(other instanceof FovusPath,
                "other must be an instance of %s", FovusPath.class.getName());
        FovusPath fovusPath = (FovusPath) other;

        if (this.equals(other)) {
            return new FovusPath(this.getFileSystem(), "");
        }

        Preconditions.checkArgument(isAbsolute(),
                "Path is already relative: %s", this);
        Preconditions.checkArgument(fovusPath.isAbsolute(),
                "Cannot relativize against a relative path: %s", fovusPath);
        Preconditions.checkArgument(fileType.equals(fovusPath.getFileType()),
                "Cannot relativize paths with different file type: '%s', '%s'",
                this, other);

        Preconditions.checkArgument(parts.size() <= fovusPath.parts.size(),
                "Cannot relativize against a parent path: '%s', '%s'",
                this, other);


        int startPart = 0;
        for (int i = 0; i < this.parts.size(); i++) {
            if (this.parts.get(i).equals(fovusPath.parts.get(i))) {
                startPart++;
            }
        }

        List<String> resultParts = new ArrayList<>();
        for (int i = startPart; i < fovusPath.parts.size(); i++) {
            resultParts.add(fovusPath.parts.get(i));
        }

        return new FovusPath(fileSystem, null, resultParts);
    }

    @Override
    public URI toUri() {
        StringBuilder builder = new StringBuilder();
        builder.append("fovus://");
        builder.append(FOVUS_PATH_PREFIX);
        builder.append(PATH_SEPARATOR);
        builder.append(fileType);
        builder.append(PATH_SEPARATOR);
        builder.append(Joiner.on(PATH_SEPARATOR).join(parts));

        // Eg: fovus:///fovus-storage/{fileType}/{key}
        return URI.create(builder.toString());
    }

    @Override
    public Path toAbsolutePath() {
        if (isAbsolute()) {
            return this;
        }

        throw new IllegalStateException(format(
                "Relative path cannot be made absolute: %s", this));
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events,
                             WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Path> iterator() {
        ImmutableList.Builder<Path> builder = ImmutableList.builder();

        for (Iterator<String> iterator = parts.iterator(); iterator.hasNext(); ) {
            String part = iterator.next();
            builder.add(new FovusPath(fileSystem, null, ImmutableList.of(part)));
        }

        return builder.build().iterator();
    }

    @Override
    public int compareTo(Path other) {
        return toString().compareTo(other.toString());
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        if (isAbsolute()) {
            builder.append(FOVUS_PATH_PREFIX);
            builder.append(PATH_SEPARATOR);
            builder.append(fileType);
            builder.append(PATH_SEPARATOR);
        }

        builder.append(Joiner.on(PATH_SEPARATOR).join(parts));

        return builder.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FovusPath path = (FovusPath) other;

        if (!Objects.equals(fileType, path.fileType)) {
            return false;
        }

        return parts.equals(path.parts);
    }

    @Override
    public int hashCode() {
        int result = fileType != null ? fileType.hashCode() : 0;
        result = 31 * result + parts.hashCode();
        return result;
    }

    /**
     * This method returns the cached {@link FovusFileMetadata} instance if this path has been created
     * while iterating a directory structures by the {@link FovusPathIterator}.
     * <br>
     * After calling this method the cached object is reset, so any following method invocation will return {@code null}.
     * This is necessary to discard the object meta-data and force to reload file attributes when required.
     *
     * @return The cached {@link FovusFileMetadata} for this path if any.
     */
    public FovusFileMetadata getFileMetadata() {
        FovusFileMetadata result = fileMetadata;
        fileMetadata = null;
        return result;
    }

    // note: package scope to limit the access to this setter
    void setFileMetadata(FovusFileMetadata fileMetadata) {
        this.fileMetadata = fileMetadata;
    }


    // ~ helpers methods
    private static Function<String, String> strip(final String... strs) {
        return new Function<String, String>() {
            public String apply(String input) {
                String res = input;
                for (String str : strs) {
                    res = res.replace(str, "");
                }
                return res;
            }
        };
    }

    private static Predicate<String> notEmpty() {
        return new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                return input != null && !input.isEmpty();
            }
        };
    }

    /*
     * delete redundant "/" and empty parts
     */
    private abstract static class KeyParts {

        private static ImmutableList<String> parse(List<String> parts) {
            return ImmutableList.copyOf(filter(transform(parts, strip("/")), notEmpty()));
        }

        private static ImmutableList<String> parse(Iterable<String> parts) {
            return ImmutableList.copyOf(filter(transform(parts, strip("/")), notEmpty()));
        }
    }

    /*
     * Helper method to get file type from URI to create a file system per file type
     */
    public static String getFileTypeOfUri(URI uri) {
        final String path = uri.getPath();
        if (path == null || !path.startsWith("/"))
            throw new IllegalArgumentException("Invalid Fovus path: " + uri);
        final String[] parts = path.split("/");
        // Part 0 is empty string
        // Part 1 is "fovus-storage"
        // Part 2 is "fileType"
        return parts.length > 2 ? parts[2] : null;
    }
}
