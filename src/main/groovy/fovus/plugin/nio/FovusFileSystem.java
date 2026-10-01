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

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import fovus.plugin.job.FovusJobClient;
import fovus.plugin.s3.FovusS3Client;

public class FovusFileSystem extends FileSystem {

    private final FovusFileSystemProvider provider;

    private final FovusJobClient jobClient;


    private final String fileType;

    public static final String NOT_ATTACHED_MESSAGE =
            "Fovus storage pipelines/ paths can only be read or written in direct mode, after the Fovus executor has started";

    /** Direct mode only: set by the executor once storage credentials exist. */
    private volatile S3Storage pipelinesStorage;

    public FovusFileSystem(FovusFileSystemProvider provider, FovusJobClient client, URI uri) {
        this.provider = provider;
        this.jobClient = client;
        this.fileType = FovusPath.getFileTypeOfUri(uri);
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }


    @Override
    public void close() {
        this.provider.fileSystems.remove(fileType);
    }

    @Override
    public boolean isOpen() {
        return this.provider.fileSystems.containsKey(fileType);
    }

    @Override
    public boolean isReadOnly() {
        return !FovusPath.PIPELINES.equals(fileType);
    }

    @Override
    public String getSeparator() {
        return FovusPath.PATH_SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        ImmutableList.Builder<Path> builder = ImmutableList.builder();
        return builder.build();
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return ImmutableList.of();
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return ImmutableSet.of("basic");
    }

    @Override
    public Path getPath(String first, String... more) {
        if (more.length == 0) {
            return new FovusPath(this, first);
        }

        return new FovusPath(this, first, more);
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException();
    }

    public FovusJobClient getJobClient() {
        return jobClient;
    }

    /** Direct mode: give the pipelines/ file system its S3 client once storage credentials exist. */
    public void attachS3Client(FovusS3Client client) {
        this.pipelinesStorage = new S3Storage(client);
    }

    /** Direct mode: whether an S3 client is already attached (by the trace observer, or the executor). */
    public boolean hasS3Client() {
        return pipelinesStorage != null;
    }

    S3Storage pipelinesStorage() {
        final S3Storage storage = pipelinesStorage;
        if (storage == null) {
            throw new IllegalStateException(NOT_ATTACHED_MESSAGE);
        }
        return storage;
    }
}
