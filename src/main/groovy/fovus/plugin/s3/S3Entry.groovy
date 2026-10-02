package fovus.plugin.s3

import groovy.transform.CompileStatic

import java.time.Instant

/** One object or folder prefix returned by {@link FovusS3Client}. */
@CompileStatic
final class S3Entry {

    final String key
    final long size
    final Instant lastModified
    final boolean directory

    S3Entry(String key, long size, Instant lastModified, boolean directory) {
        this.key = key
        this.size = size
        this.lastModified = lastModified
        this.directory = directory
    }
}
