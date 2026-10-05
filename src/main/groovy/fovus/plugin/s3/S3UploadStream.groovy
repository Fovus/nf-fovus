package fovus.plugin.s3

import groovy.transform.CompileStatic

/**
 * An upload in progress: {@link #close()} publishes the object, {@link #abort()} discards it. Nothing is visible in
 * storage before {@code close()} succeeds, so a stream that is aborted, or never closed, publishes nothing.
 */
@CompileStatic
abstract class S3UploadStream extends OutputStream {

    /** Discard the upload; nothing becomes visible. A no-op once {@link #close()} has run. */
    abstract void abort()
}
