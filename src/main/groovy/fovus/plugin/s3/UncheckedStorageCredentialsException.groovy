package fovus.plugin.s3

import groovy.transform.CompileStatic

/**
 * Carries a {@link StorageCredentialsException} out of an AWS SDK credentials provider, whose interface
 * cannot throw checked exceptions. {@link FovusS3Client} unwraps it again.
 */
@CompileStatic
class UncheckedStorageCredentialsException extends RuntimeException {
    UncheckedStorageCredentialsException(StorageCredentialsException cause) {
        super(cause.message, cause)
    }
}
