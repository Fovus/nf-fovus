package fovus.plugin.s3

import groovy.transform.CompileStatic

/**
 * Direct-mode storage credentials could not be obtained. The message is always safe to show and log:
 * it never contains key material or the Fovus CLI's stdout.
 */
@CompileStatic
class StorageCredentialsException extends IOException {

    /** True when trying again later may succeed, e.g. a network error; false for sign-in or contract problems. */
    final boolean retryable

    StorageCredentialsException(String message, boolean retryable) {
        super(message)
        this.retryable = retryable
    }
}
