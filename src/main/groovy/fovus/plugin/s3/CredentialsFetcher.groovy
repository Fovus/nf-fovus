package fovus.plugin.s3

import groovy.transform.CompileStatic

/** Somewhere fresh direct-mode storage credentials come from; the Fovus CLI in production. */
@CompileStatic
interface CredentialsFetcher {
    StorageCredentials fetch() throws StorageCredentialsException
}
