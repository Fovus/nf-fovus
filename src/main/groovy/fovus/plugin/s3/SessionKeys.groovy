package fovus.plugin.s3

import groovy.transform.CompileStatic
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials

import java.time.Instant

/** One set of temporary AWS keys from the Fovus CLI. {@link #toString()} never shows the keys. */
@CompileStatic
final class SessionKeys {

    final String accessKeyId
    final String secretAccessKey
    final String sessionToken
    final Instant expiration

    SessionKeys(String accessKeyId, String secretAccessKey, String sessionToken, Instant expiration) {
        this.accessKeyId = accessKeyId
        this.secretAccessKey = secretAccessKey
        this.sessionToken = sessionToken
        this.expiration = expiration
    }

    AwsSessionCredentials toAwsCredentials() {
        return AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken)
    }

    @Override
    String toString() {
        return "SessionKeys(expires ${expiration})".toString()
    }
}
