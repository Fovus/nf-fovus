package fovus.plugin.s3

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.CompileStatic

import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * The read and write keys for one pipeline's work directory, as printed by
 * {@code fovus storage credentials} (credential contract v1). {@link #toString()} never shows keys.
 */
@CompileStatic
final class StorageCredentials {

    static final int CONTRACT_VERSION = 1
    static final String CONTRACT_ERROR = 'Unexpected response from Fovus CLI (expected contract v1)'

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

    final String bucket
    final String region
    final String prefix
    final SessionKeys read
    final SessionKeys write

    StorageCredentials(String bucket, String region, String prefix, SessionKeys read, SessionKeys write) {
        this.bucket = bucket
        this.region = region
        this.prefix = prefix
        this.read = read
        this.write = write
    }

    /** The earlier of the two expirations: both key sets are refreshed together. */
    Instant getExpiration() {
        return read.expiration.isBefore(write.expiration) ? read.expiration : write.expiration
    }

    /**
     * Parse the CLI's stdout. Anything that is not exactly one contract v1 document for
     * {@code expectedPrefix} raises {@link #contractError()}, whose message never includes the input.
     */
    static StorageCredentials parse(byte[] json, String expectedPrefix) throws StorageCredentialsException {
        final root = readTree(json)
        if (root == null || !root.isObject()) throw contractError()

        final version = root.path('Version')
        if (!version.isInt() || version.intValue() != CONTRACT_VERSION) throw contractError()

        final prefix = text(root, 'Prefix')
        if (prefix != expectedPrefix) throw contractError()

        return new StorageCredentials(text(root, 'Bucket'), text(root, 'Region'), prefix,
                                      keys(root.path('Read')), keys(root.path('Write')))
    }

    static StorageCredentialsException contractError() {
        return new StorageCredentialsException(CONTRACT_ERROR, false)
    }

    private static JsonNode readTree(byte[] json) {
        try {
            return MAPPER.readTree(json)
        }
        catch (Exception ignored) {
            // Never include the parser's message: it can quote part of the input
            throw contractError()
        }
    }

    private static SessionKeys keys(JsonNode node) {
        if (!node.isObject()) throw contractError()
        return new SessionKeys(text(node, 'AccessKeyId'), text(node, 'SecretAccessKey'), text(node, 'SessionToken'),
                               instant(text(node, 'Expiration')))
    }

    private static Instant instant(String value) {
        try {
            return Instant.parse(value)
        }
        catch (DateTimeParseException ignored) {
            throw contractError()
        }
    }

    private static String text(JsonNode node, String field) {
        final value = node.path(field)
        if (!value.isTextual() || value.textValue().isBlank()) throw contractError()
        return value.textValue()
    }

    @Override
    String toString() {
        return "StorageCredentials(bucket=${bucket}, prefix=${prefix}, expires ${expiration})".toString()
    }
}
