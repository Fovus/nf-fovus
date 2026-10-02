package fovus.plugin.s3

import groovy.json.JsonOutput
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.time.Instant

class StorageCredentialsTest extends Specification {

    static final String PREFIX = 'pipelines/p-1-user/'

    /** A contract v1 document for {@link #PREFIX}; {@code overrides} replace top-level keys. */
    static String validJson(Map overrides = [:]) {
        final Map document = [
                Version: 1,
                Bucket : 'fovus-user-ws-us-east-2',
                Region : 'us-east-2',
                Prefix : PREFIX,
                Read   : [AccessKeyId: 'READ-KEY-ID', SecretAccessKey: 'READ-SECRET', SessionToken: 'READ-TOKEN',
                          Expiration : '2026-09-30T13:00:00Z'],
                Write  : [AccessKeyId: 'WRITE-KEY-ID', SecretAccessKey: 'WRITE-SECRET', SessionToken: 'WRITE-TOKEN',
                          Expiration : '2026-09-30T12:59:00Z'],
        ] + overrides
        return JsonOutput.toJson(document)
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8)
    }

    def 'parse should read every field of contract v1'() {
        when:
        def credentials = StorageCredentials.parse(bytes(validJson()), PREFIX)

        then:
        credentials.bucket == 'fovus-user-ws-us-east-2'
        credentials.region == 'us-east-2'
        credentials.prefix == PREFIX
        credentials.read.accessKeyId == 'READ-KEY-ID'
        credentials.read.secretAccessKey == 'READ-SECRET'
        credentials.read.sessionToken == 'READ-TOKEN'
        credentials.write.accessKeyId == 'WRITE-KEY-ID'
        credentials.expiration == Instant.parse('2026-09-30T12:59:00Z')
    }

    def 'parse should reject anything outside the contract without echoing it'() {
        when:
        StorageCredentials.parse(bytes(json), PREFIX)

        then:
        def e = thrown(StorageCredentialsException)
        e.message == StorageCredentials.CONTRACT_ERROR
        !e.retryable

        where:
        json << [
                'not json READ-SECRET',
                'Authenticating...\n' + validJson(),
                validJson() + '\nDone.',
                validJson(Version: 2),
                validJson(Version: '1'),
                validJson(Prefix: 'pipelines/p-2-user/'),
                validJson(Bucket: ''),
                validJson(Read: [AccessKeyId: 'A', SecretAccessKey: 'READ-SECRET', SessionToken: 'T']),
                validJson(Write: [AccessKeyId: 'A', SecretAccessKey: 'S', SessionToken: 'T', Expiration: 'tomorrow']),
                '[]',
        ]
    }

    def 'toString should never show key material'() {
        given:
        def credentials = StorageCredentials.parse(bytes(validJson()), PREFIX)

        expect:
        credentials.toString() == 'StorageCredentials(bucket=fovus-user-ws-us-east-2, prefix=pipelines/p-1-user/, expires 2026-09-30T12:59:00Z)'
        credentials.read.toString() == 'SessionKeys(expires 2026-09-30T13:00:00Z)'
    }

    def 'toAwsCredentials should carry the session token'() {
        when:
        def aws = StorageCredentials.parse(bytes(validJson()), PREFIX).write.toAwsCredentials()

        then:
        aws.accessKeyId() == 'WRITE-KEY-ID'
        aws.secretAccessKey() == 'WRITE-SECRET'
        aws.sessionToken() == 'WRITE-TOKEN'
    }
}
