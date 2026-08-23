package fovus.plugin.job

import groovy.json.JsonGenerator
import groovy.json.JsonSlurper
import spock.lang.Specification

class FovusJobConfigTest extends Specification {

    /** The same generator {@link FovusJobConfig#toJson} uses to write the create-job payload. */
    private static Map payload(FovusJobConfig config) {
        final json = new JsonGenerator.Options().excludeNulls().build().toJson(config)
        return new JsonSlurper().parseText(json) as Map
    }

    def 'storageConnectors should default to an empty list on a bare job config'() {
        expect:
        new FovusJobConfig().storageConnectors == []
    }

    def 'resolveStorageConnectors should default to an empty list when the attribute is absent or null'() {
        expect:
        FovusJobConfig.resolveStorageConnectors(extensionValue, defaultConnectors) == []

        where: 'the attribute is absent on the process, on the job config, or explicitly null on either'
        extensionValue | defaultConnectors
        null           | null
        null           | []
        []             | null
        []             | ['ignored-because-overridden'] as List<String>
        null           | [] as List<String>
    }

    def 'resolveStorageConnectors should keep a populated list'() {
        expect:
        FovusJobConfig.resolveStorageConnectors(['team-shared', 'archive-2024', 'ABC'], null) ==
                ['team-shared', 'archive-2024', 'ABC']
    }

    def 'resolveStorageConnectors should fall back to the job config value when the process does not set one'() {
        expect:
        FovusJobConfig.resolveStorageConnectors(null, ['from-job-config'] as List<String>) == ['from-job-config']
    }

    def 'resolveStorageConnectors should let the process override the job config value'() {
        expect:
        FovusJobConfig.resolveStorageConnectors(['from-process'], ['from-job-config'] as List<String>) ==
                ['from-process']
    }

    def 'resolveStorageConnectors should drop duplicates'() {
        expect:
        FovusJobConfig.resolveStorageConnectors(['shared', 'shared', 'other'], null) == ['shared', 'other']
    }

    def 'resolveStorageConnectors should ignore a process value that is not a list'() {
        expect:
        FovusJobConfig.resolveStorageConnectors('not-a-list', ['from-job-config'] as List<String>) ==
                ['from-job-config']
    }

    def 'resolveStorageConnectors should reject a malformed connector name and name the offending entry'() {
        when:
        FovusJobConfig.resolveStorageConnectors(['fine-one', offending], null)

        then:
        def error = thrown(Error)
        error.message.contains('Invalid storage connector name')
        error.message.contains("'${offending}'")

        where: 'anything outside letters, digits and hyphens is refused'
        offending << ['has space', 'has_underscore', 'has/slash', 'has.dot', 'UPPER lower', '', 'ünïcode']
    }

    def 'the malformed-name error should spell out the allowed shape'() {
        when:
        FovusJobConfig.resolveStorageConnectors(['team_shared'], null)

        then:
        def error = thrown(Error)
        error.message == "[Fovus] Invalid storage connector name: 'team_shared'. " +
                'A storage connector name may only contain letters, digits and hyphens (^[a-zA-Z0-9-]+$).'
    }

    def 'resolveStorageConnectors should reject a non-string entry'() {
        when:
        FovusJobConfig.resolveStorageConnectors(['fine-one', 42], null)

        then:
        def error = thrown(Error)
        error.message.contains('Invalid storage connector name')
    }

    def 'a job config JSON without storageConnectors should parse to an empty list'() {
        expect:
        FovusJobConfigBuilder.fromJsonString('{"objective": {"timeToCostPriorityRatio": "0.5/0.5"}}')
                .storageConnectors == []
    }

    def 'a job config JSON with a null storageConnectors should parse to an empty list'() {
        expect:
        FovusJobConfigBuilder.fromJsonString('{"storageConnectors": null}').storageConnectors == []
    }

    def 'a job config JSON with storageConnectors should parse them'() {
        expect:
        FovusJobConfigBuilder.fromJsonString('{"storageConnectors": ["team-shared", "archive-2024"]}')
                .storageConnectors == ['team-shared', 'archive-2024']
    }

    def 'the create-job payload should carry an empty storageConnectors list by default'() {
        expect:
        payload(new FovusJobConfig()).storageConnectors == []
    }

    def 'the create-job payload should carry the configured storageConnectors'() {
        given:
        def config = new FovusJobConfig()

        when:
        config.setStorageConnectors(['team-shared'])

        then:
        payload(config).storageConnectors == ['team-shared']
    }

    def 'setStorageConnectors should turn an explicit null into an empty list'() {
        given:
        def config = new FovusJobConfig()

        when:
        config.setStorageConnectors(null)

        then:
        config.storageConnectors == []
    }
}
