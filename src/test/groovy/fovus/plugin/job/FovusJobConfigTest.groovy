package fovus.plugin.job

import groovy.json.JsonGenerator
import groovy.json.JsonSlurper
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class FovusJobConfigTest extends Specification {

    @TempDir
    Path workDir

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

    def 'a job config JSON entry that is not a string should be rejected rather than coerced'() {
        when: 'Jackson would otherwise bind the scalar into a List<String> as its string form'
        FovusJobConfigBuilder.fromJsonString("""{"storageConnectors": ["fine-one", ${offending}]}""")

        then:
        def error = thrown(Error)
        error.message.contains('Invalid storage connector name')

        where:
        offending << ['42', 'true', '12.5']
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

    def 'resolveAllowedRegionList should let the process list win and keep its order'() {
        expect:
        FovusJobConfig.resolveAllowedRegionList(['us-west-2', 'us-east-1'], ['eu-west-1'] as List<String>) ==
                ['us-west-2', 'us-east-1']
    }

    def 'resolveAllowedRegionList should fall back to the default when the process list is null or empty'() {
        expect:
        FovusJobConfig.resolveAllowedRegionList(extensionValue, defaultList) == defaultList

        where:
        extensionValue | defaultList
        null           | ['eu-west-1'] as List<String>
        []             | ['eu-west-1'] as List<String>
        null           | null
        []             | null
    }

    def 'resolveAllowedRegionList should turn GString entries into strings'() {
        given:
        def region = 'us-east-1'

        when:
        def resolved = FovusJobConfig.resolveAllowedRegionList(["${region}"], null)

        then:
        resolved == ['us-east-1']
        resolved[0] instanceof String
    }

    def 'resolveAllowedRegionList should throw naming the attribute for #description'() {
        when:
        FovusJobConfig.resolveAllowedRegionList(extensionValue, null)

        then:
        def error = thrown(Error)
        error.message.startsWith('[Fovus]')
        error.message.contains('ext.allowedRegionList')

        where:
        description              | extensionValue
        'a string value'         | 'us-east-1'
        'a list holding a number' | ['us-east-1', 5]
    }

    def 'the payload should carry allowedRegionList under jobConstraints'() {
        given:
        def config = new FovusJobConfig()
        config.constraints = new Constraints(jobConstraints: new JobConstraints(allowedRegionList: ['us-east-1', 'us-west-2']))

        expect:
        payload(config).constraints.jobConstraints.allowedRegionList == ['us-east-1', 'us-west-2']
    }

    def 'the payload should leave out allowedRegionList when it is null'() {
        given:
        def config = new FovusJobConfig()
        config.constraints = new Constraints(jobConstraints: new JobConstraints())

        expect:
        !payload(config).constraints.jobConstraints.containsKey('allowedRegionList')
    }

    def 'a job config JSON string should bind allowedRegionList'() {
        when:
        def config = FovusJobConfigBuilder.fromJsonString(
                '{"constraints":{"jobConstraints":{"allowedRegionList":["us-east-1","us-west-2"]}}}')

        then:
        config.constraints.jobConstraints.allowedRegionList == ['us-east-1', 'us-west-2']
    }

    def 'a closure ext.allowedRegionList should resolve into the job constraints of a real task'() {
        given:
        final jobConfigFile = workDir.resolve('job_config.json')
        jobConfigFile.text = '''{
            "environment": {"containerized": {"container": "Docker", "version": "20.10.18", "imagePath": "ubuntu"}},
            "constraints": {"jobConstraints": {"supportedCpuArchitectures": ["x86-64"], "allowedRegionList": ["eu-west-1"]}, "taskConstraints": {}},
            "objective": {},
            "workload": {}
        }'''
        final regions = ['us-east-1', 'us-west-2']
        final task = new TaskRun(name: 'align (1)', workDir: workDir, processor: Stub(TaskProcessor),
                config: new TaskConfig([ext: [jobConfigFile: jobConfigFile.toString(), allowedRegionList: { regions }]]))

        when:
        final config = new FovusJobConfig(null, task)

        then:
        payload(config).constraints.jobConstraints.allowedRegionList == regions
    }
}
