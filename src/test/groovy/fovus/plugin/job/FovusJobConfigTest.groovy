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

    private static final String DEFAULT_CONFIG_AUTO_ON =
            '{"constraints": {"jobConstraints": {"benchmarkingProfileName": "bp", "supportedCpuArchitectures": ["x86-64"], ' +
            '"hyperthreadingMode": "AUTO", "enableHyperthreading": true}, "taskConstraints": {"minvCpu": 1, "maxvCpu": 1}}}'

    private static JobConstraints defaults(String json) {
        return FovusJobConfigBuilder.fromJsonString(json).constraints.jobConstraints
    }

    private static JobConstraints defaultsWith(Map jobConstraints) {
        def all = [benchmarkingProfileName: 'bp', supportedCpuArchitectures: ['x86-64']] + jobConstraints
        return defaults(new groovy.json.JsonBuilder([constraints: [jobConstraints: all]]).toString())
    }

    private static Map jobConstraintsPayload(JobConstraints jobConstraints) {
        def config = new FovusJobConfig()
        config.setConstraints(new Constraints(jobConstraints: jobConstraints, taskConstraints: new TaskConstraints()))
        return payload(config).constraints['jobConstraints'] as Map
    }

    def 'ext.hyperthreadingMode should be written in uppercase whatever its case'() {
        when:
        def written = jobConstraintsPayload(FovusJobConfig.buildJobConstraints([hyperthreadingMode: value], defaults(DEFAULT_CONFIG_AUTO_ON)))

        then:
        written.hyperthreadingMode == mode
        written.enableHyperthreading == enabled
        written.containsKey('enableHyperthreading') == (enabled != null)

        where:
        value  | mode   | enabled
        'auto' | 'AUTO' | null
        'Auto' | 'AUTO' | null
        'ON'   | 'ON'   | true
        'on'   | 'ON'   | true
        'Off'  | 'OFF'  | false
        ' off ' | 'OFF' | false
    }

    def 'ext.enableHyperthreading should still be an explicit ON or OFF'() {
        when:
        def written = jobConstraintsPayload(FovusJobConfig.buildJobConstraints([enableHyperthreading: value], defaults(DEFAULT_CONFIG_AUTO_ON)))

        then:
        written.hyperthreadingMode == mode
        written.enableHyperthreading == value

        where:
        value | mode
        true  | 'ON'
        false | 'OFF'
    }

    def 'ext.hyperthreadingMode should win over ext.enableHyperthreading'() {
        when:
        def written = jobConstraintsPayload(FovusJobConfig.buildJobConstraints(
                [hyperthreadingMode: mode, enableHyperthreading: enabled], defaults(DEFAULT_CONFIG_AUTO_ON)))

        then:
        written.hyperthreadingMode == mode
        written.enableHyperthreading == expectedEnabled

        where:
        mode   | enabled | expectedEnabled
        'OFF'  | true    | false
        'ON'   | false   | true
        'AUTO' | false   | null
    }

    def 'with neither set, the get-default-config mode should be written and no hard-coded enableHyperthreading'() {
        when:
        def written = jobConstraintsPayload(FovusJobConfig.buildJobConstraints([:], defaults(DEFAULT_CONFIG_AUTO_ON)))

        then:
        written.hyperthreadingMode == 'AUTO'
        !written.containsKey('enableHyperthreading')
    }

    def 'with neither set, a workspace default mode saved as ON or OFF should be kept'() {
        when:
        def written = jobConstraintsPayload(FovusJobConfig.buildJobConstraints([:], defaultsWith(
                [hyperthreadingMode: mode, enableHyperthreading: enabled])))

        then:
        written.hyperthreadingMode == mode
        written.enableHyperthreading == enabled

        where:
        mode  | enabled
        'ON'  | true
        'OFF' | false
    }

    def 'a job config file without hyperthreading should write neither field'() {
        when:
        def written = jobConstraintsPayload(FovusJobConfig.buildJobConstraints([:], defaultsWith([:])))

        then:
        !written.containsKey('hyperthreadingMode')
        !written.containsKey('enableHyperthreading')
    }

    def 'a job config file with only enableHyperthreading should stay an explicit ON or OFF'() {
        when:
        def written = jobConstraintsPayload(FovusJobConfig.buildJobConstraints([:], defaultsWith([enableHyperthreading: enabled])))

        then:
        written.hyperthreadingMode == mode
        written.enableHyperthreading == enabled

        where:
        enabled | mode
        true    | 'ON'
        false   | 'OFF'
    }

    def 'a job config file hyperthreadingMode should be accepted in any case and win over its enableHyperthreading'() {
        when:
        def written = jobConstraintsPayload(FovusJobConfig.buildJobConstraints([:], defaultsWith(
                [hyperthreadingMode: 'off', enableHyperthreading: true])))

        then:
        written.hyperthreadingMode == 'OFF'
        written.enableHyperthreading == false
    }

    def 'an invalid hyperthreadingMode should fail fast and list the valid values'() {
        when:
        FovusJobConfig.buildJobConstraints([hyperthreadingMode: value], defaults(DEFAULT_CONFIG_AUTO_ON))

        then:
        def error = thrown(Error)
        error.message == "[Fovus] Invalid hyperthreadingMode: '${value}'. Use AUTO, ON or OFF."

        where:
        value << ['MAYBE', '', 'enabled', true, 1]
    }

    def 'an invalid hyperthreadingMode in a job config file should fail fast'() {
        when:
        FovusJobConfig.buildJobConstraints([:], defaultsWith([hyperthreadingMode: 'SOMETIMES']))

        then:
        def error = thrown(Error)
        error.message == "[Fovus] Invalid hyperthreadingMode: 'SOMETIMES'. Use AUTO, ON or OFF."
    }

    def 'a non-boolean ext.enableHyperthreading should fail fast'() {
        when:
        FovusJobConfig.buildJobConstraints([enableHyperthreading: 'false'], defaults(DEFAULT_CONFIG_AUTO_ON))

        then:
        def error = thrown(Error)
        error.message == "[Fovus] Invalid enableHyperthreading: 'false'. Use true or false, or set hyperthreadingMode."
    }

    def 'the resolved hyperthreading should follow the mode, and AUTO the get-default-config value'() {
        expect:
        FovusJobConfig.buildJobConstraints(ext, defaultsWith(defaultJobConstraints)).resolvedHyperthreading() == resolved

        where:
        ext                           | defaultJobConstraints                                     | resolved
        [:]                           | [hyperthreadingMode: 'AUTO', enableHyperthreading: true]  | true
        [:]                           | [hyperthreadingMode: 'AUTO', enableHyperthreading: false] | false
        [hyperthreadingMode: 'auto']  | [hyperthreadingMode: 'AUTO', enableHyperthreading: true]  | true
        [hyperthreadingMode: 'ON']    | [hyperthreadingMode: 'AUTO', enableHyperthreading: false] | true
        [hyperthreadingMode: 'OFF']   | [hyperthreadingMode: 'AUTO', enableHyperthreading: true]  | false
        [enableHyperthreading: true]  | [hyperthreadingMode: 'AUTO', enableHyperthreading: false] | true
        [hyperthreadingMode: 'AUTO']  | [hyperthreadingMode: 'OFF', enableHyperthreading: false]  | null
        [:]                           | [:]                                                       | null
    }

    def 'vCPU scaling should use the resolved hyperthreading of an AUTO task'() {
        given:
        def jobConstraints = FovusJobConfig.buildJobConstraints([:], defaultsWith(
                [hyperthreadingMode: 'AUTO', enableHyperthreading: resolved]))

        when:
        def taskConstraints = new TaskConstraints(minvCpu: 1, maxvCpu: 1)
                .overrideCpuConstraints(jobConstraints.resolvedHyperthreading())

        then:
        taskConstraints.minvCpu == minvCpu
        taskConstraints.maxvCpu == minvCpu

        where:
        resolved | minvCpu
        true     | 2
        false    | 1
    }

    def 'the resolved hyperthreading should not be written to the job config'() {
        when:
        def written = jobConstraintsPayload(FovusJobConfig.buildJobConstraints([:], defaults(DEFAULT_CONFIG_AUTO_ON)))

        then:
        written.keySet().findAll { it.toLowerCase().contains('hyperthreading') } == ['hyperthreadingMode'] as Set
    }
}
