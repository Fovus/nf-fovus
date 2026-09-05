package fovus.plugin.pipeline

import fovus.plugin.CliExecutionResult
import fovus.plugin.FovusAuthConfig
import fovus.plugin.FovusConfig
import nextflow.BuildInfo
import spock.lang.Specification

class FovusPipelineClientTest extends Specification {

    private static final FovusConfig TEST_CONFIG = new FovusConfig([pipelineName: 'test-pipeline', cliPath: 'fovus'])
    private static final FovusConfig AUTH_CONFIG = new FovusConfig(
            [pipelineName: 'test-pipeline', cliPath: 'fovus'],
            new FovusAuthConfig([email: 'automation@corp.com', personalAccessToken: 'pat-secret-value'],
                                 'secrets.FOVUS_EMAIL', 'secrets.FOVUS_PAT'))
    private static final FovusPipeline TEST_PIPELINE = new FovusPipeline('test-pipeline', 'p-123')
    private static final String RUN_COMMAND = 'nextflow run hello -plugins nf-fovus'

    /** Captures the argv and env the client builds instead of spawning the CLI. */
    private static class CapturingClient extends FovusPipelineClient {
        List<String> captured
        Map<String, String> capturedEnv
        String output = "{'pipelineId': 'p-123'}"

        @Override
        protected CliExecutionResult runCli(List<String> command, Map<String, String> env) {
            captured = command
            capturedEnv = env
            return new CliExecutionResult(exitCode: 0, output: output, error: '')
        }
    }

    /** The value of a flag, ie the element following it. */
    private static String flagValue(List<String> command, String flag) {
        final i = command.indexOf(flag)
        return i >= 0 ? command[i + 1] : null
    }

    def 'createPipeline should send the workflow manager version and the run command'() {
        given:
        def client = new CapturingClient()

        when:
        def pipelineId = client.createPipeline(TEST_CONFIG, 'test-pipeline', RUN_COMMAND)

        then:
        pipelineId == 'p-123'

        and: 'the existing arguments are unchanged'
        client.captured[0..7] == ['fovus', '--silence', 'pipeline', 'create', '--name', 'test-pipeline',
                                  '--workflow-host', 'local']

        and:
        flagValue(client.captured, '--workflow-manager-version') == BuildInfo.version
        flagValue(client.captured, '--run-command') == RUN_COMMAND
    }

    def 'createPipeline should omit the run command when there is none'() {
        given:
        def client = new CapturingClient()

        when:
        client.createPipeline(TEST_CONFIG, 'test-pipeline')

        then:
        !client.captured.contains('--run-command')
        flagValue(client.captured, '--workflow-manager-version') == BuildInfo.version
    }

    def 'updatePipelineStatus should send the status, workflow manager version and run command'() {
        given:
        def client = new CapturingClient()

        when:
        client.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.RUNNING, RUN_COMMAND)

        then:
        client.captured[0..7] == ['fovus', '--silence', 'pipeline', 'update', '--pipeline-id', 'p-123',
                                  '--status', 'RUNNING']

        and:
        flagValue(client.captured, '--workflow-manager-version') == BuildInfo.version
        flagValue(client.captured, '--run-command') == RUN_COMMAND
    }

    def 'updatePipelineStatus should omit the run command when there is none'() {
        given:
        def client = new CapturingClient()

        when:
        client.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.COMPLETED)

        then:
        !client.captured.contains('--run-command')
        flagValue(client.captured, '--status') == 'COMPLETED'
        flagValue(client.captured, '--workflow-manager-version') == BuildInfo.version
    }

    def 'preConfigResources should send the Nextflow config content'() {
        given:
        def client = new CapturingClient()
        def nextflowConfig = "process {\n    ext.maxvCpu = 8\n}\n"

        when:
        client.preConfigResources(TEST_CONFIG, TEST_PIPELINE, [], nextflowConfig)

        then:
        client.captured[0..5] == ['fovus', '--silence', 'pipeline', 'pre-config-resources', '--pipeline-id', 'p-123']

        and: 'the config is passed verbatim as a single argument, needing no escaping'
        flagValue(client.captured, '--pipeline-config') == nextflowConfig
        flagValue(client.captured, '--configurations') == '[]'
    }

    def 'preConfigResources should omit the config flag when there is no config'() {
        given:
        def client = new CapturingClient()

        when:
        client.preConfigResources(TEST_CONFIG, TEST_PIPELINE, [])

        then:
        !client.captured.contains('--pipeline-config')
    }

    def 'preConfigResources should serialise the resource configurations'() {
        given:
        def client = new CapturingClient()
        def configuration = new ResourceConfiguration(benchmarkingProfileName: 'profile', maxvCpu: 8)

        when:
        client.preConfigResources(TEST_CONFIG, TEST_PIPELINE, [configuration])

        then: 'null fields are excluded'
        flagValue(client.captured, '--configurations') == '[{"benchmarkingProfileName":"profile","maxvCpu":8}]'
    }

    def 'preConfigResources should serialise the storage connectors'() {
        given:
        def client = new CapturingClient()
        def configuration = new ResourceConfiguration(benchmarkingProfileName: 'profile',
                                                      storageConnectors: ['reference-genomes'])

        when:
        client.preConfigResources(TEST_CONFIG, TEST_PIPELINE, [configuration])

        then:
        flagValue(client.captured, '--configurations') ==
                '[{"benchmarkingProfileName":"profile","storageConnectors":["reference-genomes"]}]'
    }

    def 'a failing CLI call should raise an error'() {
        given:
        def client = new CapturingClient() {
            @Override
            protected CliExecutionResult runCli(List<String> command, Map<String, String> env) {
                return new CliExecutionResult(exitCode: 1, output: '', error: 'boom')
            }
        }

        when:
        client.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.RUNNING, RUN_COMMAND)

        then:
        def e = thrown(RuntimeException)
        e.message.contains('boom')
    }

    def 'a failing CLI call should redact the configured token from the error message'() {
        given:
        def client = new CapturingClient() {
            @Override
            protected CliExecutionResult runCli(List<String> command, Map<String, String> env) {
                return new CliExecutionResult(exitCode: 1, output: '', error: 'boom: pat-secret-value leaked')
            }
        }

        when:
        client.updatePipelineStatus(AUTH_CONFIG, TEST_PIPELINE, FovusPipelineStatus.RUNNING, RUN_COMMAND)

        then:
        def e = thrown(RuntimeException)
        !e.message.contains('pat-secret-value')
        e.message.contains('[REDACTED]')
    }

    def 'createPipeline should carry no credentials when fovus.auth is not configured'() {
        given:
        def client = new CapturingClient()

        when:
        client.createPipeline(TEST_CONFIG, 'test-pipeline')

        then:
        client.capturedEnv.isEmpty()
    }

    def 'createPipeline should carry the configured credentials as environment variables'() {
        given:
        def client = new CapturingClient()

        when:
        client.createPipeline(AUTH_CONFIG, 'test-pipeline')

        then:
        client.capturedEnv == [FOVUS_EMAIL: 'automation@corp.com', FOVUS_PAT: 'pat-secret-value']

        and: 'the token never appears on the command line'
        !client.captured.contains('pat-secret-value')
    }

    def 'createPipeline should redact the configured token from the run command before sending it'() {
        given:
        def client = new CapturingClient()

        when:
        client.createPipeline(AUTH_CONFIG, 'test-pipeline', 'nextflow run hello --token pat-secret-value')

        then:
        !flagValue(client.captured, '--run-command').contains('pat-secret-value')
        flagValue(client.captured, '--run-command').contains('[REDACTED]')
    }
}
