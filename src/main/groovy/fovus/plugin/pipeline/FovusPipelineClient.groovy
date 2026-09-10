package fovus.plugin.pipeline

import groovy.json.JsonGenerator
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import fovus.plugin.CliExecutionResult
import fovus.plugin.FovusConfig
import fovus.plugin.FovusUtil
import nextflow.BuildInfo

/**
 * Client for executing Fovus CLI commands
 */
@CompileStatic
@Slf4j
class FovusPipelineClient {
    private FovusPipeline pipeline

    FovusPipelineClient() {}

    /**
     * Run a Fovus CLI command. Overridable so tests can assert on the arguments (and the
     * environment) this client builds without spawning the CLI.
     */
    protected CliExecutionResult runCli(List<String> command, Map<String, String> env) {
        return FovusUtil.executeCommand(command, env)
    }

    /**
     * Append the metadata describing the workflow manager driving this run, ie the Nextflow
     * release and the command line it was launched with. Both are omitted when unavailable.
     *
     * @param command The CLI command being built
     * @param runCommand The Nextflow command line, ie {@code session.commandLine}
     * @param config Used to redact the configured personal access token out of {@code runCommand}
     *  before it is sent to the Fovus backend as run metadata
     */
    private static void appendRunMetadata(List<String> command, String runCommand, FovusConfig config) {
        // Report the Nextflow release actually running the pipeline, not the one compiled against
        final workflowManagerVersion = BuildInfo.version
        if (workflowManagerVersion) {
            command << '--workflow-manager-version' << workflowManagerVersion
        }

        if (runCommand) {
            command << '--run-command' << config.redactSecret(runCommand)
        }
    }

    String createPipeline(FovusConfig config, String name) {
        return createPipeline(config, name, null)
    }

    String createPipeline(FovusConfig config, String name, String runCommand) {
        def command = [config.getCliPath(), '--silence', 'pipeline', 'create', '--name', name, '--workflow-host',
                       'local']
        appendRunMetadata(command, runCommand, config)

        def result = runCli(command, config.cliEnv())

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to create Fovus pipeline: ${config.redactSecret(result.error)}")
        }
        def slurper = new JsonSlurper()
        // Parse the string. JsonSlurper is often lenient with single quotes.
        log.debug "[FOVUS] Output: ${config.redactSecret(result.output.trim().split('\n')[-1])}"

        def dataObject = slurper.parseText((result.output.trim().split('\n')[-1]).replaceAll("'", '"')) as Map
        def pipelineId = dataObject.get("pipelineId") as String

        log.debug "[FOVUS] Pipeline created with ID: $pipelineId"

        this.pipeline = new FovusPipeline(name, pipelineId)

        return pipelineId
    }

    FovusPipeline getPipeline() {
        return this.pipeline
    }

    void updatePipelineStatus(FovusConfig config, FovusPipeline pipeline, FovusPipelineStatus status) {
        updatePipelineStatus(config, pipeline, status, null)
    }

    void updatePipelineStatus(FovusConfig config, FovusPipeline pipeline, FovusPipelineStatus status,
                              String runCommand) {
        log.trace "[FOVUS] Updating pipeline status to ${status.name()}"
        def command = [config.getCliPath(), '--silence', 'pipeline', 'update', '--pipeline-id', pipeline.getPipelineId(), '--status', status.name()]
        appendRunMetadata(command, runCommand, config)

        def result = runCli(command, config.cliEnv())
        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to update Fovus pipeline status: ${config.redactSecret(result.error)}")
        }
    }

    FovusPipeline getPipeline(FovusConfig config, String pipelineId) {
        def command = [config.getCliPath(), '--silence', 'pipeline', 'get', '--pipeline-id', pipelineId]
        def result = runCli(command, config.cliEnv())
        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to get Fovus pipeline: ${config.redactSecret(result.error)}")
        }

        final jsonData = new JsonSlurper().parseText(result.output)
        final pipeline = new FovusPipeline(
                jsonData["name"] as String,
                jsonData["pipelineId"] as String,
                jsonData["status"] as FovusPipelineStatus
        )

        return pipeline
    }

    void setPipeline(String pipelineName, String pipelineId) {
        this.pipeline = new FovusPipeline(pipelineName, pipelineId)
    }

    void preConfigResources(FovusConfig config, FovusPipeline pipeline, List<ResourceConfiguration> configurations) {
        preConfigResources(config, pipeline, configurations, null)
    }

    void preConfigResources(FovusConfig config, FovusPipeline pipeline, List<ResourceConfiguration> configurations,
                            String nextflowConfig) {
        def jsonGenerator = new JsonGenerator.Options().excludeNulls().build()
        def configurationsJson = jsonGenerator.toJson(configurations)

        def command = [
                config.getCliPath(),
                '--silence',
                'pipeline',
                'pre-config-resources',
                '--pipeline-id', pipeline.getPipelineId(),
                '--configurations', configurationsJson
        ]

        if (nextflowConfig) {
            command << '--pipeline-config' << nextflowConfig
        }

        def result = runCli(command, config.cliEnv())

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to configure Fovus pipeline resources: ${config.redactSecret(result.error)}")
        }
    }
}