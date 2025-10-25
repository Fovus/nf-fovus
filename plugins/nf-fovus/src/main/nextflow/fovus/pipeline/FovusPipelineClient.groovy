package nextflow.fovus.pipeline

import groovy.json.JsonGenerator
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.fovus.FovusConfig
import nextflow.fovus.FovusUtil

/**
 * Client for executing Fovus CLI commands
 */
@CompileStatic
@Slf4j
class FovusPipelineClient {
    private FovusPipeline pipeline

    FovusPipelineClient() {}

    String createPipeline(FovusConfig config, String name) {
        def command = [config.getCliPath(), '--silence', 'pipeline', 'create', '--name', name]
        def result = FovusUtil.executeCommand(command)

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to create Fovus pipeline: ${result.error}")
        }
        def slurper = new JsonSlurper()
        // Parse the string. JsonSlurper is often lenient with single quotes.
        log.debug "[FOVUS] Output: ${result.output.trim().split('\n')[-1]}"

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
        log.trace "[FOVUS] Updating pipeline status to ${status.name()}"
        def command = [config.getCliPath(), '--silence', 'pipeline', 'update', '--pipeline-id', pipeline.getPipelineId(), '--status', status.name()]
        def result = FovusUtil.executeCommand(command)
        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to update Fovus pipeline status: ${result.error}")
        }
    }

    FovusPipeline getPipeline(FovusConfig config, String pipelineId) {
        def command = [config.getCliPath(), '--silence', 'pipeline', 'get', '--pipeline-id', pipelineId]
        def result = FovusUtil.executeCommand(command)
        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to get Fovus pipeline: ${result.error}")
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
        def jsonGenerator = new JsonGenerator.Options().excludeNulls().build()
        def configurationsJson = jsonGenerator.toJson(configurations)

        def command = [config.getCliPath(), '--silence', 'pipeline', 'pre-config-resources', '--pipeline-id', pipeline.getPipelineId(), '--configurations', "'${configurationsJson}'"]

        def result = FovusUtil.executeCommand(command)

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to configure Fovus pipeline resources: ${result.error}")
        }
    }
}