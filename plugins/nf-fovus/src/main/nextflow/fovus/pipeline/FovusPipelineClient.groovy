package nextflow.fovus.pipeline

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
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

    void setPipeline(String pipelineName, String pipelineId) {
        this.pipeline = new FovusPipeline(pipelineName, pipelineId)
    }

}

