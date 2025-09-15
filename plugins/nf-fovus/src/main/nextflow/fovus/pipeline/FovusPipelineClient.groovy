package nextflow.fovus.pipeline

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.util.logging.Slf4j
import nextflow.fovus.FovusConfig

/**
 * Client for executing Fovus CLI commands
 */
@CompileStatic
@Slf4j
class FovusPipelineClient {
    private FovusPipeline pipeline

    FovusPipelineClient() {}

    void createPipeline(FovusConfig config, String name) {
        def command = [config.getCliPath(), '--silence', 'pipeline', 'create', '--name', name]
        def result = executeCommand(command.join(' '))

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to create Fovus pipeline: ${result.error}")
        }
        def slurper = new JsonSlurper()
        // Parse the string. JsonSlurper is often lenient with single quotes.
        log.debug "[FOVUS] Output: ${result.output.trim().split('\n')[-1]}"

        def dataObject = slurper.parseText((result.output.trim().split('\n')[-1]).replaceAll("'", '"')) as Map

        // Access the pipelineId
//        def pipelineId = "p-1757530231825-minhlefovus"
        def pipelineId = "p-1757530271006-minhlefovus"

        log.debug "[FOVUS] Pipeline created with ID: $pipelineId"

        this.pipeline = new FovusPipeline(name, pipelineId)
    }

    FovusPipeline getPipeline() {
        return this.pipeline
    }

    @MapConstructor
    class CliExecutionResult {
        int exitCode
        String output
        String error
    }

    /**
     * Helper method to execute Fovus CLI commands
     * @param command
     * @return
     */
    private CliExecutionResult executeCommand(String command) {
        log.debug "[FOVUS] Executing command: ${command}"

        def stdout = new StringBuilder()
        def stderr = new StringBuilder()

        def process = command.execute()
        process.consumeProcessOutput(stdout, stderr)
        process.waitFor()

        log.debug "[FOVUS] Command executed with exit code: ${process.exitValue()}"
        log.debug "[FOVUS] Command output: ${stdout}"
        log.debug "[FOVUS] Command error: ${stderr}"

        return new CliExecutionResult(exitCode: process.exitValue(), output: stdout.toString(), error: stderr.toString())
    }
}

