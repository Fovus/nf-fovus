package nextflow.fovus.job

import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.util.logging.Slf4j
import nextflow.fovus.FovusConfig


/**
 * Client for executing Fovus CLI commands
 */
@CompileStatic
@Slf4j
class FovusJobClient {
    private FovusConfig config
    private FovusJobConfig jobConfig

    FovusJobClient(FovusConfig config, FovusJobConfig jobConfig) {
        this.config = config
        this.jobConfig = jobConfig
    }

    void createJob() {
        def command = [config.getCliPath(), 'job', 'create']


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
        log.trace "[FOVUS] Executing command: ${command}"

        def process = command.execute()
        def exitCode = process.waitFor()

        return new CliExecutionResult (exitCode: exitCode, output: process.text, error: process.err.text)
    }
}
