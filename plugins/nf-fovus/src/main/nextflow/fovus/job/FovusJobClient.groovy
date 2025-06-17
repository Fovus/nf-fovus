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

    String createJob(String jobConfigFilePath, String jobDirectory, String jobName = null) {
        def command = [config.getCliPath(), 'job', 'create', jobConfigFilePath, jobDirectory]
        if (jobName) {
            command << "--job-name"
            command << jobName
        }

        def result = executeCommand(command.join(' '))


        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to create Fovus job: ${result.error}")
        }

        // Get the Job ID (the last line of the output)
        def jobId = result.output.trim().split('\n')[-1]
        log.trace "[FOVUS] Job created with ID: ${jobId}"

        return jobId
    }

    FovusJobStatus getJobStatus(String jobId) {
        def command = [config.getCliPath(), 'job', 'status', '--job-id', jobId]
        def result = executeCommand(command.join(' '))

        def jobStatus = result.output.trim().split('\n')[-1]
        log.trace "[FOVUS] Job Id: ${jobId}, status: ${jobStatus}"

        switch (jobStatus) {
            case 'Created':
                return FovusJobStatus.CREATED
            case 'Completed':
                return FovusJobStatus.COMPLETED
            case 'Failed':
                return FovusJobStatus.FAILED
            case 'Pending':
                return FovusJobStatus.PENDING
            case 'Running':
                return FovusJobStatus.RUNNING
            case 'Requeued':
                return FovusJobStatus.REQUEUED
            case 'Terminated':
                return FovusJobStatus.TERMINATED
            case 'Terminating':
                return FovusJobStatus.TERMINATING
            case 'Uncompleted':
                return FovusJobStatus.UNCOMPLETED
            case 'Walltime Reached':
                return FovusJobStatus.WALLTIME_REACHED
            case 'Provisioning Infrastructure':
                return FovusJobStatus.PROVISIONING_INFRASTRUCTURE
            case 'Cloud Strategy Optimization':
                return FovusJobStatus.CLOUD_STRATEGY_OPTIMIZATION
            default:
                log.error "[FOVUS] Unknown job status: ${jobStatus}"
                throw new RuntimeException("Unknown job status: ${jobStatus}")
        }
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

        def stdout = new StringBuilder()
        def stderr = new StringBuilder()

        def process = command.execute()
        process.consumeProcessOutput(stdout, stderr)
        process.waitFor()

        log.trace "[FOVUS] Command executed with exit code: ${process.exitValue()}"
        log.trace "[FOVUS] Command output: ${stdout}"
        log.trace "[FOVUS] Command error: ${stderr}"

        return new CliExecutionResult(exitCode: process.exitValue(), output: stdout.toString(), error: stderr.toString())
    }

}

enum FovusJobStatus {
    CREATED,
    COMPLETED,
    PENDING,
    FAILED,
    REQUEUED,
    RUNNING,
    TERMINATED,
    TERMINATING,
    UNCOMPLETED,
    WALLTIME_REACHED,
    PROVISIONING_INFRASTRUCTURE,
    CLOUD_STRATEGY_OPTIMIZATION
}
