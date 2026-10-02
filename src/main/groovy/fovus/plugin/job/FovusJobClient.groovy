package fovus.plugin.job

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import fovus.plugin.FovusConfig
import fovus.plugin.FovusUtil
import java.util.concurrent.ConcurrentHashMap

/**
 * Client for executing Fovus CLI commands
 */
@CompileStatic
@Slf4j
class FovusJobClient {
    private FovusConfig config
    private FovusJobConfig jobConfig
    private static final Map<String, Map> jobConfigCache = new ConcurrentHashMap<>()
    private static final long TTL_MS = 10 * 60 * 1000

    FovusJobClient(FovusConfig config, FovusJobConfig jobConfig) {
        this.config = config
        this.jobConfig = jobConfig
    }

    FovusJobClient(FovusConfig config) {
        this.config = config
    }

    void setJobConfig(FovusJobConfig jobConfig) {
        this.jobConfig = jobConfig
    }

    String createJob(String jobConfigFilePath, String jobDirectory, String pipelineId, List<String> includeList, String jobName = null, isArrayJob = false) {
        def command = [config.getCliPath(), '--silence', '--nextflow', 'job', 'create', jobConfigFilePath, jobDirectory]

        if(pipelineId){
            command << "--pipeline-id"
            command << pipelineId
        }

        if (jobName) {
            command << "--job-name"
            command << jobName
        }

        if (includeList.size() > 0) {
            command << "--include-paths"
            command << includeList.join(",")
        }

        if (config.projectName != null && config.projectName != "") {
            command << "--project-name"
            command << config.projectName
        }

        def result = FovusUtil.executeCommand(command, config.cliEnv())


        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to create Fovus job: " +
                    "${config.redactSecret(result.error)}")
        }

        // Get the Job ID (the last line of the output)
        def jobId = result.output.trim().split('\n')[-1]
        log.trace"[FOVUS] Job created with ID: ${jobId}"

        return jobId
    }

    FovusJobStatus getJobStatus(String jobId) {
        def command = [config.getCliPath(), 'job', 'status', '--job-id', jobId]
        def result = FovusUtil.executeCommand(command, config.cliEnv())

        def jobStatus = result.output.trim().split('\n')[-1]
        log.trace"[FOVUS] Job Id: ${jobId}, status: ${jobStatus}"

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
            case 'Walltime Reached':
                return FovusJobStatus.WALLTIME_REACHED
            case 'Provisioning Infrastructure':
                return FovusJobStatus.PROVISIONING_INFRASTRUCTURE
            case 'Cloud Strategy Optimization':
                return FovusJobStatus.CLOUD_STRATEGY_OPTIMIZATION
            case 'Waiting':
                return FovusJobStatus.WAITING
            default:
                log.error "[FOVUS] Unknown job status: ${jobStatus}"
                throw new RuntimeException("Unknown job status: ${jobStatus}")
        }
    }

    public void downloadJobOutputs(String jobDirectoryPath, String jobId) {
        def downloadJobCommand = [config.getCliPath(), 'job', 'download', jobDirectoryPath, '--job-id', jobId]

        log.trace"[FOVUS] Download job outputs"
        def result = FovusUtil.executeCommand(downloadJobCommand, config.cliEnv())

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to download Fovus job outputs: " +
                    "${config.redactSecret(result.error)}")
        }
    }

    public void terminateJob(String jobId) {
        def command = [config.getCliPath(), 'job', 'terminate', '--job-id', jobId]
        def result = FovusUtil.executeCommand(command, config.cliEnv())

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to terminate Fovus job: " +
                    "${config.redactSecret(result.error)}")
        }
    }

    String getDefaultJobConfig(String benchmarkingProfileName) {
        long currentTime = System.currentTimeMillis()

        // 1. Check if the result exists and is still valid (within TTL)
        if (jobConfigCache.containsKey(benchmarkingProfileName)) {
            // Explicitly define the map to help the static type checker
            Map entry = (Map) jobConfigCache.get(benchmarkingProfileName)

            // Cast timestamp to long specifically
            long entryTime = (long) entry.timestamp

            if ((currentTime - entryTime) < TTL_MS) {
                log.trace "[FOVUS] Returning cached config for: ${benchmarkingProfileName}"
                return (String) entry.output
            } else {
                log.trace "[FOVUS] Cache expired for: ${benchmarkingProfileName}. Re-fetching..."
                jobConfigCache.remove(benchmarkingProfileName)
            }
        }

        // 2. Execute the actual command if no valid cache exists
        def command = [config.getCliPath(), 'job', 'get-default-config', '--benchmarking-profile-name', "${benchmarkingProfileName}"]
        def result = FovusUtil.executeCommand(command, config.cliEnv())

        log.trace "[FOVUS] getDefaultJobConfig with exit code: ${result.exitCode}"
        if (result.exitCode != 0) {
            log.trace "[FOVUS] Command error: ${config.redactSecret(result.error)}"
            return null
        }

        // 3. Save to cache before returning
        jobConfigCache.put(benchmarkingProfileName, [
                output: result.output,
                timestamp: currentTime
        ])

        return result.output
    }

    String getDefaultJobConfig() {
        getDefaultJobConfig("Default")
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
    WALLTIME_REACHED,
    PROVISIONING_INFRASTRUCTURE,
    CLOUD_STRATEGY_OPTIMIZATION,
    WAITING,
    TERMINATED_INFRA,
    TERMINATE_FAILED,
    TIMEOUT,
    SCHEDULED,
    POST_PROCESSING_RUNNING,
    POST_PROCESSING_FAILED,
    POST_PROCESSING_WALLTIME_REACHED
}