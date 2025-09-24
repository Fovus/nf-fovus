package nextflow.fovus.job

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.util.logging.Slf4j
import nextflow.fovus.FovusConfig
import nextflow.fovus.FovusUtil
import nextflow.fovus.nio.FovusFileMetadata

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

        def result = executeCommand(command.join(' '))


        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to create Fovus job: ${result.error}")
        }

        // Get the Job ID (the last line of the output)
        def jobId = result.output.trim().split('\n')[-1]
        log.trace"[FOVUS] Job created with ID: ${jobId}"

        return jobId
    }

    FovusJobStatus getJobStatus(String jobId) {
        def command = [config.getCliPath(), 'job', 'status', '--job-id', jobId]
        def result = executeCommand(command.join(' '))

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

    def getStatusFromJsonOutput(fullOutputString) {
        // 1. Extract the JSON part from the string
        // This regex looks for a '[' followed by anything, ending with a ']'
        def matcher = (fullOutputString =~ /(?s)\[.*\]/)
        if (matcher.find()) {
            def jsonPart = matcher.group(0) // Get the full matched JSON string

            // 2. Parse the JSON part
            def slurper = new JsonSlurper()
            def parsedData =  (List<Map<String, Object>>)  slurper.parseText(jsonPart)

            // 3. Access the status from the first element of the array
            // Use null-safe operator and Elvis operator for robustness
            return parsedData[0]?.get("status") as String ?: "Status Not Found"
        } else {
            // Handle case where no JSON part is found
            return "No JSON found in output"
        }
    }

    FovusRunStatus getRunStatus(String jobId, String runName) {
        def command = [config.getCliPath(), 'job', 'list-runs', '--job-id', jobId, '--run-names', runName]
        try {
            def result = executeCommand(command.join(' '))
            def runStatus = getStatusFromJsonOutput(result.output)

            // If status not found immmidiately after submission then consider as CREATED
            if(runStatus == "Status Not Found" && FovusUtil.isRecentlySubmitted(jobId)){
                runStatus = FovusRunStatus.CREATED
            }

            log.trace"[FOVUS] Job Id: ${jobId}, status: ${runStatus}"

            switch (runStatus) {
                case 'Pending':
                    return FovusRunStatus.CREATED
                case 'Completed':
                    return FovusRunStatus.COMPLETED
                case 'Failed':
                    return FovusRunStatus.FAILED
                case 'Running':
                    return FovusRunStatus.RUNNING
                case 'Requeued':
                    return FovusRunStatus.REQUEUED
                case 'Terminated':
                    return FovusRunStatus.TERMINATED
                case 'Terminating':
                    return FovusRunStatus.TERMINATING
                case 'Uncompleted':
                    return FovusRunStatus.UNCOMPLETE
                case 'Walltime Reached':
                    return FovusRunStatus.WALLTIME_REACHED
                default:
                    log.error "[FOVUS] Unknown job status: ${runStatus}"
                    throw new RuntimeException("Unknown job status: ${runStatus}")
            }
        } catch(Exception e) {
            log.error("getRunStatus error, ex=${e.message}");
            throw new RuntimeException("getRunStatusError")
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
        int maxRetries = 3
        int attempt = 0
        CliExecutionResult result = null

        while (attempt < maxRetries) {
            attempt++
            log.trace"[FOVUS] Executing command (attempt ${attempt}/${maxRetries}): ${command}"

            def stdout = new StringBuilder()
            def stderr = new StringBuilder()

            def process = command.execute()
            process.consumeProcessOutput(stdout, stderr)
            process.waitFor()

            result = new CliExecutionResult(
                    exitCode: process.exitValue(),
                    output: stdout.toString(),
                    error: stderr.toString()
            )

            log.trace"[FOVUS] Command executed with exit code: ${result.exitCode}"
            log.trace"[FOVUS] Command output: ${result.output}"
            log.trace"[FOVUS] Command error: ${result.error}"

            if (result.exitCode == 0) {
                // Success, break out of retry loop
                break
            } else {
                log.warn "[FOVUS] Command failed on attempt ${attempt} with exit code ${result.exitCode}"
                if (attempt < maxRetries) {
                    log.info "[FOVUS] Retrying command in 2s..."
                    sleep(2000)  // small backoff before retry
                }
            }
        }

        return result
    }

    public void downloadJobOutputs(String jobDirectoryPath, String jobId) {
        def downloadJobCommand = [config.getCliPath(), 'job', 'download', jobDirectoryPath, '--job-id', jobId]

        log.trace"[FOVUS] Download job outputs"
        def result = executeCommand(downloadJobCommand.join(' '))

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to download Fovus job outputs: ${result.error}")
        }
    }

    public void terminateJob(String jobId) {
        def command = [config.getCliPath(), 'job', 'terminate', '--job-id', jobId]
        def result = executeCommand(command.join(' '))

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to terminate Fovus job: ${result.error}")
        }
    }

    String getDefaultJobConfig(String benchmarkingProfileName) {
        def command = [config.getCliPath(), 'job', 'get-default-config', '--benchmarking-profile-name', "${benchmarkingProfileName}"]

        def result = FovusUtil.executeCommand(command)

        log.trace "[FOVUS] getDefaultJobConfig with exit code: ${result.exitCode}"
        if (result.exitCode != 0) {
            log.trace "[FOVUS] Command error: ${result.error}"
            return null
        }

        return result.output
    }

    String getDefaultJobConfig() {
        getDefaultJobConfig("Default")
    }
    
    /**
     * Download a file from Fovus Storage (either jobs of files type) to a local directory.
     * @param jobId The job ID to download from (if any)
     * @param fovusPath The Fovus Storage path (relative to jobs/ or files/).
     *  The component after the last / will be used as value for the --include-paths option. If this is a directory, need to add / at the end.
     * @param localPath The local directory to download to
     */
    void downloadFile(String fovusPath, String localPath, String fileType) {
        log.debug "[FOVUS] Downloading file: ${fovusPath} to ${localPath}"
        def command
        if (fileType == "jobs") {
            command = getJobFileDownloadCommand(fovusPath, localPath)
        } else {
            command = getStorageFileDownloadCommand(fovusPath, localPath)
        }
        def result = FovusUtil.executeCommand(command)

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to upload file: ${result.error}")
        }
    }

    private final List<String> getJobFileDownloadCommand(String fovusPath, String localPath) {
        final parts = fovusPath.split("/");
        final String jobId = parts[0]
        final includePath = parts[1..-1].join('/')

        def command = [config.getCliPath(), '--silence', 'job', 'download', localPath, '--job-id', jobId]

        if (!includePath.isEmpty()) {
            command << '--include-paths'
            command << includePath
        }

        return command
    }

    private final List<String> getStorageFileDownloadCommand(String fovusPath, String localPath) {
        final parts = fovusPath.split("/");
        def fovusPathDir
        def includePath
        if (fovusPath.endsWith("/")) {
            fovusPathDir = parts[0..-1].join('/')
            includePath = "*"
        } else {
            fovusPathDir = parts.size() > 1 ? parts[0..-2].join('/') : ""
            includePath = parts[-1]
        }
        def command = [config.getCliPath(), '--silence', 'storage', 'download', fovusPathDir, localPath, '--include-paths', includePath]
        return command
    }


    // TODO: Validate the logic
    List<FovusFileMetadata> listFileObjects(String path, String jobId) {
        def command = [config.getCliPath(), '--silence', 'job', 'list-objects']
        if (jobId) {
            def parts = path.tokenize('/')
            def afterTwo = parts.size() > 2 ? parts[2..-1].join('/') : path
            command << afterTwo
            command << '--job-id'
            command << jobId.toString()
        } else {
            // Remove the files/ or jobs/ prefix
            def pathWithoutPrefix = path.startsWith("files/") ? path.substring(6) : path.startsWith("jobs/") ? path.substring(5) : path

            command << pathWithoutPrefix
        }

        def result = FovusUtil.executeCommand(command)

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to upload file: ${result.error}")
        }


        try {
            def output = result.output.toString()

            def jsonText = output.readLines().drop(2).join('\n')

            def json = new JsonSlurper().parseText(jsonText)
            if (!(json instanceof List)) {
                throw new RuntimeException("No objects found for path: ${path}")
            }


            List<Map> jsonList = (List<Map>) json
            List<FovusFileMetadata> metaDataList = []

            for (Map obj : jsonList) {
                def lastModifiedStr = obj['LastModified'] as String
                def dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
                Date lastModifiedDate = dateFormat.parse(lastModifiedStr)

                def objMetadata = new FovusFileMetadata(
                        obj['Key'] as String,
                        lastModifiedDate,
                        obj['ETag'] as String,
                        (obj['Size'] as Number).longValue(),
                )
                metaDataList.add(objMetadata)

            }

            return metaDataList
        } catch (Exception e) {
            log.error "[FOVUS] Error listing file objects: ${e.message}"
        }
        return null;
    }


    FovusFileMetadata getFileObject(String path, String jobId) {
        List<FovusFileMetadata> metaDataList = listFileObjects(path, jobId)

        if (metaDataList == null || metaDataList.size() == 0) {
            return null
        }

        return metaDataList[0]
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

enum FovusRunStatus {
    CREATED,
    COMPLETED,
    FAILED,
    REQUEUED,
    RUNNING,
    TERMINATED,
    TERMINATING,
    UNCOMPLETE,
    WALLTIME_REACHED
}
