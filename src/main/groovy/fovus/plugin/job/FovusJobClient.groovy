package fovus.plugin.job

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import fovus.plugin.FovusConfig
import fovus.plugin.FovusUtil
import fovus.plugin.nio.FovusFileMetadata

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

        if (isArrayJob) {
            command << "--exclude-paths"
            command << ".*"
        }

        if (config.projectName != null && config.projectName != "") {
            command << "--project-name"
            command << config.projectName
        }

        def result = FovusUtil.executeCommand(command)


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
        def result = FovusUtil.executeCommand(command)

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
        def result = FovusUtil.executeCommand(downloadJobCommand)

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to download Fovus job outputs: ${result.error}")
        }
    }

    public void terminateJob(String jobId) {
        def command = [config.getCliPath(), 'job', 'terminate', '--job-id', jobId]
        def result = FovusUtil.executeCommand(command)

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
        final parts = fovusPath.split("/")
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
        final parts = fovusPath.split("/")
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


    /**
     * List the files in the provided path.
     *
     * @param fileType jobs or files
     * @param path The path to list files from that is relative to files/ or jobs/.
     * @return List of FovusFileMetadata objects
     */
    List<FovusFileMetadata> listFileObjects(String fileType, String path) {
        def command = [config.getCliPath(), '--silence', 'job', 'list-objects']

        if (fileType == "jobs") {
            def parts = path.tokenize('/')
            if (parts.isEmpty()) {
                throw new RuntimeException("Invalid Fovus path: ${path}")
            }
            final jobId = parts[0]
            final jobRelativePath = parts.size() > 1 ? parts[1..-1].join('/') : ""
            command << jobRelativePath
            command << '--job-id'
            command << jobId.toString()
        } else {
            command << path
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
        return null
    }


    FovusFileMetadata getFileObject(String fileType, String path) {
        List<FovusFileMetadata> metaDataList = listFileObjects(fileType, path)

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