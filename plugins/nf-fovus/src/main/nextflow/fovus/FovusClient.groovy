package nextflow.fovus

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.util.logging.Slf4j
import nextflow.fovus.job.FovusJobConfig
import nextflow.fovus.nio.ObjectMetaData

import java.text.SimpleDateFormat


/**
 * Client for executing Fovus CLI commands
 */
@CompileStatic
@Slf4j
class FovusClient {
    private FovusConfig config
    private FovusJobConfig jobConfig

    FovusClient(FovusConfig config, FovusJobConfig jobConfig) {
        this.config = config
        this.jobConfig = jobConfig
    }

    FovusClient(){
        this.config = new FovusConfig();
    }

    String generateJobId(){
        def command = [config.getCliPath(), '--silence', 'job', 'generate-id']
        def result = executeCommand(command.join(' '))

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to generate Fovus job id: ${result.error}")
        }

        // Get the Job ID (the last line of the output)
        def jobId = result.output.trim().split('\n')[-1]
        log.debug "[FOVUS] Generated job ID: ${jobId}"

        return jobId
    }

    String createJob(String jobConfigFilePath, String jobDirectory, String pipelineId, List<String> includeList, String jobId, String jobName = null, isArrayJob = false) {
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

        if (includeList.size() > 0) {
            command << "--job-id"
            command << jobId
        }

        def result = executeCommand(command.join(' '))


        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to create Fovus job: ${result.error}")
        }

        // Get the Job ID (the last line of the output)
        log.debug "[FOVUS] Job created with ID: ${jobId}"

        return jobId
    }

    void uploadJobFile(String basePath, String filePath, String jobId){
        def command = []
        if(jobId){
            def parts = filePath.split('/');
            println("filePath: $filePath --> ${parts[2..-2].join('/')}")
            command = [config.getCliPath(), '--silence', 'job', 'upload', basePath, parts[2..-2].join('/'), '--job-id', jobId]
        } else {
            command = [config.getCliPath(), '--silence', 'storage', 'upload', basePath, filePath.substring(0, filePath.lastIndexOf('/'))]
        }
        def result = executeCommand(command.join(' '))

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to upload file: ${result.error}")
        }
    }

    String uploadEmptyDirectory(String filePath, String jobId){
        def command = []
        if(jobId){
            println("filePath: $filePath")
            def parts = filePath.tokenize('/')
            def afterTwo = parts.size() > 2 ? parts[2..-1].join('/') : filePath
            println afterTwo
            command = [config.getCliPath(), '--silence', 'job', 'upload', afterTwo , '--job-id', jobId, '--empty-dir True']
        } else {
            def basePath = "dummy"
            command = [config.getCliPath(), '--silence', 'storage', 'upload', basePath, filePath, '--empty-dir True']
        }
        def result = executeCommand(command.join(' '))

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to upload file: ${result.error}")
        }
        return jobId
    }

    String downloadJobFile(String jobId, String targetPath, String filePath){
        def command = [config.getCliPath(), '--silence', 'job', 'download', targetPath, '--job-id', jobId, '--include-paths', filePath]
        def result = executeCommand(command.join(' '))

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to upload file: ${result.error}")
        }
        return jobId
    }

    ObjectMetaData getFileObject(String path, String jobId){
        println("Inside getFileObject")
        def command = [config.getCliPath(), '--silence', 'job', 'list-objects']
        if (jobId) {
            def parts = path.tokenize('/')
            def afterTwo = parts.size() > 2 ? parts[2..-1].join('/') : path
            command << afterTwo
            command << '--job-id'
            command << jobId.toString()
        } else {
            command << path
        }

        def result = executeCommand(command.join(' '))

        if (result.exitCode != 0) {
            throw new RuntimeException("Failed to upload file: ${result.error}")
        }


        try{
            def output = result.output.toString()

            def jsonText = output.readLines().drop(2).join('\n')
            println("Converting response -- $jsonText")

            def json = new JsonSlurper().parseText(jsonText)
            if (!(json instanceof List)) {
                println("Converting response error")
                throw new RuntimeException("No objects found for path: ${path}")
            }

            println("Converted - ${json.toString()}")


            List<Map> jsonList = (List<Map>) json
            Map obj = jsonList.get(0)

            def lastModifiedStr = obj['LastModified'] as String
            def dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX")
            Date lastModifiedDate = dateFormat.parse(lastModifiedStr)

            def objMetaData =  new ObjectMetaData(
                    obj['Key'] as String,
                    lastModifiedDate,
                    obj['ETag'] as String,
                    obj['ChecksumAlgorithm'] as List<String>,
                    obj['ChecksumType'] as String,
                    (obj['Size'] as Number).longValue(),
                    obj['StorageClass'] as String
            )

            println("objMetaData --> ${objMetaData.toString()}")

            return objMetaData
        } catch (Exception e){
            println("error ----- $e")
        }
        return null;
    }

    FovusJobStatus getJobStatus(String jobId) {
        def command = [config.getCliPath(), 'job', 'status', '--job-id', jobId]
        def result = executeCommand(command.join(' '))

        def jobStatus = result.output.trim().split('\n')[-1]
        log.debug "[FOVUS] Job Id: ${jobId}, status: ${jobStatus}"

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
            log.debug "[FOVUS] Job Id: ${jobId}, status: ${runStatus}"

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
        }catch(Exception e) {
            log.error("getRunStatus error, ex=${e.message}")
            return "true"
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
            log.debug "[FOVUS] Executing command (attempt ${attempt}/${maxRetries}): ${command}"

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

            log.debug "[FOVUS] Command executed with exit code: ${result.exitCode}"
            log.debug "[FOVUS] Command output: ${result.output}"
            log.debug "[FOVUS] Command error: ${result.error}"

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

        log.debug "[FOVUS] Download job outputs"
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
