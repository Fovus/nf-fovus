package nextflow.fovus.task

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.fovus.FovusConfig
import nextflow.fovus.FovusUtil

/**
 * Client for interacting with Fovus Task
 */
@CompileStatic
@Slf4j
class FovusTaskClient {
    private FovusConfig config

    FovusTaskClient(FovusConfig config) {
        this.config = config
    }

    static String getStatusFromJsonOutput(fullOutputString) {
        // 1. Extract the JSON part from the string
        // This regex looks for a '[' followed by anything, ending with a ']'
        def matcher = (fullOutputString =~ /(?s)\[.*]/)
        if (matcher.find()) {
            def jsonPart = matcher.group(0) // Get the full matched JSON string

            // 2. Parse the JSON part
            def slurper = new JsonSlurper()
            def parsedData = (List<Map<String, Object>>) slurper.parseText(jsonPart)

            // 3. Access the status from the first element of the array
            // Use null-safe operator and Elvis operator for robustness
            return parsedData[0]?.get("status") as String ?: "Status Not Found"
        } else {
            // Handle case where no JSON part is found
            return "No JSON found in output"
        }
    }

    FovusTaskStatus getTaskStatus(String jobId, String taskName) {
        def command = [config.getCliPath(), 'task', 'list', '--job-id', jobId, '--task-names', taskName]
        try {
            def result = FovusUtil.executeCommand(command)
            def taskStatus = getStatusFromJsonOutput(result.output)

            // If status not found immediately after submission then consider as CREATED
            if (taskStatus == "Status Not Found" && FovusUtil.isRecentlySubmitted(jobId)) {
                log.trace "[FOVUS] Use CREATED for recently submitted task. Job Id: ${jobId}, task name: ${taskName}"
                return FovusTaskStatus.CREATED
            }

            log.trace "[FOVUS] Job Id: ${jobId}, status: ${taskStatus}"

            switch (taskStatus) {
                case 'Pending':
                    return FovusTaskStatus.CREATED
                case 'Completed':
                    return FovusTaskStatus.COMPLETED
                case 'Failed':
                    return FovusTaskStatus.FAILED
                case 'Running':
                    return FovusTaskStatus.RUNNING
                case 'Requeued':
                    return FovusTaskStatus.REQUEUED
                case 'Terminated':
                    return FovusTaskStatus.TERMINATED
                case 'Terminating':
                    return FovusTaskStatus.TERMINATING
                case 'Uncompleted':
                    return FovusTaskStatus.UNCOMPLETE
                case 'Walltime Reached':
                    return FovusTaskStatus.WALLTIME_REACHED
                default:
                    log.error "[FOVUS] Unknown job status: ${taskStatus}"
                    throw new RuntimeException("Unknown job status: ${taskStatus}")
            }
        } catch (Exception e) {
            log.error("getTaskStatus error, ex=${e.message}")
            throw new RuntimeException("getTaskStatusError")
        }
    }
}


enum FovusTaskStatus {
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
