package nextflow.fovus

import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.file.FileHelper
import nextflow.file.FileHolder
import nextflow.processor.TaskArrayRun
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.util.ArrayBag

import java.nio.file.Files
import java.nio.file.Path

/**
 * Static helper methods
 */
@Slf4j
@CompileStatic
class FovusUtil {
    /**
     * Get the workDir of a file (eg, an output of a previous task)
     * based on the session workDir
     *
     * @param sessionWorkDir The session workDir
     * @param file The file path
     * @return The absolute to the task workDir of the file
     */
//    static Path getWorkDirOfFile(Path sessionWorkDir, Path file) {
//        final relativePathOfFile = sessionWorkDir.relativize(file);
//        final relativeTaskWorkDir = relativePathOfFile.subpath(0, 2) // Eg, ab/123
//
//        return sessionWorkDir.resolve(relativeTaskWorkDir.subpath(0, 2))
//    }

    /**
     * Get the Fovus remote path of a file
     *
     * @param executor The Fovus executor with jobIdMap
     * @param currentTaskWorkDir The workDir of the current task
     * @param remoteFilePath The output file from previous task or a local input file of the current task
     * @return The Fovus remote path start with /fovus-storage if the file is remote. Otherwise, return null.
     */
//    static Path getFovusRemotePath(FovusExecutor executor, Path remoteFilePath) {
//        final inputWorkDir = getWorkDirOfFile(executor.getWorkDir(), remoteFilePath)
//
//        def jobId = getJobId(executor, inputWorkDir)
//        if (!jobId) {
//            // This could be a local input files, return the original path
//            return null
//        }
//
//        final fovusStorageRemotePath = remoteFilePath.toString().replace(inputWorkDir.parent.toString(), "/fovus-storage/jobs/$jobId")
//        log.debug "[FOVUS] Fovus remote path for file ${remoteFilePath} is ${fovusStorageRemotePath}"
//
//        return Path.of(fovusStorageRemotePath)
//    }

    /**
     * Method to check if the file is part of the local staging dir
     *
     * @param executor The Nextflow executor
     * @param filePath The absolute path of the file
     */
//    static boolean isStageFile(Executor executor, Path filePath) {
//        return filePath.toAbsolutePath().normalize().startsWith(executor.getStageDir())
//    }

    static String getJobId(FovusExecutor executor, Path inputWorkDir) {
        final jobIdMap = executor.getJobIdMap()
        return jobIdMap.get(inputWorkDir.toString())
    }

    /**
     * Method to check if the file is Fovus remote file
     */
//    static boolean isFovusRemoteFile(FovusExecutor executor, Path remoteFilePath) {
//        if (isStageFile(executor, remoteFilePath)) {
//            return false
//        }
//
//        final fovusRemotePath = getFovusRemotePath(executor, remoteFilePath)
//        return fovusRemotePath != null
//    }

    /**
     * Method to move local input files under task directory
     */
//    static boolean moveLocalFilesToTaskDir(TaskRun task, FovusExecutor executor) {
//        final jobIdMap = executor.getJobIdMap()
//
//        log.debug "[FOVUS] Task Inputs: ${task.inputs}";
//        for( def it : task.inputs ) {
//            if( it.value instanceof ArrayBag){
//                def fileHolderList = it.value;
//                fileHolderList.each { item ->
//                    if(item instanceof FileHolder){
//                        final inputWorkDir = getWorkDirOfFile(executor.getWorkDir(), item.storePath)
//                        final jobId = jobIdMap.get(inputWorkDir.toString())
//                        if (jobId) {
//                            log.debug "[FOVUS] filepath ${item.storePath} belongs to jobId: ${jobId}";
//                            return
//                        }
//                        def fileName = item.storePath.toString().split("/")[-1];
//                        def newPath = Path.of("${task.workDir}/${fileName}");
//                        if (Files.exists(newPath)) {
//                            log.debug "[FOVUS] Skipping copy, already exists: ${newPath}"
//                        } else {
//                            log.debug "[FOVUS] Moving file: ${item.storePath} to ${newPath}"
//                            FileHelper.copyPath(item.storePath, newPath)
//                        }
//                    }
//                }
//            }
//        }
//    }

//    static boolean copyFilesToTaskForArray(TaskArrayRun task) {
//        def destination =  task.workDir.toString();
//        def sourcePaths = new ArrayList<String>();
//
//        for(TaskHandler taskHandler : task.getChildren()){
//            sourcePaths.add(taskHandler.getTask().workDir.toString())
//        }
//
//        log.debug "[FOVUS] sourcePaths-> $sourcePaths";
//        log.debug "[FOVUS] destination-> $destination";
//        for( String sourcePath : sourcePaths ) {
//            def dirName = sourcePath.split("/")[-1];
//            FileHelper.copyPath(Path.of(sourcePath), Path.of("${destination}/${dirName}"));
//        }
//    }

    /**
     * Helper method to execute Fovus CLI commands with retry logic
     * @param command
     * @return
     */
    static public CliExecutionResult executeCommand(final List command) {
        int maxRetries = 3
        int attempt = 0
        CliExecutionResult result = null

        while (attempt < maxRetries) {
            attempt++
            log.debug "[FOVUS] Executing command (attempt ${attempt}/${maxRetries}): ${command.join(' ')}"

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
}

@MapConstructor
class CliExecutionResult {
    int exitCode
    String output
    String error
}
