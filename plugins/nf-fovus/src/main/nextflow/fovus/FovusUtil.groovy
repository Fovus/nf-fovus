package nextflow.fovus

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.Executor

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
    static Path getWorkDirOfFile(Path sessionWorkDir, Path file) {
        final relativePathOfFile = sessionWorkDir.relativize(file);
        final relativeTaskWorkDir = relativePathOfFile.subpath(0, 2) // Eg, ab/123

        return sessionWorkDir.resolve(relativeTaskWorkDir.subpath(0, 2))
    }

    /**
     * Get the Fovus remote path of a file
     *
     * @param executor The Fovus executor with jobIdMap
     * @param currentTaskWorkDir The workDir of the current task
     * @param remoteFilePath The output file from previous task or a local input file of the current task
     * @return The Fovus remote path start with /fovus-storage if the file is remote. Otherwise, return null.
     */
    static Path getFovusRemotePath(FovusExecutor executor, Path currentTaskWorkDir, Path remoteFilePath) {
        final jobIdMap = executor.getJobIdMap()
        final inputWorkDir = getWorkDirOfFile(executor.getWorkDir(), remoteFilePath)

        final jobId = jobIdMap.get(inputWorkDir.toString())

        if (!jobId) {
            // This could be a local input files, return the original path
            return null
        }

        final fovusStorageRemotePath = remoteFilePath.toString().replace(inputWorkDir.parent.toString(), "/fovus-storage/jobs/$jobId")
        log.trace "[FOVUS] Fovus remote path for file ${remoteFilePath} is ${fovusStorageRemotePath}"

        return Path.of(fovusStorageRemotePath)
    }

    /**
     * Method to check if the file is part of the local staging dir
     *
     * @param executor The Nextflow executor
     * @param filePath The absolute path of the file
     */
    static boolean isStageFile(Executor executor, Path filePath) {
        return filePath.toAbsolutePath().normalize().startsWith(executor.getStageDir())
    }

    /**
     * Method to check if the file is Fovus remote file
     */
    static boolean isFovusRemoteFile(FovusExecutor executor, Path currentTaskWorkDir, Path remoteFilePath) {
        if (isStageFile(executor, remoteFilePath)) {
            return false
        }

        final fovusRemotePath = getFovusRemotePath(executor, currentTaskWorkDir, remoteFilePath)
        return fovusRemotePath != null
    }
}
