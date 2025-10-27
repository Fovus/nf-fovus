package groovy.fovus

import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.util.logging.Slf4j
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

    static boolean isRecentlySubmitted(String jobId) {
        def tsStr = jobId.split("-")[0]
        def tsMs = tsStr.toLong()

        // Current UTC time in ms
        def nowMs = System.currentTimeMillis()
        def diffMs = nowMs - tsMs

        // Check if within 1 minute (100000 ms)
        return diffMs <= 1 * 60 * 1000 && diffMs >= 0
    }

}

@MapConstructor
class CliExecutionResult {
    int exitCode
    String output
    String error
}
