package fovus.plugin

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

    /**
     * Normalize Nextflow glob patterns into those compatible with aws cli include/exclude pattern.
     *
     * <pre>
     *   "*{a,b}{,_1,_2}.fq.gz" -> ["*a.fq.gz","*a_1.fq.gz","*a_2.fq.gz","*b.fq.gz","*b_1.fq.gz","*b_2.fq.gz"]
     * </pre>
     */
    static List<String> normalizeGlobPath(String pattern) {
        // aws cli doesn't treat ** specially; usually * is enough
        pattern = pattern.replace("**", "*")

        int open = -1
        int depth = 0

        // Find first top-level {...}
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i)
            if (c == (char) '{') {
                if (depth == 0) open = i
                depth++
            } else if (c == (char) '}') {
                depth--
                if (depth == 0 && open >= 0) {
                    int close = i
                    String prefix = pattern.substring(0, open)
                    String body = pattern.substring(open + 1, close)
                    String suffix = pattern.substring(close + 1)

                    // Split body by commas at depth 0 (supports nested braces)
                    List<String> parts = []
                    int partStart = 0
                    int d2 = 0
                    for (int j = 0; j < body.length(); j++) {
                        char cj = body.charAt(j)
                        if (cj == (char) '{') d2++
                        else if (cj == (char) '}') d2--
                        else if (cj == (char) ',' && d2 == 0) {
                            parts << body.substring(partStart, j)
                            partStart = j + 1
                        }
                    }
                    parts << body.substring(partStart)

                    // Recurse for each option
                    List<String> out = []
                    for (String opt : parts) {
                        out.addAll(normalizeGlobPath(prefix + opt + suffix))
                    }
                    return out
                }
            }
        }

        // No braces left
        return [pattern]
    }

}

@MapConstructor
class CliExecutionResult {
    int exitCode
    String output
    String error
}
