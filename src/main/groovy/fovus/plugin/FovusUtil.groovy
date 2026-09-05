package fovus.plugin

import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.util.ConfigObject
import groovy.util.logging.Slf4j
import nextflow.config.ConfigParser
import nextflow.config.ConfigParserFactory
import java.nio.charset.StandardCharsets
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
    static Path getWorkDirOfFile(Path sessionWorkDir, Path file) {
        final relativePathOfFile = sessionWorkDir.relativize(file);
        final relativeTaskWorkDir = relativePathOfFile.subpath(0, 2) // Eg, ab/123

        return sessionWorkDir.resolve(relativeTaskWorkDir.subpath(0, 2))
    }

    /**
     * Helper method to execute Fovus CLI commands with retry logic
     *
     * @param command The command and its arguments
     * @param extraEnv Environment variables to add to the subprocess environment, on top of the
     *  parent process's own environment (eg PATH). {@code null} or empty leaves the environment
     *  untouched.
     * @return
     */
    static public CliExecutionResult executeCommand(final List command, final Map<String, String> extraEnv = null) {
        int maxRetries = 3
        int attempt = 0
        CliExecutionResult result = null
        final secret = extraEnv?.get('FOVUS_PAT')

        while (attempt < maxRetries) {
            attempt++
            log.debug "[FOVUS] Executing command (attempt ${attempt}/${maxRetries}): ${command.join(' ')}"

            def stdout = new StringBuilder()
            def stderr = new StringBuilder()

            final pb = new ProcessBuilder(command.collect { it.toString() })
            if (extraEnv) {
                pb.environment().putAll(extraEnv)
            }
            def process = pb.start()
            // waitForProcessOutput() joins the stream-consumer threads before returning;
            // consumeProcessOutput() + waitFor() races them against process exit and can
            // return a result whose output/error are still empty.
            process.waitForProcessOutput(stdout, stderr)

            result = new CliExecutionResult(
                    exitCode: process.exitValue(),
                    output: stdout.toString(),
                    error: stderr.toString()
            )

            log.debug "[FOVUS] Command executed with exit code: ${result.exitCode}"
            log.debug "[FOVUS] Command output: ${redact(result.output, secret)}"
            log.debug "[FOVUS] Command error: ${redact(result.error, secret)}"

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

    /**
     * Scrub every occurrence of {@code secret} out of {@code text}, eg before logging CLI output or
     * embedding it in an exception message. A no-op when either argument is empty.
     */
    static String redact(String text, String secret) {
        if (!text || !secret) return text
        return text.replace(secret, '[REDACTED]')
    }

    /**
     * Re-parse the given Nextflow config files in Nextflow's secret-stripping mode (which rewrites
     * any {@code secrets.X} property reference to the literal string {@code "secrets.X"} while
     * leaving literals and method calls, including {@code System.getenv()}, untouched) and read back
     * the value at each of the given dotted config paths.
     *
     * Used to structurally verify that a config field is set via {@code secrets.X}: a literal value
     * and a resolved secret are indistinguishable once Nextflow has interpolated {@code secrets.X}
     * to its real string, so this has to be checked against a separately-stripped parse rather than
     * the normally-resolved config.
     *
     * @param configFiles The config files resolved by Nextflow, ie {@code session.configFiles}
     * @param dottedPaths The dotted config paths to read back, eg {@code fovus.auth.personalAccessToken}
     * @return A map from each requested path to the stripped value found there, or {@code null} when
     *  the path resolves to anything other than a plain string (eg it is absent, or a literal that
     *  the parser reduced to a non-string value)
     */
    static Map<String, String> stripSecretsRefs(List<Path> configFiles, List<String> dottedPaths) {
        final result = new LinkedHashMap<String, String>()
        ConfigObject merged = new ConfigObject()

        if (configFiles) {
            final ConfigParser parser = ConfigParserFactory.create().setStripSecrets(true)
            for (Path file : configFiles) {
                try {
                    if (!file || !Files.exists(file)) continue
                    merged = merged.merge(parser.parse(file)) as ConfigObject
                } catch (Exception e) {
                    log.warn "[FOVUS] Unable to parse config file `${file}` while checking secrets usage: ${e.message}"
                }
            }
        }

        for (String path : dottedPaths) {
            final value = merged.navigate(path)
            result[path] = value instanceof String ? (String) value : null
        }

        return result
    }

    /**
     * Read the verbatim content of the Nextflow config file(s) backing this run.
     *
     * A single config file is returned byte-for-byte. When Nextflow resolved several
     * files (eg, ~/.nextflow/config plus ./nextflow.config plus -c custom.config) their
     * contents are concatenated behind a comment header naming each source.
     *
     * @param configFiles The config files resolved by Nextflow, ie {@code session.configFiles}
     * @return The config content, or null when there is no readable config file
     */
    static String readNextflowConfig(List<Path> configFiles) {
        if (!configFiles) return null

        final includeHeaders = configFiles.size() > 1
        final content = new StringBuilder()

        for (Path file : configFiles) {
            try {
                if (!file || !Files.exists(file)) continue

                if (content.length() > 0) content.append('\n')
                if (includeHeaders) content.append("// ==== ${file.toAbsolutePath()} ====\n")
                content.append(new String(Files.readAllBytes(file), StandardCharsets.UTF_8))
            } catch (Exception e) {
                log.warn "[FOVUS] Unable to read Nextflow config file `${file}`: ${e.message}"
            }
        }

        return content.length() > 0 ? content.toString() : null
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
