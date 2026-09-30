package fovus.plugin

import fovus.plugin.nio.FovusPath
import fovus.plugin.s3.StorageCredentialsException
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path

/**
 * Reads a task's exit status once its Fovus job has finished.
 *
 * In direct mode a temporary S3 error must not fail a task that actually succeeded, so the read is
 * deferred to the next poll, up to {@link #MAX_CONSECUTIVE_STORAGE_FAILURES} times in a row. Mount mode
 * behaves as before: anything unreadable counts as a missing exit status.
 */
@Slf4j
@CompileStatic
class ExitStatusReader {

    static final int MAX_CONSECUTIVE_STORAGE_FAILURES = 30

    private int consecutiveFailures = 0
    private IOException failure

    /**
     * @return the exit status; {@code Integer.MAX_VALUE} when it is missing or unreadable; or {@code null}
     *  when a temporary Fovus storage error means the caller should try again on its next poll
     */
    Integer read(Path exitFile, Path taskDir) {
        try {
            if (exitFile instanceof FovusPath) {
                // One listing of the task folder: if this fails, so would Nextflow's output collection
                Files.newDirectoryStream(taskDir).close()
            }
            final status = Integer.valueOf(readText(exitFile).trim())
            consecutiveFailures = 0
            return status
        }
        catch (NoSuchFileException e) {
            log.debug "[FOVUS] Cannot read exit status: ${e.message} does not exist"
            return Integer.MAX_VALUE
        }
        catch (IOException e) {
            if (!(exitFile instanceof FovusPath)) {
                log.debug "[FOVUS] Cannot read exit status from ${exitFile} | ${e.message}"
                return Integer.MAX_VALUE
            }
            final permanent = e instanceof StorageCredentialsException && !((StorageCredentialsException) e).retryable
            if (!permanent && ++consecutiveFailures < MAX_CONSECUTIVE_STORAGE_FAILURES) {
                log.debug "[FOVUS] Temporary Fovus storage error reading ${exitFile} (${consecutiveFailures}/${MAX_CONSECUTIVE_STORAGE_FAILURES}); retrying on the next poll | ${e.message}"
                return null
            }
            failure = e
            return Integer.MAX_VALUE
        }
        catch (Exception e) {
            log.debug "[FOVUS] Cannot read exit status from ${exitFile} | ${e.message}"
            return Integer.MAX_VALUE
        }
    }

    /** The storage error that made {@link #read} give up, or {@code null}. */
    IOException getFailure() {
        return failure
    }

    private static String readText(Path file) {
        final input = Files.newInputStream(file)
        try {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8)
        }
        finally {
            input.close()
        }
    }
}
