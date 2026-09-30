package fovus.plugin.s3

import fovus.plugin.FovusConfig
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

/**
 * Fetches direct-mode storage credentials by running the hidden {@code fovus storage credentials} command.
 *
 * The CLI's stdout is an anonymous pipe read only by this JVM. It is never logged, never put in an
 * exception and never written anywhere; only the CLI's stderr, with the personal access token scrubbed,
 * appears in error messages. That is why this does not use {@code FovusUtil.executeCommand}, which logs
 * stdout.
 */
@Slf4j
@CompileStatic
class FovusStorageCredentialsSource implements CredentialsFetcher {

    static final int MAX_OUTPUT_BYTES = 64 * 1024
    static final int MAX_ATTEMPTS = 3
    static final String NOT_SIGNED_IN =
            'Fovus CLI is not signed in; run `fovus auth login` or configure `fovus.auth`'
    static final String UPGRADE_CLI =
            'Direct mode (fovus:// workDir) needs a newer Fovus CLI that provides storage credentials; ' +
            'upgrade it with `pip install --upgrade fovus`'

    private final FovusConfig config
    private final String pipelineId
    private final Duration timeout
    private final long retryDelayMillis

    FovusStorageCredentialsSource(FovusConfig config, String pipelineId) {
        this(config, pipelineId, Duration.ofSeconds(60), 2000L)
    }

    @PackageScope
    FovusStorageCredentialsSource(FovusConfig config, String pipelineId, Duration timeout, long retryDelayMillis) {
        this.config = config
        this.pipelineId = pipelineId
        this.timeout = timeout
        this.retryDelayMillis = retryDelayMillis
    }

    static String expectedPrefix(String pipelineId) {
        return "pipelines/${pipelineId}/".toString()
    }

    @Override
    StorageCredentials fetch() throws StorageCredentialsException {
        StorageCredentialsException lastFailure = null
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return fetchOnce()
            }
            catch (StorageCredentialsException e) {
                if (!e.retryable) throw e
                lastFailure = e
                log.debug "[FOVUS] Fetching Fovus storage credentials failed (attempt ${attempt}/${MAX_ATTEMPTS}): ${e.message}"
                if (attempt < MAX_ATTEMPTS) sleep(retryDelayMillis)
            }
        }
        throw lastFailure
    }

    private List<String> command() {
        return [config.getCliPath(), '--silence', 'storage', 'credentials', '--pipeline-id', pipelineId]
    }

    private StorageCredentials fetchOnce() throws StorageCredentialsException {
        final process = start()
        process.outputStream.close()
        final stdout = drain(process.inputStream)
        final stderr = drain(process.errorStream)

        if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly()
            throw new StorageCredentialsException(
                    "Timed out after ${timeout.toMillis()} ms waiting for the Fovus CLI to return storage credentials".toString(),
                    true)
        }

        final exitCode = process.exitValue()
        final errorBytes = stderr.get()
        final errorText = errorBytes == null ? '' : new String(errorBytes, StandardCharsets.UTF_8).trim()

        if (exitCode == 0) {
            final output = stdout.get()
            if (output == null) throw StorageCredentials.contractError()
            return StorageCredentials.parse(output, expectedPrefix(pipelineId))
        }
        // Exit 3 means "not signed in"; the CLI prints that message to stdout, which is never read on failure
        if (exitCode == 3) throw new StorageCredentialsException(NOT_SIGNED_IN, false)
        if (errorText.contains('No such command')) throw new StorageCredentialsException(UPGRADE_CLI, false)
        if (exitCode == 2) {
            throw new StorageCredentialsException(
                    "The Fovus CLI refused to print storage credentials: ${config.redactSecret(errorText)}".toString(), false)
        }
        throw new StorageCredentialsException(
                "Fovus CLI could not provide storage credentials (exit ${exitCode}): ${config.redactSecret(errorText)}".toString(),
                true)
    }

    private Process start() throws StorageCredentialsException {
        final builder = new ProcessBuilder(command())
        builder.environment().putAll(config.cliEnv())
        try {
            return builder.start()
        }
        catch (IOException e) {
            throw new StorageCredentialsException(
                    "Unable to run the Fovus CLI at `${config.getCliPath()}`: ${e.message}".toString(), false)
        }
    }

    /**
     * Read a stream to its end on a background thread. Completes with {@code null} when it holds more than
     * {@link #MAX_OUTPUT_BYTES}; the rest is still read and dropped so the CLI never blocks on a full pipe.
     */
    private static CompletableFuture<byte[]> drain(InputStream stream) {
        final result = new CompletableFuture<byte[]>()
        final reader = new Thread({
            try {
                final buffer = new ByteArrayOutputStream()
                final chunk = new byte[8192]
                boolean overflow = false
                int count
                while ((count = stream.read(chunk)) != -1) {
                    if (overflow || buffer.size() + count > MAX_OUTPUT_BYTES) {
                        overflow = true
                        continue
                    }
                    buffer.write(chunk, 0, count)
                }
                result.complete(overflow ? null : buffer.toByteArray())
            }
            catch (Throwable e) {
                result.completeExceptionally(e)
            }
        } as Runnable, 'fovus-cli-output')
        reader.daemon = true
        reader.start()
        return result
    }
}
