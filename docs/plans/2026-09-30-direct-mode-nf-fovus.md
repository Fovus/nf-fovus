# Direct storage mode (nf-fovus) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let nf-fovus run a pipeline with `workDir = 'fovus:///fovus-storage/pipelines'` and no FUSE mount, reading and writing the work directory with the AWS S3 SDK using temporary credentials from the Fovus CLI.

**Architecture:** The plugin's existing `fovus://` NIO filesystem gains a read/write `pipelines` area backed by `FovusS3Client` (two sync AWS SDK v2 clients: read token and write token). Credentials come from the hidden `fovus storage credentials` command through a dedicated non-logging runner and are cached and refreshed in memory. The executor picks mount or direct mode from `session.workDir` through a small `WorkDirStorage` seam; everything else (wrapper writing, staging, output collection, `-resume`, `publishDir`) is Nextflow's normal NIO code running against the new filesystem.

**Tech Stack:** Groovy 4 (`@CompileStatic`) and Java 17+, Nextflow 25.10.0 plugin API, AWS SDK for Java v2 (`s3`, `url-connection-client`), Jackson, Spock 2.3, Testcontainers MinIO.

**Spec:** `docs/specs/2026-09-30-direct-storage-mode-design.md`. The CLI half is `docs/plans/2026-09-30-direct-mode-cli-credentials-command.md`; its Task 1 (the token spike) runs before Task 5 of this plan.

**Branch:** create `feat/direct-storage-mode` from `master` in `/Users/jashminpatel/Desktop/Code/Nextflow-Plugin/nf-fovus`.

## Global Constraints

- Nextflow 25.10.0 plugin API; builds and tests on Java 17 and 21; Spock `2.3-groovy-4.0`. New Groovy classes are `@CompileStatic`; log lines start with `[FOVUS]`.
- Mount mode behaves exactly as today. The only change it sees is the removal of `chmod` calls, which never had an effect on mountpoint-s3.
- Direct mode is selected only by `workDir` = `fovus:///fovus-storage/pipelines` (also accepted: trailing `/`, and `fovus://fovus-storage/pipelines`). It is refused on Fovus-hosted runs (`WORKFLOW_HOST=REMOTE`).
- CLI invocation: `[cliPath, '--silence', 'storage', 'credentials', '--pipeline-id', <pid>]`; `FOVUS_EMAIL`/`FOVUS_PAT` only through `config.cliEnv()`; stdin closed; stdout capped at 64 KiB; 60 s timeout; 3 attempts with 2 s backoff; never through `FovusUtil.executeCommand`.
- Credential contract v1: `Version` (int 1), `Bucket`, `Region`, `Prefix` (must equal `pipelines/<pid>/`), `Read` and `Write` each `{AccessKeyId, SecretAccessKey, SessionToken, Expiration}` (ISO-8601 UTC). Any deviation → `Unexpected response from Fovus CLI (expected contract v1)`.
- Refresh: start 10 minutes before expiry; callers block for fresh credentials in the last 2 minutes.
- Credentials never reach: command-line arguments, the task environment, `.command.run`, `.command.fovus.env`, `run.sh`, the job config JSON, Nextflow's session config, any file, `.nextflow.log`, or any exception message.
- S3 clients: sync `S3Client` + `url-connection-client` only (no Apache, Netty or CRT); standard retries, max 10 attempts; checksum calculation and validation `WHEN_REQUIRED`.
- Multipart part size 16 MiB (5 MiB in tests); 4 transfer threads.
- Prefix guard: writes outside `pipelines/<pid>/` are refused before any S3 call; reads outside it are `NoSuchFileException`.
- S3 error messages carry error code, HTTP status, request ID and key only — never the S3 error body, never the SDK exception as a cause.
- Completion: a temporary storage error defers the task; it fails after 30 consecutive failed polls.
- Unit tests: `./gradlew test` (no Docker). S3 tests: `./gradlew integrationTest` (Spock `@Tag('integration')`, MinIO in Docker).

Two deliberate differences from the spec's wording, already reflected in the spec: the credential cache is a small hand-written cache with an injectable `Clock` instead of the SDK's `CachedSupplier`, and the "CLI too old" message asks for `pip install --upgrade fovus` instead of naming a version.

## Review Focus

1. `workDir` written as `fovus://fovus-storage/pipelines` (two slashes, as users type after `-w`) or with a trailing slash — must still select direct mode. Test in Task 7.
2. Task outputs and inputs whose names contain spaces, `#`, `+`, `%`, `=` or non-ASCII letters (`résumé.txt`) — must write, list, glob and read back unchanged. Test in Task 6.
3. Zero-byte files (an empty `.command.err`, empty outputs) — must exist as regular files of size 0, never look like folders. Test in Task 6.
4. Names that share a prefix (`out.txt.bak` present but `out.txt` missing; `sample/` next to `sample_2/`) — `exists(out.txt)` is false and listing `sample` shows only its own children. Test in Task 6.
5. A stray line on the CLI's stdout before the JSON (e.g. an older CLI printing `Authenticating...`) — must be a contract error that never echoes stdout. Tests in Task 1 and Task 2.

---

### Task 1: AWS SDK dependency and the credential contract

**Files:**
- Modify: `build.gradle`
- Create: `src/main/groovy/fovus/plugin/s3/StorageCredentialsException.groovy`
- Create: `src/main/groovy/fovus/plugin/s3/SessionKeys.groovy`
- Create: `src/main/groovy/fovus/plugin/s3/StorageCredentials.groovy`
- Create: `src/main/groovy/fovus/plugin/s3/CredentialsFetcher.groovy`
- Test: `src/test/groovy/fovus/plugin/s3/StorageCredentialsTest.groovy`

**Interfaces:**
- Produces:
  - `class StorageCredentialsException extends IOException` — `StorageCredentialsException(String message, boolean retryable)`, `boolean isRetryable()`.
  - `final class SessionKeys` — `SessionKeys(String accessKeyId, String secretAccessKey, String sessionToken, Instant expiration)`, getters, `AwsSessionCredentials toAwsCredentials()`, `toString()` shows only the expiry.
  - `final class StorageCredentials` — `StorageCredentials(String bucket, String region, String prefix, SessionKeys read, SessionKeys write)`, getters `bucket region prefix read write`, `Instant getExpiration()` (earlier of the two), `static StorageCredentials parse(byte[] json, String expectedPrefix)`, `static StorageCredentialsException contractError()`, `static final String CONTRACT_ERROR`.
  - `interface CredentialsFetcher { StorageCredentials fetch() throws StorageCredentialsException }`.
  - Test helper `StorageCredentialsTest.validJson(Map overrides = [:])` returning a v1 document for prefix `pipelines/p-1-user/` (reused by Task 2).

- [ ] **Step 1: Add the AWS SDK to the build**

In `build.gradle`, add inside `dependencies { … }`, after the Jackson lines:

```groovy
    // Direct storage mode: S3 access with temporary Fovus credentials. Sync client over the JDK's
    // HttpURLConnection only -- no Apache, Netty or CRT HTTP clients in the plugin.
    implementation('software.amazon.awssdk:s3:2.31.0') {
        exclude group: 'software.amazon.awssdk', module: 'apache-client'
        exclude group: 'software.amazon.awssdk', module: 'netty-nio-client'
    }
    implementation 'software.amazon.awssdk:url-connection-client:2.31.0'
```

and after the `dependencies` block:

```groovy
// Nextflow provides SLF4J at runtime; do not ship a second copy inside the plugin.
configurations.runtimeClasspath {
    exclude group: 'org.slf4j', module: 'slf4j-api'
}
```

If Gradle cannot resolve `2.31.0`, use the newest `2.31.x` from Maven Central for both artifacts.

- [ ] **Step 2: Write the failing test**

Create `src/test/groovy/fovus/plugin/s3/StorageCredentialsTest.groovy`:

```groovy
package fovus.plugin.s3

import groovy.json.JsonOutput
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.time.Instant

class StorageCredentialsTest extends Specification {

    static final String PREFIX = 'pipelines/p-1-user/'

    /** A contract v1 document for {@link #PREFIX}; {@code overrides} replace top-level keys. */
    static String validJson(Map overrides = [:]) {
        final Map document = [
                Version: 1,
                Bucket : 'fovus-user-ws-us-east-2',
                Region : 'us-east-2',
                Prefix : PREFIX,
                Read   : [AccessKeyId: 'READ-KEY-ID', SecretAccessKey: 'READ-SECRET', SessionToken: 'READ-TOKEN',
                          Expiration : '2026-09-30T13:00:00Z'],
                Write  : [AccessKeyId: 'WRITE-KEY-ID', SecretAccessKey: 'WRITE-SECRET', SessionToken: 'WRITE-TOKEN',
                          Expiration : '2026-09-30T12:59:00Z'],
        ] + overrides
        return JsonOutput.toJson(document)
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8)
    }

    def 'parse should read every field of contract v1'() {
        when:
        def credentials = StorageCredentials.parse(bytes(validJson()), PREFIX)

        then:
        credentials.bucket == 'fovus-user-ws-us-east-2'
        credentials.region == 'us-east-2'
        credentials.prefix == PREFIX
        credentials.read.accessKeyId == 'READ-KEY-ID'
        credentials.read.secretAccessKey == 'READ-SECRET'
        credentials.read.sessionToken == 'READ-TOKEN'
        credentials.write.accessKeyId == 'WRITE-KEY-ID'
        credentials.expiration == Instant.parse('2026-09-30T12:59:00Z')
    }

    def 'parse should reject anything outside the contract without echoing it'() {
        when:
        StorageCredentials.parse(bytes(json), PREFIX)

        then:
        def e = thrown(StorageCredentialsException)
        e.message == StorageCredentials.CONTRACT_ERROR
        !e.retryable

        where:
        json << [
                'not json READ-SECRET',
                'Authenticating...\n' + validJson(),
                validJson() + '\nDone.',
                validJson(Version: 2),
                validJson(Version: '1'),
                validJson(Prefix: 'pipelines/p-2-user/'),
                validJson(Bucket: ''),
                validJson(Read: [AccessKeyId: 'A', SecretAccessKey: 'READ-SECRET', SessionToken: 'T']),
                validJson(Write: [AccessKeyId: 'A', SecretAccessKey: 'S', SessionToken: 'T', Expiration: 'tomorrow']),
                '[]',
        ]
    }

    def 'toString should never show key material'() {
        given:
        def credentials = StorageCredentials.parse(bytes(validJson()), PREFIX)

        expect:
        credentials.toString() == 'StorageCredentials(bucket=fovus-user-ws-us-east-2, prefix=pipelines/p-1-user/, expires 2026-09-30T12:59:00Z)'
        credentials.read.toString() == 'SessionKeys(expires 2026-09-30T13:00:00Z)'
    }

    def 'toAwsCredentials should carry the session token'() {
        when:
        def aws = StorageCredentials.parse(bytes(validJson()), PREFIX).write.toAwsCredentials()

        then:
        aws.accessKeyId() == 'WRITE-KEY-ID'
        aws.secretAccessKey() == 'WRITE-SECRET'
        aws.sessionToken() == 'WRITE-TOKEN'
    }
}
```

- [ ] **Step 3: Run the test to see it fail**

Run: `./gradlew test --tests 'fovus.plugin.s3.StorageCredentialsTest'`
Expected: compilation FAILS with `unable to resolve class StorageCredentials`.

- [ ] **Step 4: Write the classes**

`src/main/groovy/fovus/plugin/s3/StorageCredentialsException.groovy`:

```groovy
package fovus.plugin.s3

import groovy.transform.CompileStatic

/**
 * Direct-mode storage credentials could not be obtained. The message is always safe to show and log:
 * it never contains key material or the Fovus CLI's stdout.
 */
@CompileStatic
class StorageCredentialsException extends IOException {

    /** True when trying again later may succeed, e.g. a network error; false for sign-in or contract problems. */
    final boolean retryable

    StorageCredentialsException(String message, boolean retryable) {
        super(message)
        this.retryable = retryable
    }
}
```

`src/main/groovy/fovus/plugin/s3/SessionKeys.groovy`:

```groovy
package fovus.plugin.s3

import groovy.transform.CompileStatic
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials

import java.time.Instant

/** One set of temporary AWS keys from the Fovus CLI. {@link #toString()} never shows the keys. */
@CompileStatic
final class SessionKeys {

    final String accessKeyId
    final String secretAccessKey
    final String sessionToken
    final Instant expiration

    SessionKeys(String accessKeyId, String secretAccessKey, String sessionToken, Instant expiration) {
        this.accessKeyId = accessKeyId
        this.secretAccessKey = secretAccessKey
        this.sessionToken = sessionToken
        this.expiration = expiration
    }

    AwsSessionCredentials toAwsCredentials() {
        return AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken)
    }

    @Override
    String toString() {
        return "SessionKeys(expires ${expiration})".toString()
    }
}
```

`src/main/groovy/fovus/plugin/s3/StorageCredentials.groovy`:

```groovy
package fovus.plugin.s3

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.CompileStatic

import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * The read and write keys for one pipeline's work directory, as printed by
 * {@code fovus storage credentials} (credential contract v1). {@link #toString()} never shows keys.
 */
@CompileStatic
final class StorageCredentials {

    static final int CONTRACT_VERSION = 1
    static final String CONTRACT_ERROR = 'Unexpected response from Fovus CLI (expected contract v1)'

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

    final String bucket
    final String region
    final String prefix
    final SessionKeys read
    final SessionKeys write

    StorageCredentials(String bucket, String region, String prefix, SessionKeys read, SessionKeys write) {
        this.bucket = bucket
        this.region = region
        this.prefix = prefix
        this.read = read
        this.write = write
    }

    /** The earlier of the two expirations: both key sets are refreshed together. */
    Instant getExpiration() {
        return read.expiration.isBefore(write.expiration) ? read.expiration : write.expiration
    }

    /**
     * Parse the CLI's stdout. Anything that is not exactly one contract v1 document for
     * {@code expectedPrefix} raises {@link #contractError()}, whose message never includes the input.
     */
    static StorageCredentials parse(byte[] json, String expectedPrefix) throws StorageCredentialsException {
        final root = readTree(json)
        if (root == null || !root.isObject()) throw contractError()

        final version = root.path('Version')
        if (!version.isInt() || version.intValue() != CONTRACT_VERSION) throw contractError()

        final prefix = text(root, 'Prefix')
        if (prefix != expectedPrefix) throw contractError()

        return new StorageCredentials(text(root, 'Bucket'), text(root, 'Region'), prefix,
                                      keys(root.path('Read')), keys(root.path('Write')))
    }

    static StorageCredentialsException contractError() {
        return new StorageCredentialsException(CONTRACT_ERROR, false)
    }

    private static JsonNode readTree(byte[] json) {
        try {
            return MAPPER.readTree(json)
        }
        catch (Exception ignored) {
            // Never include the parser's message: it can quote part of the input
            throw contractError()
        }
    }

    private static SessionKeys keys(JsonNode node) {
        if (!node.isObject()) throw contractError()
        return new SessionKeys(text(node, 'AccessKeyId'), text(node, 'SecretAccessKey'), text(node, 'SessionToken'),
                               instant(text(node, 'Expiration')))
    }

    private static Instant instant(String value) {
        try {
            return Instant.parse(value)
        }
        catch (DateTimeParseException ignored) {
            throw contractError()
        }
    }

    private static String text(JsonNode node, String field) {
        final value = node.path(field)
        if (!value.isTextual() || value.textValue().isBlank()) throw contractError()
        return value.textValue()
    }

    @Override
    String toString() {
        return "StorageCredentials(bucket=${bucket}, prefix=${prefix}, expires ${expiration})".toString()
    }
}
```

`src/main/groovy/fovus/plugin/s3/CredentialsFetcher.groovy`:

```groovy
package fovus.plugin.s3

import groovy.transform.CompileStatic

/** Somewhere fresh direct-mode storage credentials come from; the Fovus CLI in production. */
@CompileStatic
interface CredentialsFetcher {
    StorageCredentials fetch() throws StorageCredentialsException
}
```

- [ ] **Step 5: Run the test to see it pass**

Run: `./gradlew test --tests 'fovus.plugin.s3.StorageCredentialsTest'`
Expected: PASS (13 feature iterations).

- [ ] **Step 6: Check the plugin bundle**

Run: `./gradlew assemble && unzip -l build/distributions/nf-fovus-*.zip | grep -E 'awssdk|netty|apache|slf4j'`
Expected: lines for `s3-2.31.0.jar`, `url-connection-client-2.31.0.jar` and other `software.amazon.awssdk` jars; no `netty`, no `apache-client`, no `httpclient`, no `slf4j-api`.

- [ ] **Step 7: Commit**

```bash
git add build.gradle src/main/groovy/fovus/plugin/s3 src/test/groovy/fovus/plugin/s3
git commit -m "Add the direct mode storage credential contract"
```

---

### Task 2: Fetch credentials from the Fovus CLI

**Files:**
- Create: `src/main/groovy/fovus/plugin/s3/FovusStorageCredentialsSource.groovy`
- Test: `src/test/groovy/fovus/plugin/s3/FovusStorageCredentialsSourceTest.groovy`

**Interfaces:**
- Consumes: `FovusConfig.getCliPath()`, `FovusConfig.cliEnv()`, `FovusConfig.redactSecret(String)`; `StorageCredentials.parse`, `StorageCredentials.contractError()`, `StorageCredentialsException` (Task 1).
- Produces: `class FovusStorageCredentialsSource implements CredentialsFetcher` — `FovusStorageCredentialsSource(FovusConfig config, String pipelineId)`; package-scope test constructor `(FovusConfig, String pipelineId, Duration timeout, long retryDelayMillis)`; `static String expectedPrefix(String pipelineId)`; constants `NOT_SIGNED_IN`, `UPGRADE_CLI`, `MAX_OUTPUT_BYTES`, `MAX_ATTEMPTS`.

- [ ] **Step 1: Write the failing test**

Create `src/test/groovy/fovus/plugin/s3/FovusStorageCredentialsSourceTest.groovy`:

```groovy
package fovus.plugin.s3

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fovus.plugin.FovusAuthConfig
import fovus.plugin.FovusConfig
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

class FovusStorageCredentialsSourceTest extends Specification {

    static final String PIPELINE_ID = 'p-1-user'
    static final String PAT = 'pat-secret-value'
    static final List<String> SECRETS = ['READ-SECRET', 'READ-TOKEN', 'WRITE-SECRET', 'WRITE-TOKEN', PAT]

    @TempDir
    Path tempDir

    /** A stand-in `fovus` executable running the given bash lines. */
    private Path fakeCli(String body) {
        final script = tempDir.resolve('fovus')
        Files.writeString(script, "#!/bin/bash\n${body}\n")
        script.toFile().setExecutable(true)
        return script
    }

    private FovusStorageCredentialsSource source(Path cli, Duration timeout = Duration.ofSeconds(10)) {
        final auth = new FovusAuthConfig([email: 'automation@corp.com', personalAccessToken: PAT],
                                          'secrets.FOVUS_EMAIL', 'secrets.FOVUS_PAT')
        final config = new FovusConfig([pipelineName: 'test-pipeline', cliPath: cli.toString()], auth)
        return new FovusStorageCredentialsSource(config, PIPELINE_ID, timeout, 0L)
    }

    private Path credentialsFile() {
        return Files.writeString(tempDir.resolve('creds.json'), StorageCredentialsTest.validJson())
    }

    private int invocations() {
        final counter = tempDir.resolve('count.txt')
        return Files.exists(counter) ? Files.readAllLines(counter).size() : 0
    }

    private static ListAppender<ILoggingEvent> captureLogs() {
        final logger = LoggerFactory.getLogger('fovus.plugin.s3') as Logger
        logger.level = Level.TRACE
        final appender = new ListAppender<ILoggingEvent>()
        appender.start()
        logger.addAppender(appender)
        return appender
    }

    def 'fetch should parse the JSON the CLI prints'() {
        given:
        def cli = fakeCli("cat '${credentialsFile()}'")

        when:
        def credentials = source(cli).fetch()

        then:
        credentials.bucket == 'fovus-user-ws-us-east-2'
        credentials.prefix == 'pipelines/p-1-user/'
        credentials.read.secretAccessKey == 'READ-SECRET'
    }

    def 'fetch should pass the pipeline id as an argument and the PAT only through the environment'() {
        given:
        def cli = fakeCli("""printf '%s\\n' "\$@" > '${tempDir}/args.txt'
env > '${tempDir}/env.txt'
cat '${credentialsFile()}'""")

        when:
        source(cli).fetch()

        then:
        Files.readAllLines(tempDir.resolve('args.txt')) == ['--silence', 'storage', 'credentials', '--pipeline-id', PIPELINE_ID]
        Files.readString(tempDir.resolve('env.txt')).contains("FOVUS_PAT=${PAT}")
        !Files.readString(tempDir.resolve('args.txt')).contains(PAT)
    }

    def 'fetch should report a CLI that is not signed in without reading stdout'() {
        given:
        def cli = fakeCli("echo 'You are not signed in READ-SECRET'\nexit 3")

        when:
        source(cli).fetch()

        then:
        def e = thrown(StorageCredentialsException)
        e.message == FovusStorageCredentialsSource.NOT_SIGNED_IN
        !e.retryable
    }

    def 'fetch should ask for a CLI upgrade when the command does not exist'() {
        given:
        def cli = fakeCli("echo \"Error: No such command 'credentials'.\" >&2\nexit 2")

        when:
        source(cli).fetch()

        then:
        def e = thrown(StorageCredentialsException)
        e.message == FovusStorageCredentialsSource.UPGRADE_CLI
        !e.retryable
    }

    def 'fetch should give up at once when the CLI refuses to print credentials'() {
        given:
        def cli = fakeCli("echo x >> '${tempDir}/count.txt'\necho 'refused' >&2\nexit 2")

        when:
        source(cli).fetch()

        then:
        def e = thrown(StorageCredentialsException)
        e.message.startsWith('The Fovus CLI refused to print storage credentials')
        invocations() == 1
    }

    def 'fetch should retry a failing CLI three times and scrub the PAT from the error'() {
        given:
        def cli = fakeCli("echo x >> '${tempDir}/count.txt'\necho \"API error for ${PAT}\" >&2\nexit 1")

        when:
        source(cli).fetch()

        then:
        def e = thrown(StorageCredentialsException)
        e.retryable
        e.message.contains('[REDACTED]')
        !e.message.contains(PAT)
        invocations() == 3
    }

    def 'fetch should time out a CLI that hangs'() {
        given:
        def cli = fakeCli('sleep 30')

        when:
        source(cli, Duration.ofMillis(300)).fetch()

        then:
        def e = thrown(StorageCredentialsException)
        e.message.startsWith('Timed out after 300 ms')
    }

    def 'fetch should reject stdout over the size cap without echoing it'() {
        given:
        def cli = fakeCli('yes READ-SECRET | head -c 70000')

        when:
        source(cli).fetch()

        then:
        def e = thrown(StorageCredentialsException)
        e.message == StorageCredentials.CONTRACT_ERROR
    }

    def 'fetch should reject a stray line printed before the JSON'() {
        given:
        def cli = fakeCli("echo 'Authenticating...'\ncat '${credentialsFile()}'")

        when:
        source(cli).fetch()

        then:
        def e = thrown(StorageCredentialsException)
        e.message == StorageCredentials.CONTRACT_ERROR
    }

    def 'fetch should explain a CLI path that cannot run'() {
        given:
        def config = new FovusConfig([pipelineName: 'test-pipeline', cliPath: tempDir.resolve('missing').toString()])

        when:
        new FovusStorageCredentialsSource(config, PIPELINE_ID, Duration.ofSeconds(10), 0L).fetch()

        then:
        def e = thrown(StorageCredentialsException)
        e.message.startsWith('Unable to run the Fovus CLI at')
        !e.retryable
    }

    def 'no key and no PAT should ever reach the logs'() {
        given:
        def logs = captureLogs()
        def json = credentialsFile()

        when:
        source(fakeCli("cat '${json}'")).fetch()
        try {
            source(fakeCli("echo \"failed for ${PAT}\" >&2\nexit 1")).fetch()
        }
        catch (StorageCredentialsException ignored) {
        }

        then:
        def logged = logs.list*.formattedMessage.join('\n')
        SECRETS.every { !logged.contains(it) }
    }
}
```

- [ ] **Step 2: Run the test to see it fail**

Run: `./gradlew test --tests 'fovus.plugin.s3.FovusStorageCredentialsSourceTest'`
Expected: compilation FAILS with `unable to resolve class FovusStorageCredentialsSource`.

- [ ] **Step 3: Write the source**

Create `src/main/groovy/fovus/plugin/s3/FovusStorageCredentialsSource.groovy`:

```groovy
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
```

- [ ] **Step 4: Run the test to see it pass**

Run: `./gradlew test --tests 'fovus.plugin.s3.FovusStorageCredentialsSourceTest'`
Expected: PASS (11 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/fovus/plugin/s3/FovusStorageCredentialsSource.groovy src/test/groovy/fovus/plugin/s3/FovusStorageCredentialsSourceTest.groovy
git commit -m "Fetch direct mode storage credentials from the Fovus CLI without logging them"
```

---

### Task 3: Cache and refresh the credentials

**Files:**
- Create: `src/main/groovy/fovus/plugin/s3/RefreshingStorageCredentials.groovy`
- Create: `src/main/groovy/fovus/plugin/s3/UncheckedStorageCredentialsException.groovy`
- Test: `src/test/groovy/fovus/plugin/s3/RefreshingStorageCredentialsTest.groovy`

**Interfaces:**
- Consumes: `CredentialsFetcher`, `StorageCredentials`, `SessionKeys`, `StorageCredentialsException` (Task 1).
- Produces:
  - `class RefreshingStorageCredentials` — constructors `(CredentialsFetcher)` and `(CredentialsFetcher, Clock)`; `StorageCredentials initialize()`; `StorageCredentials get()`; `StorageCredentials forceRefresh()`; `AwsCredentialsProvider readProvider()`; `AwsCredentialsProvider writeProvider()`; constants `PREFETCH_BEFORE_EXPIRY` (10 min), `STALE_BEFORE_EXPIRY` (2 min), `FORCED_REFRESH_DEBOUNCE` (30 s).
  - `class UncheckedStorageCredentialsException extends RuntimeException` — wraps a `StorageCredentialsException` thrown inside an SDK credentials provider; `getCause()` is that exception.

- [ ] **Step 1: Write the failing test**

Create `src/test/groovy/fovus/plugin/s3/RefreshingStorageCredentialsTest.groovy`:

```groovy
package fovus.plugin.s3

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import spock.lang.Specification

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class RefreshingStorageCredentialsTest extends Specification {

    static final Instant NOW = Instant.parse('2026-09-30T12:00:00Z')

    static class MutableClock extends Clock {
        volatile Instant now

        MutableClock(Instant now) { this.now = now }

        @Override ZoneId getZone() { ZoneOffset.UTC }

        @Override Clock withZone(ZoneId zone) { this }

        @Override Instant instant() { now }
    }

    /** Hands out the queued responses in order, repeating the last; a queued exception is thrown. */
    static class QueueFetcher implements CredentialsFetcher {
        final List<Object> responses
        final long delayMillis
        final AtomicInteger calls = new AtomicInteger()

        QueueFetcher(List<Object> responses, long delayMillis = 0) {
            this.responses = responses
            this.delayMillis = delayMillis
        }

        @Override
        StorageCredentials fetch() throws StorageCredentialsException {
            final index = Math.min(calls.getAndIncrement(), responses.size() - 1)
            if (delayMillis) Thread.sleep(delayMillis)
            final response = responses[index]
            if (response instanceof StorageCredentialsException) throw (StorageCredentialsException) response
            return (StorageCredentials) response
        }
    }

    static StorageCredentials credentials(Instant expiration, String bucket = 'bucket') {
        final keys = new SessionKeys('key-id', 'secret', 'token', expiration)
        return new StorageCredentials(bucket, 'us-east-2', 'pipelines/p-1-user/', keys, keys)
    }

    MutableClock clock = new MutableClock(NOW)

    def 'get should reuse credentials until the prefetch window'() {
        given:
        def first = credentials(NOW.plus(Duration.ofHours(1)))
        def fetcher = new QueueFetcher([first])
        def cache = new RefreshingStorageCredentials(fetcher, clock)

        when:
        cache.initialize()
        clock.now = NOW.plus(Duration.ofMinutes(49))
        def result = cache.get()

        then:
        result.is(first)
        fetcher.calls.get() == 1
    }

    def 'get should refresh inside the ten-minute prefetch window'() {
        given:
        def second = credentials(NOW.plus(Duration.ofHours(2)))
        def fetcher = new QueueFetcher([credentials(NOW.plus(Duration.ofHours(1))), second])
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        cache.initialize()

        when:
        clock.now = NOW.plus(Duration.ofMinutes(51))

        then:
        cache.get().is(second)
        fetcher.calls.get() == 2
    }

    def 'a failed prefetch should keep the current credentials'() {
        given:
        def first = credentials(NOW.plus(Duration.ofHours(1)))
        def fetcher = new QueueFetcher([first, new StorageCredentialsException('CLI down', true)])
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        cache.initialize()

        when:
        clock.now = NOW.plus(Duration.ofMinutes(55))

        then:
        cache.get().is(first)
    }

    def 'a failed refresh in the last two minutes should fail the caller'() {
        given:
        def fetcher = new QueueFetcher([credentials(NOW.plus(Duration.ofHours(1))),
                                        new StorageCredentialsException('CLI down', true)])
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        cache.initialize()

        when:
        clock.now = NOW.plus(Duration.ofMinutes(59))
        cache.get()

        then:
        def e = thrown(StorageCredentialsException)
        e.message == 'Unable to refresh Fovus storage credentials: CLI down'
        e.retryable
    }

    def 'concurrent callers should share one fetch'() {
        given:
        def fetcher = new QueueFetcher([credentials(NOW.plus(Duration.ofHours(1)))], 100)
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        def pool = Executors.newFixedThreadPool(50)

        when:
        def futures = (1..50).collect { pool.submit({ cache.get() } as Callable<StorageCredentials>) }
        futures*.get()

        then:
        fetcher.calls.get() == 1

        cleanup:
        pool.shutdownNow()
    }

    def 'a refresh that moves the storage location should be rejected'() {
        given:
        def fetcher = new QueueFetcher([credentials(NOW.plus(Duration.ofHours(1))),
                                        credentials(NOW.plus(Duration.ofHours(2)), 'other-bucket')])
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        cache.initialize()

        when:
        clock.now = NOW.plus(Duration.ofMinutes(59))
        cache.get()

        then:
        def e = thrown(StorageCredentialsException)
        e.message.contains('storage location changed')
        !e.retryable
    }

    def 'forceRefresh should fetch again unless a fetch just happened'() {
        given:
        def fetcher = new QueueFetcher([credentials(NOW.plus(Duration.ofHours(1))),
                                        credentials(NOW.plus(Duration.ofHours(2)))])
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        cache.initialize()

        when: 'S3 rejects a token right after a fetch'
        cache.forceRefresh()

        then: 'the fetch that just happened is reused'
        fetcher.calls.get() == 1

        when: 'S3 rejects a token later'
        clock.now = NOW.plus(Duration.ofMinutes(5))
        cache.forceRefresh()

        then:
        fetcher.calls.get() == 2
    }

    def 'the providers should hand the SDK the read and write keys'() {
        given:
        def expiry = NOW.plus(Duration.ofHours(1))
        def stored = new StorageCredentials('b', 'us-east-2', 'pipelines/p-1-user/',
                                            new SessionKeys('read-id', 'read-secret', 'read-token', expiry),
                                            new SessionKeys('write-id', 'write-secret', 'write-token', expiry))
        def cache = new RefreshingStorageCredentials(new QueueFetcher([stored]), clock)

        expect:
        (cache.readProvider().resolveCredentials() as AwsSessionCredentials).accessKeyId() == 'read-id'
        (cache.writeProvider().resolveCredentials() as AwsSessionCredentials).sessionToken() == 'write-token'
    }

    def 'a provider should surface a credentials failure as an unchecked exception'() {
        given:
        def failure = new StorageCredentialsException('not signed in', false)
        def cache = new RefreshingStorageCredentials(new QueueFetcher([failure]), clock)

        when:
        cache.readProvider().resolveCredentials()

        then:
        def e = thrown(UncheckedStorageCredentialsException)
        e.cause.is(failure)
    }
}
```

- [ ] **Step 2: Run the test to see it fail**

Run: `./gradlew test --tests 'fovus.plugin.s3.RefreshingStorageCredentialsTest'`
Expected: compilation FAILS with `unable to resolve class RefreshingStorageCredentials`.

- [ ] **Step 3: Write the cache**

`src/main/groovy/fovus/plugin/s3/UncheckedStorageCredentialsException.groovy`:

```groovy
package fovus.plugin.s3

import groovy.transform.CompileStatic

/**
 * Carries a {@link StorageCredentialsException} out of an AWS SDK credentials provider, whose interface
 * cannot throw checked exceptions. {@link FovusS3Client} unwraps it again.
 */
@CompileStatic
class UncheckedStorageCredentialsException extends RuntimeException {
    UncheckedStorageCredentialsException(StorageCredentialsException cause) {
        super(cause.message, cause)
    }
}
```

`src/main/groovy/fovus/plugin/s3/RefreshingStorageCredentials.groovy`:

```groovy
package fovus.plugin.s3

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock

/**
 * Keeps the current direct-mode storage credentials in memory and fetches new ones before they expire.
 *
 * Fresh credentials are returned as they are. Within {@link #PREFETCH_BEFORE_EXPIRY} of expiry a caller
 * refreshes them, falling back to the current ones if that fails; within {@link #STALE_BEFORE_EXPIRY}
 * a caller must get fresh ones or fail. Only one thread fetches at a time; the others wait and reuse its
 * result.
 */
@Slf4j
@CompileStatic
class RefreshingStorageCredentials {

    static final Duration PREFETCH_BEFORE_EXPIRY = Duration.ofMinutes(10)
    static final Duration STALE_BEFORE_EXPIRY = Duration.ofMinutes(2)
    static final Duration FORCED_REFRESH_DEBOUNCE = Duration.ofSeconds(30)

    private final CredentialsFetcher fetcher
    private final Clock clock
    private final ReentrantLock refreshLock = new ReentrantLock()
    private volatile StorageCredentials current
    private volatile Instant lastFetchedAt

    RefreshingStorageCredentials(CredentialsFetcher fetcher) {
        this(fetcher, Clock.systemUTC())
    }

    RefreshingStorageCredentials(CredentialsFetcher fetcher, Clock clock) {
        this.fetcher = fetcher
        this.clock = clock
    }

    /** Fetch the first credentials now, so a bad sign-in, CLI or pipeline stops the run at start-up. */
    StorageCredentials initialize() throws StorageCredentialsException {
        return refresh(null)
    }

    StorageCredentials get() throws StorageCredentialsException {
        final snapshot = current
        if (snapshot == null) return refresh(null)

        final now = clock.instant()
        final expiration = snapshot.expiration
        if (now.isBefore(expiration.minus(PREFETCH_BEFORE_EXPIRY))) return snapshot

        if (now.isBefore(expiration.minus(STALE_BEFORE_EXPIRY))) {
            try {
                return refresh(snapshot)
            }
            catch (StorageCredentialsException e) {
                log.warn "[FOVUS] Could not refresh Fovus storage credentials; the current ones stay in use until they are close to expiry: ${e.message}"
                return snapshot
            }
        }

        try {
            return refresh(snapshot)
        }
        catch (StorageCredentialsException e) {
            throw new StorageCredentialsException("Unable to refresh Fovus storage credentials: ${e.message}".toString(), e.retryable)
        }
    }

    /** S3 rejected a token as expired: fetch new keys, unless a fetch finished moments ago. */
    StorageCredentials forceRefresh() throws StorageCredentialsException {
        final snapshot = current
        final fetchedAt = lastFetchedAt
        if (snapshot != null && fetchedAt != null && clock.instant().isBefore(fetchedAt.plus(FORCED_REFRESH_DEBOUNCE))) {
            return snapshot
        }
        return refresh(snapshot)
    }

    AwsCredentialsProvider readProvider() {
        return { -> resolve().read.toAwsCredentials() } as AwsCredentialsProvider
    }

    AwsCredentialsProvider writeProvider() {
        return { -> resolve().write.toAwsCredentials() } as AwsCredentialsProvider
    }

    private StorageCredentials resolve() {
        try {
            return get()
        }
        catch (StorageCredentialsException e) {
            throw new UncheckedStorageCredentialsException(e)
        }
    }

    private StorageCredentials refresh(StorageCredentials seen) throws StorageCredentialsException {
        refreshLock.lock()
        try {
            // Another thread refreshed while this one waited for the lock
            if (!current.is(seen)) return current

            final fresh = fetcher.fetch()
            final previous = current
            if (previous != null && (fresh.bucket != previous.bucket || fresh.region != previous.region
                    || fresh.prefix != previous.prefix)) {
                throw new StorageCredentialsException(
                        'Unexpected response from Fovus CLI (storage location changed during the run)', false)
            }
            current = fresh
            lastFetchedAt = clock.instant()
            log.debug "[FOVUS] Fetched Fovus storage credentials, expires at ${fresh.expiration}"
            return fresh
        }
        finally {
            refreshLock.unlock()
        }
    }
}
```

- [ ] **Step 4: Run the test to see it pass**

Run: `./gradlew test --tests 'fovus.plugin.s3.RefreshingStorageCredentialsTest'`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/fovus/plugin/s3/RefreshingStorageCredentials.groovy src/main/groovy/fovus/plugin/s3/UncheckedStorageCredentialsException.groovy src/test/groovy/fovus/plugin/s3/RefreshingStorageCredentialsTest.groovy
git commit -m "Cache direct mode storage credentials and refresh them before expiry"
```

---

### Task 4: `FovusS3Client` — guarded object operations and error mapping

**Files:**
- Create: `src/main/groovy/fovus/plugin/s3/S3Entry.groovy`
- Create: `src/main/groovy/fovus/plugin/s3/FovusS3Client.groovy`
- Test: `src/test/groovy/fovus/plugin/s3/FovusS3ClientTest.groovy`

**Interfaces:**
- Consumes: `RefreshingStorageCredentials` (`get()`, `forceRefresh()`, `readProvider()`, `writeProvider()`), `StorageCredentialsException`, `UncheckedStorageCredentialsException` (Tasks 1–3).
- Produces:
  - `final class S3Entry` — `S3Entry(String key, long size, Instant lastModified, boolean directory)`; getters `key size lastModified directory`.
  - `class FovusS3Client` — constructor `(S3Client reader, S3Client writer, String bucket, String prefix, RefreshingStorageCredentials credentials, int partSize = DEFAULT_PART_SIZE, int listPageSize = DEFAULT_LIST_PAGE_SIZE)`; `static FovusS3Client create(RefreshingStorageCredentials)`; getters `bucket prefix partSize`; `S3Entry head(String key)` (null when missing); `boolean hasChildren(String dirKey)`; `List<S3Entry> list(String dirKey)` (one level); `List<S3Entry> listAll(String dirKey)` (recursive); `InputStream getObject(String key)`; `InputStream getObject(String key, long fromByte)`; `void putObject(String key, byte[] bytes)`; `void putDirectoryMarker(String key)`; `void delete(String key)`; `String createMultipart(String key)`; `CompletedPart uploadPart(String key, String uploadId, int partNumber, byte[] bytes)`; `void completeMultipart(String key, String uploadId, List<CompletedPart> parts)`; `void abortMultipart(String key, String uploadId)`; constants `DEFAULT_PART_SIZE` (16 MiB), `MIN_PART_SIZE` (5 MiB), `DEFAULT_LIST_PAGE_SIZE` (1000), `MAX_ATTEMPTS` (10), `TRANSFER_THREADS` (4).
  - Errors: 404 → `NoSuchFileException`; 403 → `AccessDeniedException` with reason `Fovus storage credentials don't allow <op> on <key> (<read|write> token)`; others → `IOException("S3 <op> failed on <key>: <code> (HTTP <status>, request <id>)")`; a wrapped `StorageCredentialsException` is rethrown as itself.

- [ ] **Step 1: Write the failing test**

Create `src/test/groovy/fovus/plugin/s3/FovusS3ClientTest.groovy`:

```groovy
package fovus.plugin.s3

import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable
import spock.lang.Specification

import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import java.time.Instant

class FovusS3ClientTest extends Specification {

    static final String PREFIX = 'pipelines/p-1-user/'
    static final String KEY = PREFIX + 'fovus-work/ab/cdef/.exitcode'

    S3Client s3 = Mock()
    RefreshingStorageCredentials credentials = Mock()
    FovusS3Client client = new FovusS3Client(s3, s3, 'bucket', PREFIX, credentials)

    /** An S3 error whose body text must never reach a message. */
    static S3Exception s3Error(int status, String code) {
        return (S3Exception) S3Exception.builder()
                .statusCode(status)
                .requestId('req-1')
                .message('raw S3 body text SECRET-BODY')
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).errorMessage('raw S3 body text SECRET-BODY').build())
                .build()
    }

    def 'writes outside the pipeline prefix should be refused before calling S3'() {
        when:
        client.putObject('pipelines/p-2-user/x', new byte[0])

        then:
        def e = thrown(AccessDeniedException)
        e.reason == 'Refusing to write outside pipelines/p-1-user/'
        0 * s3._
    }

    def 'reads outside the pipeline prefix should look like missing files'() {
        when:
        def head = client.head('pipelines/p-2-user/x')

        then:
        head == null
        0 * s3._

        when:
        client.getObject('pipelines/p-2-user/x')

        then:
        thrown(NoSuchFileException)
        0 * s3._
    }

    def 'the pipeline folder itself should be readable'() {
        when:
        def found = client.hasChildren(PREFIX)

        then:
        1 * s3.listObjectsV2(_ as ListObjectsV2Request) >> ListObjectsV2Response.builder().keyCount(1).build()
        found
    }

    def 'head should return size and time, and null for a missing object'() {
        given:
        def modified = Instant.parse('2026-09-30T12:00:00Z')

        when:
        def found = client.head(KEY)
        def missing = client.head(PREFIX + 'missing')

        then:
        2 * s3.headObject(_ as HeadObjectRequest) >>> [HeadObjectResponse.builder().contentLength(1L).lastModified(modified).build()] >> { throw s3Error(404, 'NotFound') }
        found.size == 1L
        found.lastModified == modified
        !found.directory
        missing == null
    }

    def 'access denied should name the token and hide the S3 body'() {
        given:
        s3.putObject(_ as PutObjectRequest, _ as RequestBody) >> { throw s3Error(403, 'AccessDenied') }

        when:
        client.putObject(KEY, 'x'.bytes)

        then:
        def e = thrown(AccessDeniedException)
        e.reason == "Fovus storage credentials don't allow write on ${KEY} (write token)".toString()
        !e.message.contains('SECRET-BODY')
    }

    def 'other S3 errors should report code, status and request id only'() {
        given:
        s3.getObject(_ as GetObjectRequest) >> { throw s3Error(500, 'InternalError') }

        when:
        client.getObject(KEY)

        then:
        def e = thrown(IOException)
        e.message == "S3 read failed on ${KEY}: InternalError (HTTP 500, request req-1)".toString()
        e.cause == null
    }

    def 'an expired token should force one refresh and retry once'() {
        when:
        def found = client.head(KEY)

        then:
        1 * s3.headObject(_ as HeadObjectRequest) >> { throw s3Error(400, 'ExpiredToken') }
        1 * credentials.forceRefresh()
        1 * s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(7L).build()
        found.size == 7L
    }

    def 'a credentials failure inside the SDK should surface as that failure'() {
        given:
        def failure = new StorageCredentialsException('Unable to refresh Fovus storage credentials: CLI down', false)
        s3.getObject(_ as GetObjectRequest) >> {
            throw SdkClientException.create('Unable to load credentials', new UncheckedStorageCredentialsException(failure))
        }

        when:
        client.getObject(KEY)

        then:
        def e = thrown(StorageCredentialsException)
        e.is(failure)
    }

    def 'list should return one folder level, files and sub-folders'() {
        given:
        def modified = Instant.parse('2026-09-30T12:00:00Z')
        s3.listObjectsV2Paginator(_ as ListObjectsV2Request) >> { ListObjectsV2Request request -> new ListObjectsV2Iterable(s3, request) }
        s3.listObjectsV2(_ as ListObjectsV2Request) >> ListObjectsV2Response.builder()
                .contents(S3Object.builder().key(PREFIX + 'dir/').size(0L).lastModified(modified).build(),
                          S3Object.builder().key(PREFIX + 'dir/a.txt').size(5L).lastModified(modified).build())
                .commonPrefixes(CommonPrefix.builder().prefix(PREFIX + 'dir/sub/').build())
                .isTruncated(false)
                .build()

        when:
        def entries = client.list(PREFIX + 'dir/')

        then:
        entries*.key == [PREFIX + 'dir/', PREFIX + 'dir/a.txt', PREFIX + 'dir/sub/']
        entries*.directory == [true, false, true]
        entries[1].size == 5L
    }

    def 'a directory marker should be an empty object ending with a slash'() {
        when:
        client.putDirectoryMarker(PREFIX + 'ab/cdef')

        then:
        1 * s3.putObject({ PutObjectRequest r -> r.key() == PREFIX + 'ab/cdef/' && r.bucket() == 'bucket' }, _ as RequestBody)
    }
}
```

- [ ] **Step 2: Run the test to see it fail**

Run: `./gradlew test --tests 'fovus.plugin.s3.FovusS3ClientTest'`
Expected: compilation FAILS with `unable to resolve class FovusS3Client`.

- [ ] **Step 3: Write `S3Entry` and `FovusS3Client`**

`src/main/groovy/fovus/plugin/s3/S3Entry.groovy`:

```groovy
package fovus.plugin.s3

import groovy.transform.CompileStatic

import java.time.Instant

/** One object or folder prefix returned by {@link FovusS3Client}. */
@CompileStatic
final class S3Entry {

    final String key
    final long size
    final Instant lastModified
    final boolean directory

    S3Entry(String key, long size, Instant lastModified, boolean directory) {
        this.key = key
        this.size = size
        this.lastModified = lastModified
        this.directory = directory
    }
}
```

`src/main/groovy/fovus/plugin/s3/FovusS3Client.groovy`:

```groovy
package fovus.plugin.s3

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*

import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import java.time.Duration

/**
 * S3 access to the direct-mode work directory: {@code pipelines/<pid>/} in the user's Fovus bucket.
 *
 * Reads use the pipeline-scoped download token and writes the upload token. Every key is checked against
 * the pipeline prefix before any call; the upload token itself can write to all of {@code files/} and
 * {@code pipelines/}, so this guard is what keeps the plugin inside its own pipeline. S3 errors are
 * reported by code, HTTP status, request ID and key only -- never the S3 error body, which can echo the
 * access key ID.
 */
@Slf4j
@CompileStatic
class FovusS3Client {

    static final int DEFAULT_PART_SIZE = 16 * 1024 * 1024
    static final int MIN_PART_SIZE = 5 * 1024 * 1024
    static final int DEFAULT_LIST_PAGE_SIZE = 1000
    static final int MAX_ATTEMPTS = 10
    static final int TRANSFER_THREADS = 4
    static final Set<String> EXPIRED_TOKEN_CODES =
            ['ExpiredToken', 'ExpiredTokenException', 'InvalidToken', 'TokenRefreshRequired'] as Set<String>

    private final S3Client reader
    private final S3Client writer
    final String bucket
    final String prefix
    final int partSize
    private final int listPageSize
    private final RefreshingStorageCredentials credentials

    FovusS3Client(S3Client reader, S3Client writer, String bucket, String prefix, RefreshingStorageCredentials credentials,
                  int partSize = DEFAULT_PART_SIZE, int listPageSize = DEFAULT_LIST_PAGE_SIZE) {
        this.reader = reader
        this.writer = writer
        this.bucket = bucket
        this.prefix = prefix
        this.credentials = credentials
        this.partSize = partSize
        this.listPageSize = listPageSize
    }

    /** Clients for the bucket and region named by the (already initialized) credentials. */
    static FovusS3Client create(RefreshingStorageCredentials credentials) throws StorageCredentialsException {
        final first = credentials.get()
        return new FovusS3Client(buildClient(first.region, credentials.readProvider()),
                                 buildClient(first.region, credentials.writeProvider()),
                                 first.bucket, first.prefix, credentials)
    }

    private static S3Client buildClient(String region, AwsCredentialsProvider provider) {
        final retries = AwsRetryStrategy.standardRetryStrategy().toBuilder().maxAttempts(MAX_ATTEMPTS).build()
        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(provider)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(30))
                        .socketTimeout(Duration.ofMinutes(5)))
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .overrideConfiguration(ClientOverrideConfiguration.builder().retryStrategy(retries).build())
                .build()
    }

    // -- reads

    /** Size and time of an object, or {@code null} when it does not exist or is outside the pipeline. */
    S3Entry head(String key) throws IOException {
        if (!readable(key)) return null
        final request = HeadObjectRequest.builder().bucket(bucket).key(key).build()
        try {
            final response = call('read', key) { reader.headObject(request) }
            return new S3Entry(key, response.contentLength() ?: 0L, response.lastModified(), false)
        }
        catch (NoSuchFileException ignored) {
            return null
        }
    }

    /** True when at least one object starts with {@code dirKey}, which should end with {@code /}. */
    boolean hasChildren(String dirKey) throws IOException {
        if (!readable(dirKey)) return false
        final request = ListObjectsV2Request.builder().bucket(bucket).prefix(dirKey).maxKeys(1).build()
        final response = call('list', dirKey) { reader.listObjectsV2(request) }
        return (response.keyCount() ?: 0) > 0
    }

    /** The objects and sub-folder prefixes directly under {@code dirKey}, which should end with {@code /}. */
    List<S3Entry> list(String dirKey) throws IOException {
        return listing(dirKey, '/')
    }

    /** Every object under {@code dirKey}, at any depth. */
    List<S3Entry> listAll(String dirKey) throws IOException {
        return listing(dirKey, null)
    }

    InputStream getObject(String key) throws IOException {
        return getObject(key, 0L)
    }

    InputStream getObject(String key, long fromByte) throws IOException {
        if (!readable(key)) throw new NoSuchFileException(uri(key))
        final builder = GetObjectRequest.builder().bucket(bucket).key(key)
        if (fromByte > 0) builder.range("bytes=${fromByte}-".toString())
        final request = builder.build()
        return call('read', key) { reader.getObject(request) }
    }

    // -- writes

    void putObject(String key, byte[] bytes) throws IOException {
        writable(key)
        final request = PutObjectRequest.builder().bucket(bucket).key(key).build()
        call('write', key) { writer.putObject(request, RequestBody.fromBytes(bytes)) }
    }

    /** A zero-byte {@code <key>/} object, so an empty folder exists the way Nextflow expects. */
    void putDirectoryMarker(String key) throws IOException {
        putObject(key.endsWith('/') ? key : key + '/', new byte[0])
    }

    void delete(String key) throws IOException {
        writable(key)
        final request = DeleteObjectRequest.builder().bucket(bucket).key(key).build()
        call('delete', key) { writer.deleteObject(request) }
    }

    String createMultipart(String key) throws IOException {
        writable(key)
        final request = CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build()
        return call('write', key) { writer.createMultipartUpload(request).uploadId() }
    }

    CompletedPart uploadPart(String key, String uploadId, int partNumber, byte[] bytes) throws IOException {
        final request = UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber).build()
        final response = call('write', key) { writer.uploadPart(request, RequestBody.fromBytes(bytes)) }
        return CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build()
    }

    void completeMultipart(String key, String uploadId, List<CompletedPart> parts) throws IOException {
        final request = CompleteMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                .build()
        call('write', key) { writer.completeMultipartUpload(request) }
    }

    /** Best effort: an upload that cannot be aborted stays invisible until the bucket's lifecycle rule removes it. */
    void abortMultipart(String key, String uploadId) {
        try {
            writer.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).build())
        }
        catch (Exception e) {
            log.debug "[FOVUS] Could not abort the multipart upload of ${key}: ${e.class.simpleName}"
        }
    }

    // -- helpers

    private List<S3Entry> listing(String dirKey, String delimiter) throws IOException {
        if (!readable(dirKey)) return []
        final builder = ListObjectsV2Request.builder().bucket(bucket).prefix(dirKey).maxKeys(listPageSize)
        if (delimiter != null) builder.delimiter(delimiter)
        final request = builder.build()
        return call('list', dirKey) {
            final List<S3Entry> entries = []
            for (ListObjectsV2Response page : reader.listObjectsV2Paginator(request)) {
                for (S3Object object : page.contents()) {
                    entries.add(new S3Entry(object.key(), object.size() ?: 0L, object.lastModified(), object.key().endsWith('/')))
                }
                for (CommonPrefix common : page.commonPrefixes()) {
                    entries.add(new S3Entry(common.prefix(), 0L, null, true))
                }
            }
            return entries
        }
    }

    /**
     * Run one S3 call. An expired token forces one credentials refresh and one retry; every failure is
     * mapped by {@link #mapError}.
     */
    private <T> T call(String operation, String key, Closure<T> action) throws IOException {
        try {
            return action.call()
        }
        catch (S3Exception e) {
            if (credentials == null || !EXPIRED_TOKEN_CODES.contains(errorCode(e))) throw mapError(operation, key, e)
            credentials.forceRefresh()
            try {
                return action.call()
            }
            catch (Exception retryFailure) {
                throw mapError(operation, key, retryFailure)
            }
        }
        catch (Exception e) {
            throw mapError(operation, key, e)
        }
    }

    private static IOException mapError(String operation, String key, Exception error) {
        final credentialsFailure = credentialsFailure(error)
        if (credentialsFailure != null) return credentialsFailure
        if (error instanceof IOException) return (IOException) error
        if (error instanceof S3Exception) {
            final s3Error = (S3Exception) error
            if (s3Error.statusCode() == 404) return new NoSuchFileException(uri(key))
            if (s3Error.statusCode() == 403) {
                return new AccessDeniedException(uri(key), null,
                        "Fovus storage credentials don't allow ${operation} on ${key} (${tokenFor(operation)} token)".toString())
            }
            return new IOException("S3 ${operation} failed on ${key}: ${errorCode(s3Error)} (HTTP ${s3Error.statusCode()}, request ${s3Error.requestId()})".toString())
        }
        if (error instanceof SdkClientException) {
            return new IOException("S3 ${operation} failed on ${key}: ${error.message}".toString(), error)
        }
        return new IOException("S3 ${operation} failed on ${key}: ${error.class.simpleName}".toString(), error)
    }

    private static StorageCredentialsException credentialsFailure(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.cause) {
            if (cause instanceof StorageCredentialsException) return (StorageCredentialsException) cause
        }
        return null
    }

    private static String errorCode(S3Exception error) {
        return error.awsErrorDetails()?.errorCode() ?: ''
    }

    private static String tokenFor(String operation) {
        return operation in ['read', 'list'] ? 'read' : 'write'
    }

    static String uri(String key) {
        return "fovus:///fovus-storage/${key}".toString()
    }

    /** Inside this pipeline, including the pipeline folder itself. */
    private boolean readable(String key) {
        return key.startsWith(prefix) || key + '/' == prefix
    }

    private String writable(String key) throws AccessDeniedException {
        if (!key.startsWith(prefix)) {
            throw new AccessDeniedException(uri(key), null, "Refusing to write outside ${prefix}".toString())
        }
        return key
    }
}
```

- [ ] **Step 4: Run the test to see it pass**

Run: `./gradlew test --tests 'fovus.plugin.s3.FovusS3ClientTest'`
Expected: PASS (10 tests). If `AwsRetryStrategy` or `requestChecksumCalculation` does not resolve, the SDK is older than 2.30 — go back to Task 1's version.

- [ ] **Step 5: Commit**

```bash
git add src/main/groovy/fovus/plugin/s3/S3Entry.groovy src/main/groovy/fovus/plugin/s3/FovusS3Client.groovy src/test/groovy/fovus/plugin/s3/FovusS3ClientTest.groovy
git commit -m "Add a pipeline-scoped S3 client for direct mode"
```

---

### Task 5: Streams, transfers, and the MinIO test harness

Before this task, run Task 1 of the CLI plan (the token spike) and record the results in spec §12; this task relies on its answers for `AbortMultipartUpload` and `CopyObject`.

**Files:**
- Modify: `build.gradle`
- Create: `src/main/groovy/fovus/plugin/s3/S3MultipartOutputStream.groovy`
- Create: `src/main/groovy/fovus/plugin/s3/S3ReadChannel.groovy`
- Create: `src/main/groovy/fovus/plugin/s3/S3WriteChannel.groovy`
- Modify: `src/main/groovy/fovus/plugin/s3/FovusS3Client.groovy` (add transfer methods)
- Create (test helpers): `src/test/groovy/fovus/plugin/s3/MinioSupport.groovy`, `src/test/groovy/fovus/plugin/s3/CountingInterceptor.groovy`
- Test: `src/test/groovy/fovus/plugin/s3/S3TransferFailureTest.groovy` (unit), `src/test/groovy/fovus/plugin/s3/FovusS3ClientIT.groovy` (integration)

**Interfaces:**
- Consumes: `FovusS3Client` operations from Task 4.
- Produces:
  - `FovusS3Client`: `S3MultipartOutputStream newOutputStream(String key)`; `void uploadFile(Path file, String key)`; `void downloadFile(String key, Path target)`; `void copy(String sourceKey, String targetKey, long size)`; constant `MAX_COPY_OBJECT_SIZE` (5 GiB).
  - `class S3MultipartOutputStream extends OutputStream` — `close()` publishes (PutObject, or CompleteMultipartUpload); `void abort()` discards; a stream never closed publishes nothing.
  - `class S3ReadChannel implements SeekableByteChannel` — `S3ReadChannel(FovusS3Client client, String key, long size)`.
  - `class S3WriteChannel implements SeekableByteChannel` — `S3WriteChannel(OutputStream output)`.
  - Test helpers: `MinioSupport.start()`, `MinioSupport.s3Client(MinIOContainer, ExecutionInterceptor...)`, `MinioSupport.fovusClient(S3Client, int partSize = MIN_PART_SIZE, int listPageSize = DEFAULT_LIST_PAGE_SIZE)`, `MinioSupport.BUCKET`, `MinioSupport.PREFIX` (`pipelines/p-1-user/`); `CountingInterceptor.count(String requestClassName)`, `reset()`.

- [ ] **Step 1: Add Testcontainers and the `integrationTest` task**

In `build.gradle` `dependencies { … }` add:

```groovy
    testImplementation 'org.testcontainers:testcontainers:1.21.3'
    testImplementation 'org.testcontainers:minio:1.21.3'
```

Replace the existing `test { … }` block with:

```groovy
// Run tests from a scratch directory. The pipeline cache resolves `.fovus/pipeline_cache.json`
// against the working directory, so specs covering it would otherwise write into the source tree.
test {
    workingDir = layout.buildDirectory.dir('test-workdir').get().asFile
    doFirst { workingDir.mkdirs() }
    useJUnitPlatform {
        excludeTags 'integration'
    }
}

// Direct-mode S3 tests against a MinIO container. Needs Docker, so it is kept out of `check`.
tasks.register('integrationTest', Test) {
    description = 'Runs the direct-mode S3 tests against a MinIO container (needs Docker).'
    group = 'verification'
    testClassesDirs = sourceSets.test.output.classesDirs
    classpath = sourceSets.test.runtimeClasspath
    workingDir = layout.buildDirectory.dir('integration-test-workdir').get().asFile
    doFirst { workingDir.mkdirs() }
    useJUnitPlatform {
        includeTags 'integration'
    }
}
```

If Testcontainers later reports that the Docker client API version is too old, bump both artifacts to the newest `1.21.x`.

- [ ] **Step 2: Write the test helpers**

`src/test/groovy/fovus/plugin/s3/MinioSupport.groovy`:

```groovy
package fovus.plugin.s3

import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest

/** A MinIO container standing in for the user's Fovus bucket in the direct-mode S3 tests. */
class MinioSupport {

    static final String IMAGE = 'minio/minio:RELEASE.2023-09-04T19-57-37Z'
    static final String BUCKET = 'fovus-test-bucket'
    static final String PREFIX = 'pipelines/p-1-user/'

    static MinIOContainer start() {
        final minio = new MinIOContainer(IMAGE)
        minio.start()
        s3Client(minio).withCloseable { it.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()) }
        return minio
    }

    static S3Client s3Client(MinIOContainer minio, ExecutionInterceptor... interceptors) {
        return S3Client.builder()
                .endpointOverride(URI.create(minio.getS3URL()))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(minio.getUserName(), minio.getPassword())))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .executionInterceptors(interceptors as List<ExecutionInterceptor>)
                        .build())
                .build()
    }

    static FovusS3Client fovusClient(S3Client s3, int partSize = FovusS3Client.MIN_PART_SIZE,
                                     int listPageSize = FovusS3Client.DEFAULT_LIST_PAGE_SIZE) {
        return new FovusS3Client(s3, s3, BUCKET, PREFIX, null, partSize, listPageSize)
    }
}
```

`src/test/groovy/fovus/plugin/s3/CountingInterceptor.groovy`:

```groovy
package fovus.plugin.s3

import software.amazon.awssdk.core.interceptor.Context
import software.amazon.awssdk.core.interceptor.ExecutionAttributes
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** Counts S3 requests by type, e.g. {@code count('HeadObjectRequest')}. */
class CountingInterceptor implements ExecutionInterceptor {

    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>()

    @Override
    void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
        counts.computeIfAbsent(context.request().getClass().simpleName) { new AtomicInteger() }.incrementAndGet()
    }

    int count(String requestType) {
        return counts.get(requestType)?.get() ?: 0
    }

    void reset() {
        counts.clear()
    }
}
```

- [ ] **Step 3: Write the failing tests**

`src/test/groovy/fovus/plugin/s3/S3TransferFailureTest.groovy` (unit, no Docker):

```groovy
package fovus.plugin.s3

import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class S3TransferFailureTest extends Specification {

    static final String KEY = 'pipelines/p-1-user/fovus-work/ab/cdef/out.bin'

    @TempDir
    Path tempDir

    S3Client s3 = Mock()
    FovusS3Client client = new FovusS3Client(s3, s3, 'bucket', 'pipelines/p-1-user/', null, FovusS3Client.MIN_PART_SIZE)

    def 'a failed part upload should abort the upload and publish nothing'() {
        given:
        def out = client.newOutputStream(KEY)
        def part = new byte[FovusS3Client.MIN_PART_SIZE]

        when:
        out.write(part)
        try {
            out.write(part)
        }
        catch (IOException ignored) {
        }
        out.close()

        then:
        1 * s3.createMultipartUpload(_ as CreateMultipartUploadRequest) >> CreateMultipartUploadResponse.builder().uploadId('u-1').build()
        1 * s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> UploadPartResponse.builder().eTag('e1').build()
        1 * s3.uploadPart(_ as UploadPartRequest, _ as RequestBody) >> { throw FovusS3ClientTest.s3Error(500, 'InternalError') }
        1 * s3.abortMultipartUpload({ AbortMultipartUploadRequest r -> r.uploadId() == 'u-1' })
        0 * s3.completeMultipartUpload(_)
        0 * s3.putObject(_, _)
    }

    def 'a failed ranged download should leave no file behind'() {
        given:
        def target = tempDir.resolve('out.bin')
        s3.headObject(_ as HeadObjectRequest) >> HeadObjectResponse.builder().contentLength(3L * FovusS3Client.MIN_PART_SIZE).build()
        s3.getObjectAsBytes(_ as GetObjectRequest) >> { throw FovusS3ClientTest.s3Error(500, 'InternalError') }

        when:
        client.downloadFile(KEY, target)

        then:
        thrown(IOException)
        !Files.exists(target)
        Files.list(tempDir).withCloseable { it.count() } == 0
    }
}
```

`src/test/groovy/fovus/plugin/s3/FovusS3ClientIT.groovy` (integration):

```groovy
package fovus.plugin.s3

import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.services.s3.S3Client
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Tag
import spock.lang.TempDir

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

@Tag('integration')
class FovusS3ClientIT extends Specification {

    @Shared MinIOContainer minio
    @Shared S3Client s3
    @Shared CountingInterceptor requests = new CountingInterceptor()

    @TempDir
    Path tempDir

    FovusS3Client client
    String base

    def setupSpec() {
        minio = MinioSupport.start()
        s3 = MinioSupport.s3Client(minio, requests)
    }

    def cleanupSpec() {
        s3?.close()
        minio?.stop()
    }

    def setup() {
        client = MinioSupport.fovusClient(s3)
        base = "${MinioSupport.PREFIX}${UUID.randomUUID()}/".toString()
        requests.reset()
    }

    private static byte[] randomBytes(int size) {
        final bytes = new byte[size]
        new Random(42).nextBytes(bytes)
        return bytes
    }

    private byte[] read(String key) {
        return client.getObject(key).withCloseable { it.readAllBytes() }
    }

    def 'a small stream should become one PutObject'() {
        given:
        def key = base + 'small.txt'

        when:
        client.newOutputStream(key).withCloseable { it.write('hello'.bytes) }

        then:
        read(key) == 'hello'.bytes
        requests.count('PutObjectRequest') == 1
        requests.count('CreateMultipartUploadRequest') == 0
    }

    def 'a large stream should become a multipart upload'() {
        given:
        def key = base + 'large.bin'
        def data = randomBytes(3 * FovusS3Client.MIN_PART_SIZE + 17)

        when:
        client.newOutputStream(key).withCloseable { out ->
            for (int offset = 0; offset < data.length; offset += 1024 * 1024) {
                out.write(data, offset, Math.min(1024 * 1024, data.length - offset))
            }
        }

        then:
        read(key) == data
        requests.count('CreateMultipartUploadRequest') == 1
        requests.count('UploadPartRequest') == 4
    }

    def 'a stream that is never closed should leave no object'() {
        given:
        def key = base + 'interrupted.bin'
        def out = client.newOutputStream(key)

        when:
        out.write(randomBytes(2 * FovusS3Client.MIN_PART_SIZE + 1))

        then:
        client.head(key) == null

        cleanup:
        out.abort()
    }

    def 'files should upload and download whole, in parts when large'() {
        given:
        def small = Files.write(tempDir.resolve('small.txt'), 'hello'.bytes)
        def large = Files.write(tempDir.resolve('large.bin'), randomBytes(3 * FovusS3Client.MIN_PART_SIZE + 1))

        when:
        client.uploadFile(small, base + 'small.txt')
        client.uploadFile(large, base + 'large.bin')
        requests.reset()
        client.downloadFile(base + 'small.txt', tempDir.resolve('out/small.txt'))
        client.downloadFile(base + 'large.bin', tempDir.resolve('out/large.bin'))

        then:
        Files.readAllBytes(tempDir.resolve('out/small.txt')) == Files.readAllBytes(small)
        Files.readAllBytes(tempDir.resolve('out/large.bin')) == Files.readAllBytes(large)
        requests.count('GetObjectRequest') == 1 + 4
    }

    def 'copy should duplicate an object inside the pipeline'() {
        given:
        client.putObject(base + 'a.txt', 'a'.bytes)

        when:
        client.copy(base + 'a.txt', base + 'b.txt', 1L)

        then:
        read(base + 'b.txt') == 'a'.bytes
    }

    def 'a read channel should seek within an object'() {
        given:
        client.putObject(base + 'seek.txt', '0123456789'.bytes)
        def channel = new S3ReadChannel(client, base + 'seek.txt', 10L)
        def buffer = ByteBuffer.allocate(3)

        when:
        channel.position(4)
        channel.read(buffer)

        then:
        new String(buffer.array()) == '456'
        channel.position() == 7

        cleanup:
        channel.close()
    }
}
```

- [ ] **Step 4: Run the tests to see them fail**

Run: `./gradlew test --tests 'fovus.plugin.s3.S3TransferFailureTest'`
Expected: compilation FAILS with `Cannot find matching method ... newOutputStream`.

- [ ] **Step 5: Write the streams and transfers**

`src/main/groovy/fovus/plugin/s3/S3MultipartOutputStream.groovy`:

```groovy
package fovus.plugin.s3

import groovy.transform.CompileStatic
import software.amazon.awssdk.services.s3.model.CompletedPart

/**
 * Writes one object. Up to one part is buffered in memory; closing publishes it with a single PutObject,
 * larger streams become a multipart upload completed on close. Nothing is visible in storage until
 * {@link #close()} succeeds: a failed part aborts the upload, and a stream that is never closed (e.g. the
 * JVM is killed) leaves only an invisible, incomplete upload.
 */
@CompileStatic
class S3MultipartOutputStream extends OutputStream {

    private final FovusS3Client client
    private final String key
    private final List<CompletedPart> parts = []
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream(8192)
    private String uploadId
    private boolean closed

    S3MultipartOutputStream(FovusS3Client client, String key) {
        this.client = client
        this.key = key
    }

    @Override
    void write(int b) throws IOException {
        ensureOpen()
        buffer.write(b)
        if (buffer.size() >= client.partSize) flushPart()
    }

    @Override
    void write(byte[] bytes, int offset, int length) throws IOException {
        ensureOpen()
        int position = offset
        int remaining = length
        while (remaining > 0) {
            final int chunk = Math.min(remaining, client.partSize - buffer.size())
            buffer.write(bytes, position, chunk)
            position += chunk
            remaining -= chunk
            if (buffer.size() >= client.partSize) flushPart()
        }
    }

    @Override
    void close() throws IOException {
        if (closed) return
        closed = true
        try {
            if (uploadId == null) {
                client.putObject(key, buffer.toByteArray())
                return
            }
            if (buffer.size() > 0) uploadBufferedPart()
            client.completeMultipart(key, uploadId, parts)
        }
        catch (IOException e) {
            discard()
            throw e
        }
        finally {
            buffer = null
        }
    }

    /** Throw away everything written so far; nothing becomes visible in storage. */
    void abort() {
        if (closed) return
        closed = true
        discard()
        buffer = null
    }

    private void flushPart() throws IOException {
        try {
            uploadBufferedPart()
        }
        catch (IOException e) {
            abort()
            throw e
        }
    }

    private void uploadBufferedPart() throws IOException {
        if (uploadId == null) uploadId = client.createMultipart(key)
        parts.add(client.uploadPart(key, uploadId, parts.size() + 1, buffer.toByteArray()))
        buffer.reset()
    }

    private void discard() {
        if (uploadId != null) client.abortMultipart(key, uploadId)
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("The stream to ${key} is closed".toString())
    }
}
```

`src/main/groovy/fovus/plugin/s3/S3ReadChannel.groovy`:

```groovy
package fovus.plugin.s3

import groovy.transform.CompileStatic

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonWritableChannelException
import java.nio.channels.SeekableByteChannel

/** A read-only channel over one object; seeking reopens the object from the new offset with a ranged GET. */
@CompileStatic
class S3ReadChannel implements SeekableByteChannel {

    private final FovusS3Client client
    private final String key
    private final long objectSize
    private long currentOffset = 0
    private InputStream stream
    private long streamOffset = -1
    private boolean channelOpen = true

    S3ReadChannel(FovusS3Client client, String key, long size) {
        this.client = client
        this.key = key
        this.objectSize = size
    }

    @Override
    int read(ByteBuffer destination) throws IOException {
        ensureOpen()
        if (currentOffset >= objectSize) return -1
        if (stream == null || streamOffset != currentOffset) {
            stream?.close()
            stream = client.getObject(key, currentOffset)
            streamOffset = currentOffset
        }
        final chunk = new byte[Math.min(destination.remaining(), 64 * 1024)]
        final count = stream.read(chunk)
        if (count < 0) return -1
        destination.put(chunk, 0, count)
        currentOffset += count
        streamOffset += count
        return count
    }

    @Override
    int write(ByteBuffer source) {
        throw new NonWritableChannelException()
    }

    @Override
    long position() {
        return currentOffset
    }

    @Override
    SeekableByteChannel position(long newPosition) throws IOException {
        ensureOpen()
        currentOffset = newPosition
        return this
    }

    @Override
    long size() {
        return objectSize
    }

    @Override
    SeekableByteChannel truncate(long size) {
        throw new NonWritableChannelException()
    }

    @Override
    boolean isOpen() {
        return channelOpen
    }

    @Override
    void close() throws IOException {
        channelOpen = false
        stream?.close()
    }

    private void ensureOpen() throws ClosedChannelException {
        if (!channelOpen) throw new ClosedChannelException()
    }
}
```

`src/main/groovy/fovus/plugin/s3/S3WriteChannel.groovy`:

```groovy
package fovus.plugin.s3

import groovy.transform.CompileStatic

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonReadableChannelException
import java.nio.channels.SeekableByteChannel

/** A write-only, sequential channel over an {@link S3MultipartOutputStream}. */
@CompileStatic
class S3WriteChannel implements SeekableByteChannel {

    private final OutputStream output
    private long written = 0
    private boolean channelOpen = true

    S3WriteChannel(OutputStream output) {
        this.output = output
    }

    @Override
    int write(ByteBuffer source) throws IOException {
        if (!channelOpen) throw new ClosedChannelException()
        final count = source.remaining()
        final bytes = new byte[count]
        source.get(bytes)
        output.write(bytes)
        written += count
        return count
    }

    @Override
    int read(ByteBuffer destination) {
        throw new NonReadableChannelException()
    }

    @Override
    long position() {
        return written
    }

    @Override
    SeekableByteChannel position(long newPosition) {
        if (newPosition != written) throw new UnsupportedOperationException('Fovus storage objects are written sequentially')
        return this
    }

    @Override
    long size() {
        return written
    }

    @Override
    SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException('Fovus storage objects cannot be truncated')
    }

    @Override
    boolean isOpen() {
        return channelOpen
    }

    @Override
    void close() throws IOException {
        if (!channelOpen) return
        channelOpen = false
        output.close()
    }
}
```

In `FovusS3Client.groovy`, add these imports:

```groovy
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
```

add the constant next to the others:

```groovy
    static final long MAX_COPY_OBJECT_SIZE = 5L * 1024 * 1024 * 1024
```

and add this section before `// -- helpers`:

```groovy
    // -- transfers

    /** A stream that uploads to {@code key}; see {@link S3MultipartOutputStream}. */
    S3MultipartOutputStream newOutputStream(String key) throws IOException {
        return new S3MultipartOutputStream(this, writable(key))
    }

    /** Upload a local file: one PutObject up to one part, otherwise parts in parallel. */
    void uploadFile(Path file, String key) throws IOException {
        writable(key)
        final long size = Files.size(file)
        if (size <= partSize) {
            final request = PutObjectRequest.builder().bucket(bucket).key(key).build()
            call('write', key) { writer.putObject(request, RequestBody.fromFile(file)) }
            return
        }

        final uploadId = createMultipart(key)
        final pool = Executors.newFixedThreadPool(TRANSFER_THREADS)
        try {
            final List<Future<CompletedPart>> futures = []
            int partNumber = 1
            for (long offset = 0; offset < size; offset += partSize) {
                final long start = offset
                final int length = (int) Math.min((long) partSize, size - offset)
                final int number = partNumber++
                futures.add(pool.submit({ -> uploadPart(key, uploadId, number, readSlice(file, start, length)) } as Callable<CompletedPart>))
            }
            final List<CompletedPart> parts = []
            for (Future<CompletedPart> future : futures) parts.add(await(future))
            completeMultipart(key, uploadId, parts)
        }
        catch (IOException e) {
            abortMultipart(key, uploadId)
            throw e
        }
        finally {
            pool.shutdownNow()
        }
    }

    /**
     * Download an object to a local file: one GET up to one part, otherwise parallel ranged GETs. The data
     * goes to a temporary file next to {@code target}, moved into place only on success.
     */
    void downloadFile(String key, Path target) throws IOException {
        final entry = head(key)
        if (entry == null) throw new NoSuchFileException(uri(key))
        final directory = target.toAbsolutePath().parent
        Files.createDirectories(directory)
        final temp = Files.createTempFile(directory, ".${target.fileName}.".toString(), '.part')
        try {
            if (entry.size <= partSize) {
                getObject(key).withCloseable { InputStream input -> Files.copy(input, temp, StandardCopyOption.REPLACE_EXISTING) }
            }
            else {
                downloadRanges(key, entry.size, temp)
            }
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING)
        }
        catch (IOException e) {
            Files.deleteIfExists(temp)
            throw e
        }
    }

    /** Copy inside the pipeline: CopyObject when allowed, otherwise a streamed download and upload. */
    void copy(String sourceKey, String targetKey, long size) throws IOException {
        writable(targetKey)
        if (!readable(sourceKey)) throw new NoSuchFileException(uri(sourceKey))
        if (size <= MAX_COPY_OBJECT_SIZE) {
            final request = CopyObjectRequest.builder()
                    .sourceBucket(bucket).sourceKey(sourceKey)
                    .destinationBucket(bucket).destinationKey(targetKey)
                    .build()
            try {
                call('copy', targetKey) { writer.copyObject(request) }
                return
            }
            catch (AccessDeniedException ignored) {
                log.debug "[FOVUS] CopyObject is not allowed for ${sourceKey}; copying it through a download instead"
            }
        }
        final out = newOutputStream(targetKey)
        try {
            getObject(sourceKey).withCloseable { InputStream input -> input.transferTo(out) }
        }
        catch (IOException e) {
            out.abort()
            throw e
        }
        out.close()
    }

    private void downloadRanges(String key, long size, Path temp) throws IOException {
        final pool = Executors.newFixedThreadPool(TRANSFER_THREADS)
        try {
            FileChannel.open(temp, StandardOpenOption.WRITE).withCloseable { FileChannel channel ->
                final List<Future<Object>> futures = []
                for (long start = 0; start < size; start += partSize) {
                    final long from = start
                    final long to = Math.min(start + partSize, size) - 1
                    futures.add(pool.submit({ -> readRange(key, from, to, channel); return null } as Callable<Object>))
                }
                for (Future<Object> future : futures) await(future)
            }
        }
        finally {
            pool.shutdownNow()
        }
    }

    private void readRange(String key, long from, long to, FileChannel channel) throws IOException {
        final request = GetObjectRequest.builder().bucket(bucket).key(key).range("bytes=${from}-${to}".toString()).build()
        final bytes = call('read', key) { reader.getObjectAsBytes(request).asByteArray() }
        final buffer = ByteBuffer.wrap(bytes)
        long position = from
        while (buffer.hasRemaining()) position += channel.write(buffer, position)
    }

    private static byte[] readSlice(Path file, long start, int length) throws IOException {
        final bytes = new byte[length]
        final buffer = ByteBuffer.wrap(bytes)
        FileChannel.open(file, StandardOpenOption.READ).withCloseable { FileChannel channel ->
            while (buffer.hasRemaining()) {
                if (channel.read(buffer, start + buffer.position()) < 0) throw new EOFException("${file} ended early".toString())
            }
        }
        return bytes
    }

    private static <T> T await(Future<T> future) throws IOException {
        try {
            return future.get()
        }
        catch (ExecutionException e) {
            final cause = e.cause
            if (cause instanceof IOException) throw (IOException) cause
            throw new IOException(cause?.message ?: 'S3 transfer failed', cause)
        }
    }
```

- [ ] **Step 6: Run the unit tests to see them pass**

Run: `./gradlew test --tests 'fovus.plugin.s3.*'`
Expected: PASS, including `S3TransferFailureTest` (2 tests); the `FovusS3ClientIT` spec is skipped by the tag filter.

- [ ] **Step 7: Run the integration tests (Docker must be running)**

Run: `./gradlew integrationTest --tests 'fovus.plugin.s3.FovusS3ClientIT'`
Expected: PASS (6 tests). The first run pulls the MinIO image.

- [ ] **Step 8: Commit**

```bash
git add build.gradle src/main/groovy/fovus/plugin/s3 src/test/groovy/fovus/plugin/s3
git commit -m "Add S3 streams and transfers for direct mode, with MinIO integration tests"
```

---

### Task 6: The `pipelines` area of the `fovus://` filesystem

**Files:**
- Modify: `src/main/groovy/fovus/plugin/nio/FovusPath.java` (constructor check lines 94-97; add constant and method)
- Modify: `src/main/groovy/fovus/plugin/nio/FovusFileSystem.java`
- Replace: `src/main/groovy/fovus/plugin/nio/FovusFileSystemProvider.java` (full new content below)
- Create: `src/main/groovy/fovus/plugin/nio/PipelinesStorage.groovy`
- Create (test helper): `src/test/groovy/fovus/plugin/nio/PipelinesTestSupport.groovy`
- Test: `src/test/groovy/fovus/plugin/nio/FovusPipelinesAreaTest.groovy` (unit), `src/test/groovy/fovus/plugin/nio/PipelinesStorageIT.groovy` (integration)

**Interfaces:**
- Consumes: `FovusS3Client` (`head`, `hasChildren`, `list`, `listAll`, `getObject`, `newOutputStream`, `putDirectoryMarker`, `delete`, `copy`, `uploadFile`, `downloadFile`), `S3Entry`, `S3ReadChannel`, `S3WriteChannel` (Tasks 4–5).
- Produces:
  - `FovusPath.PIPELINES` (`"pipelines"`), `boolean FovusPath.isPipelinesAreaRoot()`.
  - `FovusFileSystem.attachS3Client(FovusS3Client client)`, `FovusFileSystem.NOT_ATTACHED_MESSAGE`, package-private `PipelinesStorage pipelinesStorage()`.
  - `class PipelinesStorage` (package `fovus.plugin.nio`) — NIO operations for `pipelines` paths: `newInputStream`, `newOutputStream(FovusPath, OpenOption...)`, `newByteChannel`, `newDirectoryStream`, `readAttributes`, `exists`, `createDirectory`, `delete`, `copy`, `move`, `upload(Path, FovusPath, CopyOption...)`, `download(FovusPath, Path, CopyOption...)`; `static String keyOf(FovusPath)`.
  - Test helper `PipelinesTestSupport.fileSystem(FovusS3Client client = null)` → a fresh provider's `pipelines` `FovusFileSystem`.

- [ ] **Step 1: Write the failing unit test**

`src/test/groovy/fovus/plugin/nio/PipelinesTestSupport.groovy`:

```groovy
package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client

class PipelinesTestSupport {

    static final URI AREA = URI.create('fovus:///fovus-storage/pipelines')

    /** A fresh provider's pipelines/ file system, with an S3 client attached when one is given. */
    static FovusFileSystem fileSystem(FovusS3Client client = null) {
        final fs = (FovusFileSystem) new FovusFileSystemProvider().newFileSystem(AREA, [:])
        if (client != null) fs.attachS3Client(client)
        return fs
    }
}
```

`src/test/groovy/fovus/plugin/nio/FovusPipelinesAreaTest.groovy`:

```groovy
package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.S3Entry
import spock.lang.Specification

import java.nio.file.AccessDeniedException
import java.nio.file.Files

class FovusPipelinesAreaTest extends Specification {

    def 'pipelines paths should parse and print as the compute node sees them'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()

        when:
        def path = (FovusPath) fs.getPath('/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef')

        then:
        path.fileType == 'pipelines'
        path.key == 'p-1-user/fovus-work/ab/cdef'
        path.toString() == '/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef'
        path.toUri().toString() == 'fovus:///fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef'
        path.parent.toString() == '/fovus-storage/pipelines/p-1-user/fovus-work/ab'
        !fs.isReadOnly()
    }

    def 'the area root should be the direct-mode work directory'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()

        expect:
        (fs.getPath('/fovus-storage/pipelines') as FovusPath).isPipelinesAreaRoot()
        (fs.getPath('/fovus-storage/pipelines/') as FovusPath).isPipelinesAreaRoot()
        !(fs.getPath('/fovus-storage/pipelines/p-1-user') as FovusPath).isPipelinesAreaRoot()
    }

    def 'Nextflow can create and check the work directory before any credentials exist'() {
        given:
        def root = PipelinesTestSupport.fileSystem().getPath('/fovus-storage/pipelines')

        when:
        Files.createDirectories(root)

        then:
        Files.exists(root)
        Files.isDirectory(root)
    }

    def 'any other access before the S3 client is attached should explain direct mode'() {
        given:
        def path = PipelinesTestSupport.fileSystem().getPath('/fovus-storage/pipelines/p-1-user/x')

        when:
        Files.readAllBytes(path)

        then:
        def e = thrown(IllegalStateException)
        e.message == FovusFileSystem.NOT_ATTACHED_MESSAGE
    }

    def 'the files area should stay read-only'() {
        given:
        def fs = new FovusFileSystemProvider().newFileSystem(URI.create('fovus:///fovus-storage/files'),
                                                             [pipelineName: 'test-pipeline'])

        expect:
        fs.isReadOnly()
    }

    def 'a delete the write token does not allow should only warn'() {
        given:
        def client = Stub(FovusS3Client) {
            head(_) >> new S3Entry('pipelines/p-1-user/x', 1L, null, false)
            delete(_) >> {
                throw new AccessDeniedException('fovus:///fovus-storage/pipelines/p-1-user/x', null,
                                                "Fovus storage credentials don't allow delete on pipelines/p-1-user/x (write token)")
            }
        }
        def path = PipelinesTestSupport.fileSystem(client).getPath('/fovus-storage/pipelines/p-1-user/x')

        when:
        Files.delete(path)

        then:
        noExceptionThrown()
    }
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `./gradlew test --tests 'fovus.plugin.nio.FovusPipelinesAreaTest'`
Expected: compilation FAILS (`attachS3Client` and `isPipelinesAreaRoot` do not exist).

- [ ] **Step 3: Teach `FovusPath` the `pipelines` area**

In `FovusPath.java`, add below `FOVUS_PATH_PREFIX`:

```java
    public static final String PIPELINES = "pipelines";
```

Replace the absolute-path check in the `FovusPath(FovusFileSystem, String, String...)` constructor:

```java
            Preconditions.checkArgument(parts.size() >= 2 &&
                            parts.get(1).equals("fovus-storage") &&
                            (parts.get(2).equals("jobs") || parts.get(2).equals("files") || parts.get(2).equals("shared")),
                    "Invalid Fovus file path. Path must start with fovus-storage prefix and followed by 'files' or 'jobs' or 'shared");
```

with:

```java
            Preconditions.checkArgument(parts.size() >= 3 &&
                            parts.get(1).equals("fovus-storage") &&
                            (parts.get(2).equals("jobs") || parts.get(2).equals("files") || parts.get(2).equals("shared")
                                    || parts.get(2).equals(PIPELINES)),
                    "Invalid Fovus file path. Path must start with fovus-storage prefix and followed by 'files', 'jobs', 'pipelines' or 'shared'");
```

Add after `toRemoteFilePath()`:

```java
    /**
     * @return true for {@code /fovus-storage/pipelines} itself: the work directory of direct mode
     */
    public boolean isPipelinesAreaRoot() {
        return PIPELINES.equals(fileType) && parts.isEmpty();
    }
```

- [ ] **Step 4: Give `FovusFileSystem` its S3 client**

In `FovusFileSystem.java`, add the import `import fovus.plugin.s3.FovusS3Client;`, then add below the `fileType` field:

```java
    public static final String NOT_ATTACHED_MESSAGE =
            "Fovus storage pipelines/ paths can only be read or written in direct mode, after the Fovus executor has started";

    /** Direct mode only: set by the executor once storage credentials exist. */
    private volatile PipelinesStorage pipelinesStorage;
```

Replace `isReadOnly()`:

```java
    @Override
    public boolean isReadOnly() {
        return !FovusPath.PIPELINES.equals(fileType);
    }
```

and add at the end of the class:

```java
    /** Direct mode: give the pipelines/ file system its S3 client once storage credentials exist. */
    public void attachS3Client(FovusS3Client client) {
        this.pipelinesStorage = new PipelinesStorage(client);
    }

    PipelinesStorage pipelinesStorage() {
        final PipelinesStorage storage = pipelinesStorage;
        if (storage == null) {
            throw new IllegalStateException(NOT_ATTACHED_MESSAGE);
        }
        return storage;
    }
```

- [ ] **Step 5: Write `PipelinesStorage`**

`src/main/groovy/fovus/plugin/nio/PipelinesStorage.groovy`:

```groovy
package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.S3Entry
import fovus.plugin.s3.S3ReadChannel
import fovus.plugin.s3.S3WriteChannel
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileHelper

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessDeniedException
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.NotDirectoryException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import java.time.Instant

/**
 * NIO operations for the {@code pipelines/} area of Fovus storage in direct mode, on top of
 * {@link FovusS3Client}. A path's S3 key is its {@link FovusPath#toRemoteFilePath()}, e.g.
 * {@code pipelines/<pid>/fovus-work/ab/cdef/.command.run}. Folders are key prefixes, with a zero-byte
 * {@code <key>/} marker once created. Deleting always succeeds, as for the rest of the provider.
 */
@Slf4j
@CompileStatic
class PipelinesStorage {

    private final FovusS3Client s3

    PipelinesStorage(FovusS3Client s3) {
        this.s3 = s3
    }

    static String keyOf(FovusPath path) {
        return path.toRemoteFilePath()
    }

    InputStream newInputStream(FovusPath path) throws IOException {
        return s3.getObject(keyOf(path))
    }

    OutputStream newOutputStream(FovusPath path, OpenOption... options) throws IOException {
        final opts = Arrays.asList(options)
        if (opts.contains(StandardOpenOption.APPEND)) {
            throw new UnsupportedOperationException('Appending to a file in Fovus storage is not supported')
        }
        if (opts.contains(StandardOpenOption.CREATE_NEW) && exists(path)) {
            throw new FileAlreadyExistsException(path.toString())
        }
        return s3.newOutputStream(keyOf(path))
    }

    SeekableByteChannel newByteChannel(FovusPath path, Set<? extends OpenOption> options) throws IOException {
        if (options.contains(StandardOpenOption.WRITE) || options.contains(StandardOpenOption.APPEND)) {
            return new S3WriteChannel(newOutputStream(path, options.toArray(new OpenOption[0])))
        }
        final attributes = readAttributes(path)
        if (attributes.isDirectory()) throw new IOException("${path} is a directory".toString())
        return new S3ReadChannel(s3, keyOf(path), attributes.size())
    }

    DirectoryStream<Path> newDirectoryStream(FovusPath dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        final dirKey = keyOf(dir) + '/'
        final entries = s3.list(dirKey)
        if (entries.isEmpty()) {
            if (s3.head(keyOf(dir)) != null) throw new NotDirectoryException(dir.toString())
            throw new NoSuchFileException(dir.toUri().toString())
        }

        final List<Path> children = []
        for (S3Entry entry : entries) {
            // skip the folder's own marker object
            if (entry.key == dirKey) continue
            final name = entry.key.substring(dirKey.length()).replaceFirst('/$', '')
            final child = (FovusPath) dir.resolve(name)
            // cache the listing's size and time so walking a folder needs no HeadObject per entry
            child.setFileMetadata(new FovusFileMetadata(
                    entry.directory ? child.getKey() + '/' : child.getKey(),
                    entry.lastModified == null ? null : Date.from(entry.lastModified),
                    null,
                    entry.size))
            if (filter == null || filter.accept(child)) children.add(child)
        }
        return new ListedDirectoryStream(children)
    }

    FovusFileAttributes readAttributes(FovusPath path) throws IOException {
        final cached = path.getFileMetadata()
        if (cached != null) {
            return cached.key.endsWith('/')
                    ? directory(cached.key)
                    : file(cached.key, cached.size, cached.lastModified?.toInstant())
        }

        final key = keyOf(path)
        final entry = s3.head(key)
        if (entry != null) return file(key, entry.size, entry.lastModified)
        if (s3.hasChildren(key + '/')) return directory(key + '/')
        throw new NoSuchFileException(path.toUri().toString())
    }

    boolean exists(FovusPath path) throws IOException {
        try {
            readAttributes(path)
            return true
        }
        catch (NoSuchFileException ignored) {
            return false
        }
    }

    void createDirectory(FovusPath dir) throws IOException {
        s3.putDirectoryMarker(keyOf(dir))
    }

    void delete(FovusPath path) throws IOException {
        final key = keyOf(path)
        try {
            if (s3.head(key) != null) s3.delete(key)
            // a folder: remove its marker, if it has one
            else s3.delete(key + '/')
        }
        catch (AccessDeniedException e) {
            log.warn "[FOVUS] ${e.reason ?: e.message} -- ${path} was left in place"
        }
    }

    void copy(FovusPath source, FovusPath target, CopyOption... options) throws IOException {
        if (!Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING) && exists(target)) {
            throw new FileAlreadyExistsException(target.toString())
        }
        final attributes = readAttributes(source)
        // Nextflow copies a folder's content itself, one file at a time
        if (attributes.isDirectory()) {
            createDirectory(target)
            return
        }
        s3.copy(keyOf(source), keyOf(target), attributes.size())
    }

    void move(FovusPath source, FovusPath target, CopyOption... options) throws IOException {
        if (readAttributes(source).isDirectory()) {
            throw new IOException("Moving a folder within Fovus storage is not supported: ${source}".toString())
        }
        copy(source, target, options)
        delete(source)
    }

    void upload(Path local, FovusPath target, CopyOption... options) throws IOException {
        if (!Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING) && exists(target)) {
            throw new FileAlreadyExistsException(target.toString())
        }
        if (!Files.isDirectory(local)) {
            s3.uploadFile(local, keyOf(target))
            return
        }
        final FovusS3Client client = s3
        Files.walkFileTree(local, new SimpleFileVisitor<Path>() {
            @Override
            FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                client.putDirectoryMarker(keyOf(within(target, local, dir)))
                return FileVisitResult.CONTINUE
            }

            @Override
            FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                client.uploadFile(file, keyOf(within(target, local, file)))
                return FileVisitResult.CONTINUE
            }
        })
    }

    void download(FovusPath source, Path local, CopyOption... options) throws IOException {
        if (Files.exists(local)) {
            if (!Arrays.asList(options).contains(StandardCopyOption.REPLACE_EXISTING)) {
                throw new FileAlreadyExistsException(local.toString())
            }
            FileHelper.deletePath(local)
        }
        if (!readAttributes(source).isDirectory()) {
            s3.downloadFile(keyOf(source), local)
            return
        }
        final dirKey = keyOf(source) + '/'
        Files.createDirectories(local)
        for (S3Entry entry : s3.listAll(dirKey)) {
            final relative = entry.key.substring(dirKey.length())
            if (relative.isEmpty()) continue
            final target = local.resolve(relative)
            if (entry.key.endsWith('/')) {
                Files.createDirectories(target)
                continue
            }
            Files.createDirectories(target.parent)
            s3.downloadFile(entry.key, target)
        }
    }

    private static FovusPath within(FovusPath target, Path localRoot, Path local) {
        final relative = localRoot.relativize(local).toString()
        return relative.isEmpty() ? target : (FovusPath) target.resolve(relative)
    }

    private static FovusFileAttributes directory(String key) {
        return new FovusFileAttributes(key, null, 0L, true, false)
    }

    private static FovusFileAttributes file(String key, long size, Instant lastModified) {
        return new FovusFileAttributes(key, lastModified == null ? null : FileTime.from(lastModified), size, false, true)
    }

    @CompileStatic
    private static class ListedDirectoryStream implements DirectoryStream<Path> {
        private final List<Path> children

        ListedDirectoryStream(List<Path> children) {
            this.children = children
        }

        @Override
        Iterator<Path> iterator() {
            return children.iterator()
        }

        @Override
        void close() {
        }
    }
}
```

- [ ] **Step 6: Route `pipelines` paths through `PipelinesStorage` in the provider**

Replace the whole content of `src/main/groovy/fovus/plugin/nio/FovusFileSystemProvider.java` with:

```java
/*
 * Copyright 2020-2022, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package fovus.plugin.nio;

import com.google.common.base.Preconditions;
import fovus.plugin.job.FovusJobClient;
import fovus.plugin.FovusConfig;
import fovus.plugin.util.FovusFileMetadataLookup;
import nextflow.extension.FilesEx;
import nextflow.file.CopyOptions;
import nextflow.file.FileHelper;
import nextflow.file.FileSystemTransferAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * File system provider for Fovus storage. The implementation was adapted from S3FileSystemProvider.
 *
 * Spec:
 * <p>
 * URI: fovus://fovus-storage/{fileType}/{filePath}
 * <p>
 * FileSystem roots: /fovus-storage/{fileType}/
 * <p>
 * Treatment of Fovus objects: - If a key ends in "/" it's considered a directory
 * *and* a regular file. Otherwise, it's just a regular file. - It is legal for
 * a key "xyz" and "xyz/" to exist at the same time. The latter is treated as a
 * directory. - If a file "a/b/c" exists but there's no "a" or "a/b/", these are
 * considered "implicit" directories. They can be listed, traversed and deleted.
 * <p>
 * Deviations from FileSystem provider API: - Deleting a file or directory
 * always succeeds, regardless of whether the file/directory existed before the
 * operation was issued i.e. Files.delete() and Files.deleteIfExists() are
 * equivalent.
 * <p>
 * The {@code files} and {@code jobs} areas are read-only and served through the Fovus CLI. The
 * {@code pipelines} area is the work directory of direct mode: it is read and written through
 * {@link PipelinesStorage} with the AWS S3 SDK, once the executor has attached an S3 client.
 */
public class FovusFileSystemProvider extends FileSystemProvider implements FileSystemTransferAware {

    private static final Logger log = LoggerFactory.getLogger(FovusFileSystemProvider.class);

    final Map<String, FovusFileSystem> fileSystems = new HashMap<>();

    private final FovusFileMetadataLookup fovusFileMetadataLookup = new FovusFileMetadataLookup();

    @Override
    public String getScheme() {
        return "fovus";
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        Preconditions.checkNotNull(uri, "uri is null");
        Preconditions.checkArgument(uri.getScheme().equals("fovus"), "uri scheme must be 'fovus': '%s'", uri);

        final String fileType = FovusPath.getFileTypeOfUri(uri);
        synchronized (fileSystems) {
            if (fileSystems.containsKey(fileType))
                throw new FileSystemAlreadyExistsException("Fovus filesystem already exists. Use getFileSystem() instead");
            // The pipelines area needs no CLI client: Nextflow creates it while parsing workDir, before the
            // pipeline or any credentials exist, and the executor attaches an S3 client later.
            final FovusFileSystem result = FovusPath.PIPELINES.equals(fileType)
                    ? new FovusFileSystem(this, null, uri)
                    : createFileSystem(uri, new FovusConfig(env));
            fileSystems.put(fileType, result);
            return result;
        }
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        final String fileType = FovusPath.getFileTypeOfUri(uri);

        final FileSystem fileSystem = this.fileSystems.get(fileType);

        if (fileSystem == null) {
            throw new FileSystemNotFoundException("Fovus filesystem not yet created. Use newFileSystem() instead");
        }

        return fileSystem;
    }

    /**
     * Deviation from spec: throws FileSystemNotFoundException if FileSystem
     * hasn't yet been initialized.
     *
     * In this case, initialize the file system with newFileSystem() within the Executor's getWorkdir or getStageDir.
     */
    @Override
    public Path getPath(URI uri) {
        Preconditions.checkArgument(uri.getScheme().equals(getScheme()), "URI scheme must be %s", getScheme());
        return getFileSystem(uri).getPath(uri.getPath());
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {

        Preconditions.checkArgument(dir instanceof FovusPath, "path must be an instance of %s", FovusPath.class.getName());
        final FovusPath fovusPath = (FovusPath) dir;
        if (isPipelines(fovusPath)) {
            return pipelines(fovusPath).newDirectoryStream(fovusPath, filter);
        }

        return new DirectoryStream<Path>() {
            @Override
            public void close() throws IOException {
                // nothing to do here
            }

            @Override
            public Iterator<Path> iterator() {
                return new FovusPathIterator(fovusPath.getKey() + "/", fovusPath);
            }
        };
    }


    @Override
    public boolean canUpload(Path source, Path target) {
        return FileSystems.getDefault().equals(source.getFileSystem()) && target instanceof FovusPath;
    }

    @Override
    public boolean canDownload(Path source, Path target) {
        return source instanceof FovusPath && FileSystems.getDefault().equals(target.getFileSystem());
    }

    @Override
    public void download(Path remoteFile, Path localDestination, CopyOption... options) throws IOException {
        final FovusPath source = (FovusPath) remoteFile;
        if (isPipelines(source)) {
            pipelines(source).download(source, localDestination, options);
            return;
        }

        final CopyOptions opts = CopyOptions.parse(options);
        // delete target if it exists and REPLACE_EXISTING is specified
        if (opts.replaceExisting()) {
            FileHelper.deletePath(localDestination);
        } else if (Files.exists(localDestination))
            throw new FileAlreadyExistsException(localDestination.toString());

        final Optional<FovusFileAttributes> attrs = readAttr1(source);
        final boolean isDir = attrs.isPresent() && attrs.get().isDirectory();
        final String type = isDir ? "directory" : "file";
        final FovusJobClient fovusJobClient = source.getFileSystem().getJobClient();
        log.debug("Fovus download {} from={} to={}", type, FilesEx.toUriString(source), localDestination);

        if (isDir) {
            fovusJobClient.downloadFile(source.getKey() + "/", localDestination.toAbsolutePath().toString(), source.getFileType());
        } else {
            // Need to use getParent because the download destination is expected to be a directory
            fovusJobClient.downloadFile(source.getKey(), localDestination.getParent().toAbsolutePath().toString(), source.getFileType());
        }
    }

    @Override
    public void upload(Path localFile, Path remoteDestination, CopyOption... options) throws IOException {
        if (isPipelines(remoteDestination)) {
            pipelines(remoteDestination).upload(localFile, (FovusPath) remoteDestination, options);
            return;
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. upload is not supported");
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        if (isPipelines(path)) {
            return pipelines(path).newInputStream((FovusPath) path);
        }
        return super.newInputStream(path, options);
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        if (isPipelines(path)) {
            return pipelines(path).newOutputStream((FovusPath) path, options);
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. newOutputStream is not supported");
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path,
                                              Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException {
        if (isPipelines(path)) {
            return pipelines(path).newByteChannel((FovusPath) path, options);
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. newByteChannel is not supported");
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs)
            throws IOException {
        // The direct-mode work directory itself: nothing to create, and no credentials exist yet
        if (isPipelinesAreaRoot(dir)) return;
        if (isPipelines(dir)) {
            pipelines(dir).createDirectory((FovusPath) dir);
            return;
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. createDirectory is not supported");
    }

    @Override
    public void delete(Path path) throws IOException {
        if (isPipelines(path)) {
            pipelines(path).delete((FovusPath) path);
            return;
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. delete is not supported");
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options)
            throws IOException {
        if (isPipelines(source) && isPipelines(target)) {
            pipelines(target).copy((FovusPath) source, (FovusPath) target, options);
            return;
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. copy is not supported");
    }


    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        if (isPipelines(source) && isPipelines(target)) {
            pipelines(target).move((FovusPath) source, (FovusPath) target, options);
            return;
        }
        throw new UnsupportedOperationException("Fovus Storage is read-only. move is not supported");
    }

    @Override
    public boolean isSameFile(Path path1, Path path2) throws IOException {
        return path1.isAbsolute() && path2.isAbsolute() && path1.equals(path2);
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        // TODO: When required, add permission check for shared file
        FovusPath fovusPath = (FovusPath) path;
        Preconditions.checkArgument(fovusPath.isAbsolute(),
                "path must be absolute: %s", fovusPath);
        if (isPipelines(fovusPath) && !fovusPath.isPipelinesAreaRoot()) {
            // throws NoSuchFileException when the path does not exist
            pipelines(fovusPath).readAttributes(fovusPath);
        }
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        Preconditions.checkArgument(path instanceof FovusPath,
                "path must be an instance of %s", FovusPath.class.getName());
        FovusPath fovusPath = (FovusPath) path;
        if (type.isAssignableFrom(BasicFileAttributeView.class)) {
            try {
                return (V) new FovusFileAttributesView(readAttributesOf(fovusPath));
            } catch (IOException e) {
                throw new RuntimeException("Unable read attributes for file: " + FilesEx.toUriString(fovusPath), e);
            }
        }
        throw new UnsupportedOperationException("Not a valid Fovus file system provider file attribute view: " + type.getName());
    }


    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        Preconditions.checkArgument(path instanceof FovusPath,
                "path must be an instance of %s", FovusPath.class.getName());
        FovusPath fovusPath = (FovusPath) path;

        if (type.isAssignableFrom(BasicFileAttributes.class)) {
            A attributes = (A) readAttributesOf(fovusPath);
            log.trace("+++ Attributes for path {}: {}", path, attributes);
            return attributes;
        }
        // not support attribute class
        throw new UnsupportedOperationException(format("only %s supported", BasicFileAttributes.class));
    }

    private FovusFileAttributes readAttributesOf(FovusPath fovusPath) throws IOException {
        if (fovusPath.isPipelinesAreaRoot()) {
            return new FovusFileAttributes(FovusPath.PIPELINES + "/", null, 0, true, false);
        }
        if (isPipelines(fovusPath)) {
            return pipelines(fovusPath).readAttributes(fovusPath);
        }
        return "".equals(fovusPath.getKey())
                ? new FovusFileAttributes("/", null, 0, true, false)
                // read the target path attributes
                : readAttr0(fovusPath);
    }

    private Optional<FovusFileAttributes> readAttr1(FovusPath fovusPath) throws IOException {
        try {
            return Optional.of(readAttr0(fovusPath));
        } catch (NoSuchFileException e) {
            return Optional.<FovusFileAttributes>empty();
        }
    }

    private FovusFileAttributes readAttr0(FovusPath fovusPath) throws IOException {
        FovusFileMetadata fileMetadata = fovusFileMetadataLookup.lookup(fovusPath);

        // parse the data to BasicFileAttributes.
        FileTime lastModifiedTime = null;
        if (fileMetadata.getLastModified() != null) {
            lastModifiedTime = FileTime.from(fileMetadata.getLastModified().getTime(), TimeUnit.MILLISECONDS);
        }

        long size = fileMetadata.getSize();
        boolean directory = false;
        boolean regularFile = false;
        String key = fileMetadata.getKey();
        // Check if this is a directory on Fovus Storage and the key explicitly exists (i.e, an empty directory object was created)
        if (fileMetadata.getKey().equals(fovusPath.getKey() + "/") && fileMetadata.getKey().endsWith("/")) {
            directory = true;
        }
        // Here it is a directory, but the key doest not explicitly exist
        else if ((!fileMetadata.getKey().equals(fovusPath.getKey()) || "".equals(fovusPath.getKey())) && fileMetadata.getKey().startsWith(fovusPath.getKey())) {
            directory = true;
            // no metadata, we fake one
            size = 0;
            // delete extra part
            key = fovusPath.getKey() + "/";
        }
        // is a file:
        else {
            regularFile = true;
        }

        return new FovusFileAttributes(key, lastModifiedTime, size, directory, regularFile);
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value,
                             LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    protected FovusFileSystem createFileSystem(URI uri, FovusConfig fovusConfig) {
        FovusJobClient fovusJobClient = new FovusJobClient(fovusConfig);
        return new FovusFileSystem(this, fovusJobClient, uri);
    }


    /**
     * check that the paths exists or not
     *
     * @param path FovusFovusPath
     * @return true if exists
     */
    @Override
    public boolean exists(Path path, LinkOption... options) {
        if (path instanceof FovusPath fovusPath) { // Java 16+ pattern matching for instanceof
            if (fovusPath.isPipelinesAreaRoot()) {
                return true;
            }
            if (isPipelines(fovusPath)) {
                try {
                    return pipelines(fovusPath).exists(fovusPath);
                } catch (IOException e) {
                    return false;
                }
            }
            try {
                fovusFileMetadataLookup.lookup(fovusPath);
                return true;
            } catch (NoSuchFileException e) { // <-- more specific exception preferred
                return false;
            } catch (IOException e) { // fallback if lookup does I/O
                return false;
            }
        }
        return super.exists(path, options); // no else needed — early return above
    }

    private static boolean isPipelines(Path path) {
        return path instanceof FovusPath && FovusPath.PIPELINES.equals(((FovusPath) path).getFileType());
    }

    private static boolean isPipelinesAreaRoot(Path path) {
        return path instanceof FovusPath && ((FovusPath) path).isPipelinesAreaRoot();
    }

    private static PipelinesStorage pipelines(Path path) {
        return ((FovusPath) path).getFileSystem().pipelinesStorage();
    }
}
```

- [ ] **Step 7: Run the unit test to see it pass**

Run: `./gradlew test --tests 'fovus.plugin.nio.*'`
Expected: PASS (6 tests).

- [ ] **Step 8: Write the integration test**

`src/test/groovy/fovus/plugin/nio/PipelinesStorageIT.groovy`:

```groovy
package fovus.plugin.nio

import fovus.plugin.s3.CountingInterceptor
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.MinioSupport
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.services.s3.S3Client
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Tag
import spock.lang.TempDir

import java.nio.file.AccessDeniedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.stream.Collectors

@Tag('integration')
class PipelinesStorageIT extends Specification {

    @Shared MinIOContainer minio
    @Shared S3Client s3
    @Shared CountingInterceptor requests = new CountingInterceptor()

    @TempDir
    Path tempDir

    FovusPath dir

    def setupSpec() {
        minio = MinioSupport.start()
        s3 = MinioSupport.s3Client(minio, requests)
    }

    def cleanupSpec() {
        s3?.close()
        minio?.stop()
    }

    def setup() {
        // a page size of 2 makes every listing below span several pages
        final fs = PipelinesTestSupport.fileSystem(MinioSupport.fovusClient(s3, FovusS3Client.MIN_PART_SIZE, 2))
        dir = (FovusPath) fs.getPath("/fovus-storage/pipelines/p-1-user/${UUID.randomUUID()}")
        Files.createDirectories(dir)
        requests.reset()
    }

    private static List<String> names(Path folder) {
        return Files.list(folder).withCloseable { stream ->
            stream.map { Path p -> p.fileName.toString() }.sorted().collect(Collectors.toList())
        }
    }

    def 'a file written the way Nextflow writes .command.run should read back'() {
        given:
        def file = dir.resolve('.command.run')

        when:
        Files.newBufferedWriter(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
                .withCloseable { it.write('#!/bin/bash\necho hi\n') }

        then:
        file.text == '#!/bin/bash\necho hi\n'
        Files.size(file) == 21
        Files.isRegularFile(file)
    }

    def 'a created folder should exist before anything is written into it'() {
        given:
        def folder = dir.resolve('ab/cdef')
        assert !Files.exists(folder)

        when:
        Files.createDirectories(folder)

        then:
        Files.exists(folder)
        Files.isDirectory(folder)
        names(folder) == []
    }

    def 'listing should span pages and report files and sub-folders'() {
        given:
        (1..5).each { Files.writeString(dir.resolve("file${it}.txt"), "content ${it}") }
        Files.writeString(dir.resolve('sub/inner.txt'), 'inner')

        expect:
        names(dir) == ['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt', 'file5.txt', 'sub']
        Files.isDirectory(dir.resolve('sub'))
    }

    def 'a glob walk like output collection should not look up each file'() {
        given:
        ['a.txt', 'b.txt', 'c.log', 'nested/d.txt', 'nested/deeper/e.txt'].each { Files.writeString(dir.resolve(it), it) }
        requests.reset()
        def found = []

        when:
        FileHelper.visitFiles([type: 'file'], dir, '**.txt') { Path p -> found << dir.relativize(p).toString() }

        then:
        found.sort() == ['a.txt', 'b.txt', 'nested/d.txt', 'nested/deeper/e.txt']
        requests.count('HeadObjectRequest') <= 1
    }

    def 'names with spaces, symbols and non-ASCII letters should round-trip'() {
        given:
        def fileNames = ['my file (1).txt', 'a#b+c%d=e.txt', 'résumé.txt']

        when:
        fileNames.each { Files.writeString(dir.resolve(it), it) }

        then:
        names(dir) == fileNames.sort(false)
        fileNames.every { dir.resolve(it).text == it }
    }

    def 'an empty file should be a regular file of size 0'() {
        given:
        def empty = dir.resolve('.command.err')

        when:
        Files.write(empty, new byte[0])

        then:
        Files.exists(empty)
        Files.isRegularFile(empty)
        !Files.isDirectory(empty)
        Files.size(empty) == 0
        empty.text == ''
    }

    def 'names that share a prefix should not be confused'() {
        given:
        Files.writeString(dir.resolve('out.txt.bak'), 'backup')
        Files.writeString(dir.resolve('sample_2/x.txt'), 'x')
        Files.writeString(dir.resolve('sample/y.txt'), 'y')

        expect:
        !Files.exists(dir.resolve('out.txt'))
        names(dir.resolve('sample')) == ['y.txt']
    }

    def 'writes outside this pipeline should be refused and reads should find nothing'() {
        given:
        def other = dir.fileSystem.getPath('/fovus-storage/pipelines/p-2-user/x.txt')

        when:
        Files.writeString(other, 'nope')

        then:
        thrown(AccessDeniedException)
        !Files.exists(other)
    }

    def 'delete should remove files and deletePath should remove folders'() {
        given:
        Files.writeString(dir.resolve('a.txt'), 'a')
        Files.writeString(dir.resolve('sub/b.txt'), 'b')

        when:
        Files.delete(dir.resolve('a.txt'))
        FileHelper.deletePath(dir.resolve('sub'))

        then:
        !Files.exists(dir.resolve('a.txt'))
        !Files.exists(dir.resolve('sub'))
    }

    def 'copy and move should work inside the pipeline'() {
        given:
        Files.writeString(dir.resolve('a.txt'), 'a')

        when:
        Files.copy(dir.resolve('a.txt'), dir.resolve('b.txt'))
        Files.move(dir.resolve('b.txt'), dir.resolve('c.txt'))

        then:
        dir.resolve('a.txt').text == 'a'
        !Files.exists(dir.resolve('b.txt'))
        dir.resolve('c.txt').text == 'a'
    }

    def 'local files and folders should upload and download the way Nextflow copies them'() {
        given:
        def input = Files.writeString(tempDir.resolve('input.txt'), 'hello')
        def bin = Files.createDirectories(tempDir.resolve('bin/lib'))
        Files.writeString(tempDir.resolve('bin/tool.sh'), '#!/bin/bash')
        Files.writeString(bin.resolve('helper.py'), 'x')
        Files.createDirectories(dir.resolve('tmp'))

        when: 'an input is staged, the bin folder is uploaded and a folder is published locally'
        FileHelper.copyPath(input, dir.resolve('stage/input.txt'))
        FilesEx.copyTo(tempDir.resolve('bin'), dir.resolve('tmp'))
        FileHelper.copyPath(dir.resolve('tmp/bin'), tempDir.resolve('published'))

        then:
        Files.size(dir.resolve('stage/input.txt')) == Files.size(input)
        dir.resolve('tmp/bin/tool.sh').text == '#!/bin/bash'
        tempDir.resolve('published/tool.sh').text == '#!/bin/bash'
        tempDir.resolve('published/lib/helper.py').text == 'x'
    }
}
```

- [ ] **Step 9: Run the integration test to see it pass**

Run: `./gradlew integrationTest --tests 'fovus.plugin.nio.PipelinesStorageIT'`
Expected: PASS (11 tests).

- [ ] **Step 10: Run everything and commit**

Run: `./gradlew test integrationTest`
Expected: all PASS; the existing specs (`FovusUtilTest`, `FovusScriptLauncherTest`, …) are unchanged.

```bash
git add src/main/groovy/fovus/plugin/nio src/test/groovy/fovus/plugin/nio
git commit -m "Make the pipelines area of the fovus:// filesystem read and write S3 in direct mode"
```

---

### Task 7: Choose mount or direct mode from `workDir`

**Files:**
- Create: `src/main/groovy/fovus/plugin/storage/WorkDirStorage.groovy`
- Create: `src/main/groovy/fovus/plugin/storage/MountedWorkDirStorage.groovy`
- Create: `src/main/groovy/fovus/plugin/storage/DirectWorkDirStorage.groovy`
- Create: `src/main/groovy/fovus/plugin/storage/S3Connector.groovy`
- Create: `src/main/groovy/fovus/plugin/storage/CliS3Connector.groovy`
- Create: `src/main/groovy/fovus/plugin/storage/WorkDirStorageFactory.groovy`
- Replace: `src/main/groovy/fovus/plugin/FovusExecutor.groovy` (full new content below)
- Test: `src/test/groovy/fovus/plugin/storage/WorkDirStorageFactoryTest.groovy`, `src/test/groovy/fovus/plugin/storage/MountedWorkDirStorageTest.groovy`, `src/test/groovy/fovus/plugin/storage/DirectWorkDirStorageTest.groovy`, `src/test/groovy/fovus/plugin/FovusExecutorTest.groovy`

**Interfaces:**
- Consumes: `FovusStorageClient.validateOrMountFovusStorage(Path)` (existing); `FovusStorageCredentialsSource`, `RefreshingStorageCredentials`, `FovusS3Client.create` (Tasks 2–4); `FovusPath.isPipelinesAreaRoot()`, `FovusFileSystem.attachS3Client` (Task 6).
- Produces:
  - `interface WorkDirStorage { void prepare(String pipelineId); boolean isForeignFile(Path path); Path remotePath(Path path) }`.
  - `MountedWorkDirStorage(FovusStorageClient storageClient, Path sessionWorkDir)`.
  - `DirectWorkDirStorage(FovusPath workDir, S3Connector connector)`.
  - `interface S3Connector { FovusS3Client connect(String pipelineId) throws IOException }`; `CliS3Connector(FovusConfig config)`.
  - `WorkDirStorageFactory.create(Path workDir, boolean isHostedMode, FovusConfig config)`; `WorkDirStorageFactory.DIRECT_MODE_WORK_DIR`.
  - `FovusExecutor.workDirStorage` (protected field); `getRemotePath` and `isForeignFile` delegate to it.

- [ ] **Step 1: Write the failing tests**

`src/test/groovy/fovus/plugin/storage/WorkDirStorageFactoryTest.groovy`:

```groovy
package fovus.plugin.storage

import fovus.plugin.FovusConfig
import fovus.plugin.nio.FovusFileSystemProvider
import fovus.plugin.nio.PipelinesTestSupport
import fovus.plugin.util.FovusPathFactory
import nextflow.exception.AbortOperationException
import nextflow.file.FileHelper
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.FileSystems
import java.nio.file.Path

class WorkDirStorageFactoryTest extends Specification {

    static final FovusConfig CONFIG = new FovusConfig([pipelineName: 'test-pipeline'])

    @TempDir
    Path tempDir

    def setupSpec() {
        FileHelper.getOrInstallProvider(FovusFileSystemProvider)
    }

    def 'a local workDir ending in pipelines should select mount mode'() {
        expect:
        WorkDirStorageFactory.create(tempDir.resolve('mnt/pipelines'), false, CONFIG) instanceof MountedWorkDirStorage
    }

    def 'a local workDir not ending in pipelines should be rejected as today'() {
        when:
        WorkDirStorageFactory.create(tempDir.resolve('work'), false, CONFIG)

        then:
        def e = thrown(AbortOperationException)
        e.message.startsWith('[FOVUS] Working directory must end with pipelines.')
    }

    def 'the triple-slash URI Nextflow resolves should select direct mode'() {
        given:
        def workDir = FileHelper.asPath(URI.create(uri))

        expect:
        WorkDirStorageFactory.create(workDir, false, CONFIG) instanceof DirectWorkDirStorage

        where:
        uri << ['fovus:///fovus-storage/pipelines', 'fovus:///fovus-storage/pipelines/']
    }

    def 'the two-slash spelling users type after -w should select direct mode'() {
        given:
        def workDir = new FovusPathFactory().parseUri(spelling)

        expect:
        WorkDirStorageFactory.create(workDir, false, CONFIG) instanceof DirectWorkDirStorage

        where:
        spelling << ['fovus://fovus-storage/pipelines', 'fovus://fovus-storage/pipelines/']
    }

    def 'a fovus workDir other than the pipelines area should be rejected'() {
        given:
        def workDir = PipelinesTestSupport.fileSystem().getPath('/fovus-storage/pipelines/p-1-user')

        when:
        WorkDirStorageFactory.create(workDir, false, CONFIG)

        then:
        def e = thrown(AbortOperationException)
        e.message.startsWith('[FOVUS] In direct mode, workDir must be fovus:///fovus-storage/pipelines.')
    }

    def 'direct mode should be refused on a Fovus-hosted run'() {
        given:
        def workDir = PipelinesTestSupport.fileSystem().getPath('/fovus-storage/pipelines')

        when:
        WorkDirStorageFactory.create(workDir, true, CONFIG)

        then:
        def e = thrown(AbortOperationException)
        e.message.startsWith('[FOVUS] Direct mode (a fovus:// workDir) is only for pipelines launched on your own machine')
    }

    def 'any other workDir scheme should be rejected'() {
        given:
        def zip = FileSystems.newFileSystem(tempDir.resolve('work.zip'), [create: 'true'])

        when:
        WorkDirStorageFactory.create(zip.getPath('/pipelines'), false, CONFIG)

        then:
        def e = thrown(AbortOperationException)
        e.message.startsWith('[FOVUS] The Fovus executor needs workDir to be a Fovus storage mount')

        cleanup:
        zip.close()
    }
}
```

`src/test/groovy/fovus/plugin/storage/MountedWorkDirStorageTest.groovy`:

```groovy
package fovus.plugin.storage

import fovus.plugin.nio.PipelinesTestSupport
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class MountedWorkDirStorageTest extends Specification {

    @TempDir
    Path tempDir

    def 'prepare should mount the parent of the work directory'() {
        given:
        def client = Mock(FovusStorageClient)
        def storage = new MountedWorkDirStorage(client, tempDir.resolve('mnt/pipelines'))

        when:
        storage.prepare('p-1-user')

        then:
        1 * client.validateOrMountFovusStorage(tempDir.resolve('mnt'))
    }

    def 'paths should be rewritten to /fovus-storage as today'() {
        given:
        def storage = new MountedWorkDirStorage(Mock(FovusStorageClient), tempDir.resolve('mnt/pipelines'))

        expect:
        storage.remotePath(tempDir.resolve('mnt/pipelines/p-1/fovus-work/ab/cdef/.command.run')) ==
                Path.of('/fovus-storage/pipelines/p-1/fovus-work/ab/cdef/.command.run')
    }

    def 'files outside the mount or on another file system should be foreign'() {
        given:
        def storage = new MountedWorkDirStorage(Mock(FovusStorageClient), tempDir.resolve('mnt/pipelines'))

        expect:
        !storage.isForeignFile(tempDir.resolve('mnt/files/input.txt'))
        storage.isForeignFile(tempDir.resolve('elsewhere/input.txt'))
        storage.isForeignFile(PipelinesTestSupport.fileSystem().getPath('/fovus-storage/files/input.txt'))
    }
}
```

`src/test/groovy/fovus/plugin/storage/DirectWorkDirStorageTest.groovy`:

```groovy
package fovus.plugin.storage

import fovus.plugin.nio.FovusPath
import fovus.plugin.nio.PipelinesTestSupport
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusStorageCredentialsSource
import fovus.plugin.s3.S3Entry
import fovus.plugin.s3.StorageCredentialsException
import nextflow.exception.AbortOperationException
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

class DirectWorkDirStorageTest extends Specification {

    def 'prepare should attach the S3 client for the pipeline'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()
        def client = Stub(FovusS3Client) {
            head('pipelines/p-1-user/x') >> new S3Entry('pipelines/p-1-user/x', 3L, null, false)
        }
        def connector = Mock(S3Connector)
        def storage = new DirectWorkDirStorage((FovusPath) fs.getPath('/fovus-storage/pipelines'), connector)

        when:
        storage.prepare('p-1-user')

        then:
        1 * connector.connect('p-1-user') >> client
        Files.size(fs.getPath('/fovus-storage/pipelines/p-1-user/x')) == 3
    }

    def 'a credentials failure should stop the run with its message'() {
        given:
        def connector = Stub(S3Connector) {
            connect(_) >> { throw new StorageCredentialsException(FovusStorageCredentialsSource.NOT_SIGNED_IN, false) }
        }
        def root = (FovusPath) PipelinesTestSupport.fileSystem().getPath('/fovus-storage/pipelines')

        when:
        new DirectWorkDirStorage(root, connector).prepare('p-1-user')

        then:
        def e = thrown(AbortOperationException)
        e.message == "[FOVUS] ${FovusStorageCredentialsSource.NOT_SIGNED_IN}".toString()
    }

    def 'Fovus paths are local to direct mode and are already compute-node paths'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()
        def storage = new DirectWorkDirStorage((FovusPath) fs.getPath('/fovus-storage/pipelines'), Stub(S3Connector))

        expect:
        storage.isForeignFile(Path.of('/home/me/input.txt'))
        !storage.isForeignFile(fs.getPath('/fovus-storage/files/input.txt'))
        storage.remotePath(fs.getPath('/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef/.command.run')) ==
                Path.of('/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef/.command.run')
    }
}
```

`src/test/groovy/fovus/plugin/FovusExecutorTest.groovy`:

```groovy
package fovus.plugin

import fovus.plugin.storage.WorkDirStorage
import spock.lang.Specification

import java.nio.file.Path

class FovusExecutorTest extends Specification {

    def 'the executor should leave path decisions to the work directory storage'() {
        given:
        def storage = Mock(WorkDirStorage)
        def executor = new FovusExecutor()
        executor.workDirStorage = storage
        def path = Path.of('/x')

        when:
        def remote = executor.getRemotePath(path)
        def foreign = executor.isForeignFile(path)

        then:
        1 * storage.remotePath(path) >> Path.of('/fovus-storage/x')
        1 * storage.isForeignFile(path) >> true
        remote == Path.of('/fovus-storage/x')
        foreign
    }
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `./gradlew test --tests 'fovus.plugin.storage.*' --tests 'fovus.plugin.FovusExecutorTest'`
Expected: compilation FAILS (`WorkDirStorage` does not exist).

- [ ] **Step 3: Write the storage modes**

`src/main/groovy/fovus/plugin/storage/WorkDirStorage.groovy`:

```groovy
package fovus.plugin.storage

import groovy.transform.CompileStatic

import java.nio.file.Path

/** Where a pipeline's work directory lives: a Fovus storage mount, or Fovus storage itself (direct mode). */
@CompileStatic
interface WorkDirStorage {

    /** Make the work directory usable for this pipeline: mount it, or fetch credentials and attach the S3 client. */
    void prepare(String pipelineId)

    /** Whether Nextflow must stage (copy) this input into the work directory before a task can use it. */
    boolean isForeignFile(Path path)

    /** The path as the compute node sees it, under {@code /fovus-storage}. */
    Path remotePath(Path path)
}
```

`src/main/groovy/fovus/plugin/storage/MountedWorkDirStorage.groovy`:

```groovy
package fovus.plugin.storage

import groovy.transform.CompileStatic

import java.nio.file.FileSystems
import java.nio.file.Path

/** Mount mode, unchanged: Fovus storage is FUSE-mounted at the parent of {@code workDir}. */
@CompileStatic
class MountedWorkDirStorage implements WorkDirStorage {

    private static final String REMOTE_MOUNT_POINT = '/fovus-storage'

    private final FovusStorageClient storageClient
    private final Path mountDir

    MountedWorkDirStorage(FovusStorageClient storageClient, Path sessionWorkDir) {
        this.storageClient = storageClient
        this.mountDir = sessionWorkDir.parent
    }

    @Override
    void prepare(String pipelineId) {
        storageClient.validateOrMountFovusStorage(mountDir)
    }

    @Override
    boolean isForeignFile(Path path) {
        if (path.fileSystem != FileSystems.default) return true
        return !path.toAbsolutePath().startsWith(mountDir.toAbsolutePath())
    }

    @Override
    Path remotePath(Path file) {
        // Replace the mount point part with the REMOTE_MOUNT_POINT
        return Path.of(REMOTE_MOUNT_POINT, file.toString().replace(mountDir.toString(), ''))
    }
}
```

`src/main/groovy/fovus/plugin/storage/S3Connector.groovy`:

```groovy
package fovus.plugin.storage

import fovus.plugin.s3.FovusS3Client
import groovy.transform.CompileStatic

/** Builds the S3 client for one pipeline's work directory. */
@CompileStatic
interface S3Connector {
    FovusS3Client connect(String pipelineId) throws IOException
}
```

`src/main/groovy/fovus/plugin/storage/CliS3Connector.groovy`:

```groovy
package fovus.plugin.storage

import fovus.plugin.FovusConfig
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusStorageCredentialsSource
import fovus.plugin.s3.RefreshingStorageCredentials
import groovy.transform.CompileStatic

/** Credentials from the Fovus CLI, fetched once now so a bad sign-in, CLI or pipeline fails at start-up. */
@CompileStatic
class CliS3Connector implements S3Connector {

    private final FovusConfig config

    CliS3Connector(FovusConfig config) {
        this.config = config
    }

    @Override
    FovusS3Client connect(String pipelineId) throws IOException {
        final credentials = new RefreshingStorageCredentials(new FovusStorageCredentialsSource(config, pipelineId))
        credentials.initialize()
        return FovusS3Client.create(credentials)
    }
}
```

`src/main/groovy/fovus/plugin/storage/DirectWorkDirStorage.groovy`:

```groovy
package fovus.plugin.storage

import fovus.plugin.nio.FovusPath
import fovus.plugin.s3.FovusS3Client
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException

import java.nio.file.Path

/**
 * Direct mode: the work directory is {@code fovus:///fovus-storage/pipelines}, read and written with the
 * AWS S3 SDK. A {@link FovusPath} prints as {@code /fovus-storage/...}, which is already the path the
 * compute node sees.
 */
@Slf4j
@CompileStatic
class DirectWorkDirStorage implements WorkDirStorage {

    private final FovusPath workDir
    private final S3Connector connector

    DirectWorkDirStorage(FovusPath workDir, S3Connector connector) {
        this.workDir = workDir
        this.connector = connector
    }

    @Override
    void prepare(String pipelineId) {
        workDir.getFileSystem().attachS3Client(connect(pipelineId))
        log.debug "[FOVUS] Direct mode: using Fovus storage for pipeline ${pipelineId} without a mount"
    }

    @Override
    boolean isForeignFile(Path path) {
        return !(path instanceof FovusPath)
    }

    @Override
    Path remotePath(Path path) {
        return Path.of(path.toString())
    }

    private FovusS3Client connect(String pipelineId) {
        try {
            return connector.connect(pipelineId)
        }
        catch (IOException e) {
            throw new AbortOperationException("[FOVUS] ${e.message}".toString(), e)
        }
    }
}
```

`src/main/groovy/fovus/plugin/storage/WorkDirStorageFactory.groovy`:

```groovy
package fovus.plugin.storage

import fovus.plugin.FovusConfig
import fovus.plugin.nio.FovusPath
import groovy.transform.CompileStatic
import nextflow.exception.AbortOperationException
import nextflow.extension.FilesEx

import java.nio.file.FileSystems
import java.nio.file.Path

/** Picks mount or direct mode from Nextflow's {@code workDir}, and rejects anything else. */
@CompileStatic
class WorkDirStorageFactory {

    static final String DIRECT_MODE_WORK_DIR = 'fovus:///fovus-storage/pipelines'

    static WorkDirStorage create(Path workDir, boolean isHostedMode, FovusConfig config) {
        if (workDir instanceof FovusPath) {
            return direct((FovusPath) workDir, isHostedMode, config)
        }
        if (workDir.fileSystem == FileSystems.default) {
            if (!workDir.endsWith('pipelines')) {
                throw new AbortOperationException(
                        "[FOVUS] Working directory must end with pipelines. Current work directory: ${workDir}".toString())
            }
            return new MountedWorkDirStorage(new FovusStorageClient(config), workDir)
        }
        throw new AbortOperationException(
                "[FOVUS] The Fovus executor needs workDir to be a Fovus storage mount (…/pipelines) or ${DIRECT_MODE_WORK_DIR}. Current work directory: ${FilesEx.toUriString(workDir)}".toString())
    }

    private static WorkDirStorage direct(FovusPath workDir, boolean isHostedMode, FovusConfig config) {
        if (isHostedMode) {
            throw new AbortOperationException(
                    '[FOVUS] Direct mode (a fovus:// workDir) is only for pipelines launched on your own machine; ' +
                    'Fovus-hosted runs use the Fovus storage mount')
        }
        if (!workDir.isPipelinesAreaRoot()) {
            throw new AbortOperationException(
                    "[FOVUS] In direct mode, workDir must be ${DIRECT_MODE_WORK_DIR}. Current work directory: ${workDir.toUri()}".toString())
        }
        return new DirectWorkDirStorage(workDir, new CliS3Connector(config))
    }
}
```

- [ ] **Step 4: Wire the executor to the seam**

Replace the whole content of `src/main/groovy/fovus/plugin/FovusExecutor.groovy` with:

```groovy
package fovus.plugin

import fovus.plugin.pipeline.FovusPipelineClient
import fovus.plugin.storage.WorkDirStorage
import fovus.plugin.storage.WorkDirStorageFactory
import fovus.plugin.util.FovusEnvironment
import fovus.plugin.util.PublishDirResolver
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.executor.TaskArrayExecutor
import nextflow.extension.FilesEx
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint

import java.nio.file.Path

@Slf4j
@ServiceName('fovus')
@CompileStatic
class FovusExecutor extends Executor implements ExtensionPoint, TaskArrayExecutor {
    protected FovusConfig fovusConfig

    protected FovusPipelineClient pipelineClient;
    /** Where the work directory lives: a Fovus storage mount, or Fovus storage itself (direct mode) */
    protected WorkDirStorage workDirStorage
    protected Path remoteBinDir;

    /**
     * Map the local work directory with Fovus job id
     */
    volatile Map<String, String> jobIdMap = [:]

    Map<String, String> getJobIdMap() { jobIdMap }

    /**
     * @return The monitor instance that monitor submitted Fovus jobs
     */
    @Override
    protected TaskMonitor createTaskMonitor() {
        return TaskPollingMonitor.create(session, config, name, Duration.of("10 sec"))
    }

    @Override
    protected void register() {
        super.register()

        final isHostedMode = FovusEnvironment.isHostedMode()
        fovusConfig = FovusConfig.fromSession(session);
        // Pick and validate the storage mode from workDir before anything is created
        workDirStorage = WorkDirStorageFactory.create(session.workDir, isHostedMode, fovusConfig)

        if (!isHostedMode && fovusConfig.auth.isConfigured()) {
            warmUpAuth(fovusConfig)
        }

        log.debug "[FOVUS] Creating fovus pipeline."
        this.pipelineClient = new FovusPipelineClient();

        final pipelineId = FovusPipelineCache.getOrCreatePipelineId(this.pipelineClient, fovusConfig,
                                                                    this.fovusConfig.getPipelineName(),
                                                                    session?.getCommandLine())

        workDirStorage.prepare(pipelineId)
        uploadBinDir()

        if (isHostedMode) {
            PublishDirResolver.initialize(
                FovusEnvironment.getFovusUserBucket() ?: '',
                FovusEnvironment.getPipelineId() ?: ''
            )
        }
    }

    /**
     * One `fovus auth user` call, single-threaded, before any Nextflow task runs, so the Fovus CLI's
     * per-PAT cache is warm before task fan-out, and a bad configured credential fails clearly here
     * rather than on whichever task happens to run first.
     */
    private void warmUpAuth(FovusConfig config) {
        log.debug "[FOVUS] Warming up Fovus CLI authentication"
        final result = FovusUtil.executeCommand([config.getCliPath(), 'auth', 'user'], config.cliEnv())

        if (result.exitCode != 0) {
            throw new RuntimeException("[FOVUS] Failed to authenticate with Fovus using the configured " +
                    "fovus.auth credentials: ${config.redactSecret(result.error)}")
        }
    }

    protected void uploadBinDir() {
        /*
         * upload local binaries
         */
        if (session.binDir && !session.binDir.empty() && !session.disableRemoteBinDir) {
            def tempDir = getTempDir()
            def copyBinDir = FilesEx.copyTo(session.binDir, tempDir)
            // No chmod: Fovus storage mounts fix file modes at mount time (0770), so the scripts are executable
            remoteBinDir = getRemotePath(copyBinDir)
        }
    }

    @PackageScope
    Path getRemoteBinDir() {
        return remoteBinDir
    }

    @Override
    Path getWorkDir() {
        return session.workDir
                .resolve(this.pipelineClient.getPipeline().pipelineId)
                .resolve("fovus-work")
    }

    /**
     * Not literally true -- fovus has no native secrets provider -- but it stops Nextflow
     * from invoking the global {@code SecretsProvider} when building the task wrapper, which
     * can otherwise fail task submission depending on what other plugins are loaded.
     */
    @Override
    boolean isSecretNative() {
        return true
    }

    @Override
    boolean isForeignFile(Path path) {
        return workDirStorage.isForeignFile(path)
    }

    /**
     * Create as task handler for each of Fovus job
     *
     * @param task The {@link TaskRun} instance to be executed
     * @return A {@FovusTaskHandler} for the given task
     */
    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir

        if(task.inputs.size() > 0){
            log.debug "[FOVUS] Moving local files > ${task}"
        }

        log.debug "[FOVUS] Launching process > ${task.name} -- work folder: ${task.workDir}"
        return new FovusTaskHandler(task, this)
    }

    @Override
    String getArrayIndexName() {
        return "FOVUS_TASK_ARRAY"
    }

    @Override
    int getArrayIndexStart() {
        return 0
    }

    @Override
    String getArrayTaskId(String jobId, int index) {
        return "${jobId}:${index}"
    }

    @Override
    String getArrayLaunchCommand(String taskDir) {
        return TaskArrayExecutor.super.getArrayLaunchCommand(taskDir);
    }

    /** The path as the compute node sees it, under /fovus-storage */
    Path getRemotePath(Path file) {
        return workDirStorage.remotePath(file)
    }

}
```

- [ ] **Step 5: Run the tests to see them pass**

Run: `./gradlew test`
Expected: all PASS, including the new `WorkDirStorageFactoryTest` (9 iterations), `MountedWorkDirStorageTest` (3), `DirectWorkDirStorageTest` (3) and `FovusExecutorTest` (1).

Run: `grep -n chmod src/main/groovy/fovus/plugin/FovusExecutor.groovy`
Expected: only the comment line in `uploadBinDir()`.

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/fovus/plugin/storage src/main/groovy/fovus/plugin/FovusExecutor.groovy src/test/groovy/fovus/plugin/storage src/test/groovy/fovus/plugin/FovusExecutorTest.groovy
git commit -m "Pick mount or direct mode from workDir in the Fovus executor"
```

---

### Task 8: Task handler — tolerate temporary storage errors, drop `chmod`

**Files:**
- Create: `src/main/groovy/fovus/plugin/ExitStatusReader.groovy`
- Modify: `src/main/groovy/fovus/plugin/FovusTaskHandler.groovy` (lines 184-186, 210, 334-337, 340-348, 409-411, import of `Escape`)
- Test: `src/test/groovy/fovus/plugin/ExitStatusReaderTest.groovy`, `src/test/groovy/fovus/plugin/FovusFileCopyStrategyTest.groovy`

**Interfaces:**
- Consumes: `FovusPath`, `PipelinesTestSupport.fileSystem(FovusS3Client)` (Task 6), `S3Entry`, `StorageCredentialsException` (Tasks 1, 4).
- Produces: `class ExitStatusReader` — `Integer read(Path exitFile, Path taskDir)` returning the status, `Integer.MAX_VALUE` when missing/unreadable, or `null` for "try again on the next poll"; `IOException getFailure()`; `MAX_CONSECUTIVE_STORAGE_FAILURES` (30).

- [ ] **Step 1: Write the failing tests**

`src/test/groovy/fovus/plugin/ExitStatusReaderTest.groovy`:

```groovy
package fovus.plugin

import fovus.plugin.nio.PipelinesTestSupport
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.S3Entry
import fovus.plugin.s3.StorageCredentialsException
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path

class ExitStatusReaderTest extends Specification {

    static final String TASK_KEY = 'pipelines/p-1-user/fovus-work/ab/cdef'

    @TempDir
    Path tempDir

    def reader = new ExitStatusReader()

    /** A direct-mode task folder whose .exitcode reads fail or succeed as {@code getObject} says. */
    private Path directTaskDir(Closure getObject) {
        final client = Stub(FovusS3Client) {
            list(_) >> [new S3Entry(TASK_KEY + '/', 0L, null, true)]
            getObject(*_) >> getObject
        }
        return PipelinesTestSupport.fileSystem(client).getPath('/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef')
    }

    def 'a local exit file should be read as before'() {
        given:
        def exitFile = Files.writeString(tempDir.resolve('.exitcode'), '0\n')

        expect:
        reader.read(exitFile, tempDir) == 0
        reader.read(tempDir.resolve('missing'), tempDir) == Integer.MAX_VALUE
        reader.read(Files.writeString(tempDir.resolve('empty'), ''), tempDir) == Integer.MAX_VALUE
        reader.failure == null
    }

    def 'a direct-mode exit file should be read from Fovus storage'() {
        given:
        def taskDir = directTaskDir { new ByteArrayInputStream('1\n'.bytes) }

        expect:
        reader.read(taskDir.resolve('.exitcode'), taskDir) == 1
    }

    def 'a temporary storage error should defer until the limit, then give up'() {
        given:
        def taskDir = directTaskDir { throw new IOException('connection reset') }
        def exitFile = taskDir.resolve('.exitcode')

        expect:
        (1..<ExitStatusReader.MAX_CONSECUTIVE_STORAGE_FAILURES).every { reader.read(exitFile, taskDir) == null }
        reader.read(exitFile, taskDir) == Integer.MAX_VALUE
        reader.failure.message == 'connection reset'
    }

    def 'credentials that can no longer be refreshed should fail at once'() {
        given:
        def taskDir = directTaskDir { throw new StorageCredentialsException('not signed in', false) }

        expect:
        reader.read(taskDir.resolve('.exitcode'), taskDir) == Integer.MAX_VALUE
        reader.failure.message == 'not signed in'
    }

    def 'a missing direct-mode exit file should be read as missing'() {
        given:
        def taskDir = directTaskDir { throw new NoSuchFileException('.exitcode') }

        expect:
        reader.read(taskDir.resolve('.exitcode'), taskDir) == Integer.MAX_VALUE
        reader.failure == null
    }
}
```

`src/test/groovy/fovus/plugin/FovusFileCopyStrategyTest.groovy`:

```groovy
package fovus.plugin

import fovus.plugin.nio.PipelinesTestSupport
import nextflow.processor.TaskBean
import spock.lang.Specification

class FovusFileCopyStrategyTest extends Specification {

    def 'an output of a direct-mode task should be copied into the compute workspace'() {
        given:
        def workDir = PipelinesTestSupport.fileSystem().getPath('/fovus-storage/pipelines/p-1-user/fovus-work')
        def executor = Stub(FovusExecutor) { getWorkDir() >> workDir }
        def bean = new TaskBean()
        bean.workDir = workDir.resolve('ab/cdef')

        expect:
        new FovusFileCopyStrategy(bean, executor).copyFile('out.txt', workDir.resolve('ab/cdef/out.txt')) ==
                'cp out.txt /compute_workspace/cdef/out.txt'
    }
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `./gradlew test --tests 'fovus.plugin.ExitStatusReaderTest' --tests 'fovus.plugin.FovusFileCopyStrategyTest'`
Expected: compilation FAILS (`ExitStatusReader` does not exist). If only `FovusFileCopyStrategyTest` compiles, it should already PASS — it pins behaviour Task 6 made possible.

- [ ] **Step 3: Write `ExitStatusReader`**

`src/main/groovy/fovus/plugin/ExitStatusReader.groovy`:

```groovy
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
```

- [ ] **Step 4: Use it in `FovusTaskHandler` and remove the `chmod` calls**

In `FovusTaskHandler.groovy`:

1. Add a field after `protected FovusTaskClient taskClient;`:

```groovy
    private final ExitStatusReader exitStatusReader = new ExitStatusReader()
```

2. Replace

```groovy
        task.stdout = outputFile

        task.exitStatus = readExitFile()
```

with

```groovy
        final exitStatus = exitStatusReader.read(exitFile, task.workDir)
        if (exitStatus == null) {
            // A temporary Fovus storage error (direct mode): check again on the next poll
            return false
        }

        task.stdout = outputFile

        task.exitStatus = exitStatus
```

3. Directly before `status = TaskStatus.COMPLETED` in `checkIfCompleted()`, add:

```groovy
        if (!task.error && exitStatusReader.failure) {
            task.error = new ProcessException("Unable to read the task results from Fovus storage: ${exitStatusReader.failure.message}")
        }
```

4. Delete the now-unused `readExitFile()` method (lines 340-348).

5. At the end of `submit()`, delete:

```groovy
        // Change the run scripts permission in background
        "chmod +x ${Escape.path(wrapperFile)} ${Escape.path(scriptFile)}".execute()
        // Allow creating new files in work directory
        "chmod 777 ${Escape.path(task.workDir)}".execute()
```

6. At the end of the loop body in `prepareArrayTasks()`, delete:

```groovy
            "chmod +x ${Escape.path(runScriptPath)}".execute()
            "chmod +x ${Escape.path(handler.wrapperFile)} ${Escape.path(handler.scriptFile)}".execute()
            "chmod 777 ${Escape.path(handler.task.workDir)}".execute()
```

7. Remove the `import nextflow.util.Escape` line (nothing else in the file uses it).

- [ ] **Step 5: Run the tests to see them pass**

Run: `./gradlew test`
Expected: all PASS, including `ExitStatusReaderTest` (5) and `FovusFileCopyStrategyTest` (1).

Run: `grep -rn "chmod" src/main/groovy/fovus/plugin/FovusTaskHandler.groovy`
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add src/main/groovy/fovus/plugin/ExitStatusReader.groovy src/main/groovy/fovus/plugin/FovusTaskHandler.groovy src/test/groovy/fovus/plugin/ExitStatusReaderTest.groovy src/test/groovy/fovus/plugin/FovusFileCopyStrategyTest.groovy
git commit -m "Defer task completion on temporary Fovus storage errors and drop no-op chmod calls"
```

---

### Task 9: One task's files end to end against MinIO

**Files:**
- Test: `src/test/groovy/fovus/plugin/DirectModeTaskFilesIT.groovy`

**Interfaces:**
- Consumes: `FovusScriptLauncher(TaskBean, FovusExecutor, FovusJobConfig, boolean)` (existing), `ExitStatusReader` (Task 8), `PipelinesTestSupport` (Task 6), `MinioSupport` (Task 5), Nextflow `FileHelper.visitFiles` and `FileHelper.copyPath`.
- Produces: nothing new; this pins the whole direct-mode task-file path.

- [ ] **Step 1: Write the test**

```groovy
package fovus.plugin

import fovus.plugin.nio.FovusPath
import fovus.plugin.nio.PipelinesTestSupport
import fovus.plugin.s3.MinioSupport
import nextflow.file.FileHelper
import nextflow.processor.TaskBean
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.services.s3.S3Client
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Tag
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

@Tag('integration')
class DirectModeTaskFilesIT extends Specification {

    @Shared MinIOContainer minio
    @Shared S3Client s3

    @TempDir
    Path tempDir

    FovusPath workDir

    def setupSpec() {
        minio = MinioSupport.start()
        s3 = MinioSupport.s3Client(minio)
    }

    def cleanupSpec() {
        s3?.close()
        minio?.stop()
    }

    def setup() {
        final fs = PipelinesTestSupport.fileSystem(MinioSupport.fovusClient(s3))
        workDir = (FovusPath) fs.getPath("/fovus-storage/pipelines/p-1-user/fovus-work/ab/${UUID.randomUUID()}")
        Files.createDirectories(workDir)
    }

    def 'a direct-mode task can be prepared, read back and published over the SDK'() {
        given: 'the task Nextflow hands to the plugin'
        def bean = new TaskBean()
        bean.name = 'hello'
        bean.workDir = workDir
        bean.targetDir = workDir
        bean.script = 'echo hello > out.txt'
        bean.shell = ['bash']
        bean.environment = [GREETING: 'hello']
        bean.inputFiles = [:]
        bean.outputFiles = ['out.txt']
        def executor = Stub(FovusExecutor) {
            getRemoteBinDir() >> null
            getWorkDir() >> workDir.parent.parent
        }

        when: 'the plugin prepares it'
        new FovusScriptLauncher(bean, executor, null, false).build()

        then: 'the scripts are in Fovus storage'
        workDir.resolve('.command.sh').text.contains('echo hello > out.txt')
        workDir.resolve('.command.run').text.contains('.command.sh')
        workDir.resolve('.command.fovus.env').text == 'export GREETING="hello"\n'

        when: 'the compute node finishes and Fovus syncs its results back'
        Files.writeString(workDir.resolve('.exitcode'), '0\n')
        Files.writeString(workDir.resolve('out.txt'), 'hello\n')
        def exitStatus = new ExitStatusReader().read(workDir.resolve('.exitcode'), workDir)
        def outputs = []
        FileHelper.visitFiles([type: 'file'], workDir, 'out.txt') { Path p -> outputs << p }
        def published = tempDir.resolve('results/out.txt')
        FileHelper.copyPath(workDir.resolve('out.txt'), published)

        then: 'the plugin and Nextflow read them back and publish locally'
        exitStatus == 0
        outputs == [workDir.resolve('out.txt')]
        published.text == 'hello\n'
    }
}
```

If `build()` throws a `NullPointerException` or an assertion about a missing `TaskBean` property, set that property the way Nextflow's own `BashWrapperBuilderTest` does (for example `bean.statsEnabled = false`, `bean.outputEnvNames = []`); do not change plugin code to get past it.

- [ ] **Step 2: Run it**

Run: `./gradlew integrationTest --tests 'fovus.plugin.DirectModeTaskFilesIT'`
Expected: PASS (1 test).

- [ ] **Step 3: Commit**

```bash
git add src/test/groovy/fovus/plugin/DirectModeTaskFilesIT.groovy
git commit -m "Add an end-to-end direct mode test for one task's files"
```

---

### Task 10: Document direct mode in the README

**Files:**
- Modify: `README.md` (new section after "Automated authentication (`fovus.auth`)")

**Interfaces:** none.

- [ ] **Step 1: Add the section**

Insert before the `## Building` heading:

````markdown
## Direct mode: run without mounting Fovus storage

By default the plugin mounts Fovus storage on the machine running Nextflow (`fovus storage mount`),
which needs FUSE. If you cannot mount FUSE there, point `workDir` at Fovus storage instead:

```groovy
workDir = 'fovus:///fovus-storage/pipelines'

fovus {
    pipelineName = 'my-pipeline'
}
```

or on the command line: `nextflow run main.nf -w fovus:///fovus-storage/pipelines`.

In direct mode the plugin reads and writes the pipeline's work directory with the AWS S3 SDK, using
short-lived credentials it gets from the Fovus CLI. Nothing is mounted, and compute nodes see the same
files at `/fovus-storage/pipelines/...` as in mount mode.

- It needs a Fovus CLI that provides storage credentials. If yours is too old, the run stops and asks
  you to run `pip install --upgrade fovus`.
- It is only for pipelines launched on your own machine; Fovus-hosted runs always use the mount.
- `workDir` must be exactly `fovus:///fovus-storage/pipelines`.
- Local and remote (`http`, `s3://`) inputs are uploaded into the pipeline's work directory. Inputs
  already in Fovus storage (`fovus:///fovus-storage/files/...`) are used in place.
- `publishDir` to a local folder downloads the results; `symlink` and `link` modes become `copy`.
  Publishing into Fovus storage `files/` is not supported in direct mode yet.
- `cleanup = true` has no effect, as for any remote work directory in Nextflow.
- Switching a pipeline between mount and direct mode re-runs its tasks on `-resume`.
- Your own AWS credentials are neither used nor changed: `s3://` inputs from your buckets keep using them.
````

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "Document direct storage mode"
```

---

### Task 11: End-to-end acceptance on a machine without FUSE (manual)

Needs the CLI branch from the CLI plan and a beta Fovus account. Nothing here is committed.

**Files:** none committed. Scratch: `/tmp/direct-mode-e2e/main.nf`, `/tmp/direct-mode-e2e/big.bin`.

**Interfaces:** consumes everything above.

- [ ] **Step 1: Build and install the plugin on the host**

```bash
cd /Users/jashminpatel/Desktop/Code/Nextflow-Plugin/nf-fovus && ./gradlew assemble install
```

Expected: `BUILD SUCCESSFUL`; the plugin is under `~/.nextflow/plugins/nf-fovus-<version>`.

- [ ] **Step 2: Write the test pipeline**

`/tmp/direct-mode-e2e/main.nf`:

```groovy
params.big = 'big.bin'
params.samplesheet = null

process HELLO {
    publishDir 'results', mode: 'copy'

    input:
    path big
    path samplesheet

    output:
    path 'out/*.txt'
    env 'SIZE'

    script:
    """
    mkdir -p out
    wc -c < ${big} | tr -d ' ' > out/size.txt
    head -n1 ${samplesheet} > out/header.txt
    SIZE=\$(cat out/size.txt)
    """
}

process FLAKY {
    errorStrategy 'retry'
    maxRetries 1

    output:
    stdout

    script:
    """
    if [ ${task.attempt} -eq 1 ]; then exit 1; fi
    echo recovered
    """
}

process ARRAYED {
    array 3

    input:
    val x

    output:
    path "${x}.txt"

    script:
    "echo ${x} > ${x}.txt"
}

workflow {
    HELLO(file(params.big), file(params.samplesheet))
    FLAKY().view()
    ARRAYED(Channel.of('a', 'b', 'c')).view()
}
```

Create the large input: `dd if=/dev/urandom of=/tmp/direct-mode-e2e/big.bin bs=1M count=120`. Upload any small CSV to Fovus storage and note its path, e.g. `/fovus-storage/files/e2e/samplesheet.csv`.

- [ ] **Step 3: Start a container that cannot mount FUSE**

```bash
docker run --rm -it -v /tmp/direct-mode-e2e:/e2e -v "$HOME/.nextflow/plugins:/root/.nextflow/plugins" -v /Users/jashminpatel/Desktop/Code/CLI/fovus-cli-python:/cli -w /e2e eclipse-temurin:21 bash
```

Inside: `apt-get update && apt-get install -y python3-pip curl && pip install /cli && curl -s https://get.nextflow.io | bash`, then sign in with `export FOVUS_EMAIL=… FOVUS_PAT=…` (or configure `fovus.auth` with Nextflow secrets).

Check FUSE really is unavailable: `fovus storage mount` must fail.

- [ ] **Step 4: Run and check**

```bash
./nextflow run main.nf -plugins nf-fovus -w fovus:///fovus-storage/pipelines --samplesheet fovus:///fovus-storage/files/e2e/samplesheet.csv
```

Expected: the run completes; `results/out/size.txt` contains `125829120`; `results/out/header.txt` is the CSV's first line; `FLAKY` prints `recovered` after one retry; `ARRAYED` prints three paths.

- [ ] **Step 5: Resume, interrupt, and refresh**

1. Re-run with `-resume`: every task reports `cached`.
2. Add `process.cache = false` temporarily, start the run, press Ctrl-C while `HELLO` is staging, then run again with `-resume` (and the cache line removed): the run completes and `results/` is correct.
3. Add a process with `script: "sleep 4500"` and run it once: it completes, and `grep -c 'Fetched Fovus storage credentials' .nextflow.log` is at least 2 (with `-trace fovus.plugin.s3` on the command line).

- [ ] **Step 6: Check nothing leaked**

```bash
grep -E 'ASIA[A-Z0-9]{16}' .nextflow.log ~/.fovus/logs/* ; echo "exit=$?"
grep -iE 'SessionToken|SecretAccessKey' .nextflow.log ~/.fovus/logs/* ; echo "exit=$?"
```

Expected: both print only `exit=1` (no matches).

- [ ] **Step 7: Mount mode still works**

On a machine that can mount FUSE, run the same pipeline with `-w <mount>/pipelines` (the current way). Expected: it completes as before.

- [ ] **Step 8: Record the result**

Note the date, Nextflow version, CLI version and outcome of Steps 4–7 in the pull request description.
