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
