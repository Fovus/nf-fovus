package fovus.plugin

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class FovusUtilTest extends Specification {

    @TempDir
    Path tempDir

    /** Captures log events emitted by FovusUtil for the duration of a test. */
    private static ListAppender<ILoggingEvent> captureFovusUtilLogs() {
        final logger = LoggerFactory.getLogger(FovusUtil) as Logger
        logger.level = Level.DEBUG
        final appender = new ListAppender<ILoggingEvent>()
        appender.start()
        logger.addAppender(appender)
        return appender
    }

    def 'readNextflowConfig should return null when there is no config file'() {
        expect:
        FovusUtil.readNextflowConfig(configFiles) == null

        where:
        configFiles << [null, []]
    }

    def 'readNextflowConfig should return a single config file verbatim'() {
        given:
        def content = """\
            fovus {
                pipelineName = 'test-pipeline'
            }

            // a comment that must survive
            process {
                ext.benchmarkingProfileName = 'profile'
            }
            """.stripIndent()
        def configFile = Files.write(tempDir.resolve('nextflow.config'), content.bytes)

        when:
        def result = FovusUtil.readNextflowConfig([configFile])

        then:
        result == content
    }

    def 'readNextflowConfig should concatenate several config files behind a header naming each source'() {
        given:
        def base = Files.write(tempDir.resolve('nextflow.config'), "fovus.pipelineName = 'test-pipeline'\n".bytes)
        def extra = Files.write(tempDir.resolve('extra.config'), "process.ext.maxvCpu = 8\n".bytes)

        when:
        def result = FovusUtil.readNextflowConfig([base, extra])

        then:
        result == "// ==== ${base.toAbsolutePath()} ====\n" +
                  "fovus.pipelineName = 'test-pipeline'\n" +
                  "\n" +
                  "// ==== ${extra.toAbsolutePath()} ====\n" +
                  "process.ext.maxvCpu = 8\n"
    }

    def 'readNextflowConfig should skip a config file that does not exist'() {
        given:
        def missing = tempDir.resolve('missing.config')
        def existing = Files.write(tempDir.resolve('nextflow.config'), "process.ext.maxvCpu = 8\n".bytes)

        when:
        def result = FovusUtil.readNextflowConfig([missing, existing])

        then:
        noExceptionThrown()
        result == "// ==== ${existing.toAbsolutePath()} ====\n" +
                  "process.ext.maxvCpu = 8\n"
    }

    def 'readNextflowConfig should return null when none of the config files can be read'() {
        expect:
        FovusUtil.readNextflowConfig([tempDir.resolve('missing.config')]) == null
    }

    def 'executeCommand should carry extra environment variables into the subprocess'() {
        given:
        final command = ['sh', '-c', 'printf "%s" "$FOVUS_TEST_VAR"']

        when:
        final result = FovusUtil.executeCommand(command, [FOVUS_TEST_VAR: 'hello-env'])

        then:
        result.exitCode == 0
        result.output.trim() == 'hello-env'
    }

    def 'executeCommand should not disturb the rest of the subprocess environment'() {
        given:
        final command = ['sh', '-c', 'printf "%s" "$PATH"']

        when:
        final withoutExtra = FovusUtil.executeCommand(command)
        final withExtra = FovusUtil.executeCommand(command, [FOVUS_TEST_VAR: 'hello-env'])

        then:
        withoutExtra.output == withExtra.output
        withoutExtra.output.length() > 0
    }

    def 'executeCommand should never log the configured token raw, even when the CLI echoes it'() {
        given:
        final appender = captureFovusUtilLogs()
        // The token never appears in the command's own argv (that would defeat the point of env
        // injection) - it only shows up here because the CLI itself echoes $FOVUS_PAT back on stderr.
        final command = ['sh', '-c', 'echo "boom: $FOVUS_PAT leaked" 1>&2']

        when:
        FovusUtil.executeCommand(command, [FOVUS_PAT: 'pat-secret-value'])

        then:
        final messages = appender.list.collect { it.formattedMessage }
        messages.any { it.contains('[REDACTED]') }
        !messages.any { it.contains('pat-secret-value') }
    }

    def 'executeCommand should behave exactly as before when no extra environment is given'() {
        given:
        final command = ['sh', '-c', 'echo ok']

        expect:
        FovusUtil.executeCommand(command).output.trim() == 'ok'
    }

    def 'redact should replace every occurrence of the secret'() {
        expect:
        FovusUtil.redact('token=abc123 and again abc123', 'abc123') == 'token=[REDACTED] and again [REDACTED]'
    }

    def 'redact should be a no-op when there is nothing to redact'() {
        expect:
        FovusUtil.redact('plain text', null) == 'plain text'
        FovusUtil.redact('plain text', '') == 'plain text'
        FovusUtil.redact(null, 'secret') == null
    }

    def 'stripSecretsRefs should read back a secrets.X reference as its literal marker'() {
        given:
        def configFile = Files.write(tempDir.resolve('nextflow.config'), """\
            fovus {
                auth {
                    personalAccessToken = secrets.FOVUS_PAT
                }
            }
            """.stripIndent().bytes)

        expect:
        FovusUtil.stripSecretsRefs([configFile], ['fovus.auth.personalAccessToken']) ==
                [(('fovus.auth.personalAccessToken')): 'secrets.FOVUS_PAT']
    }

    def 'stripSecretsRefs should read back a literal value verbatim'() {
        given:
        def configFile = Files.write(tempDir.resolve('nextflow.config'), """\
            fovus {
                auth {
                    personalAccessToken = 'a-literal-value'
                }
            }
            """.stripIndent().bytes)

        expect:
        FovusUtil.stripSecretsRefs([configFile], ['fovus.auth.personalAccessToken']) ==
                [(('fovus.auth.personalAccessToken')): 'a-literal-value']
    }

    def 'stripSecretsRefs should return null for a path that is not set'() {
        given:
        def configFile = Files.write(tempDir.resolve('nextflow.config'), "fovus.pipelineName = 'test'\n".bytes)

        expect:
        FovusUtil.stripSecretsRefs([configFile], ['fovus.auth.personalAccessToken']) ==
                [(('fovus.auth.personalAccessToken')): null]
    }

    def 'stripSecretsRefs should tolerate no config files'() {
        expect:
        FovusUtil.stripSecretsRefs(null, ['fovus.auth.personalAccessToken']) ==
                [(('fovus.auth.personalAccessToken')): null]
    }
}
