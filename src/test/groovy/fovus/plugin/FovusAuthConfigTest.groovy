package fovus.plugin

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class FovusAuthConfigTest extends Specification {

    @TempDir
    Path tempDir

    private static final Map VALID_CONFIG = [email: 'automation@corp.com', personalAccessToken: 'pat-value']
    private static final String VALID_EMAIL_REF = 'secrets.FOVUS_EMAIL'
    private static final String VALID_PAT_REF = 'secrets.FOVUS_PAT'

    // -- validating constructor

    def 'should construct successfully when neither field is set'() {
        when:
        def config = new FovusAuthConfig([:], null, null)

        then:
        !config.isConfigured()
        config.email == null
        config.personalAccessToken == null
    }

    def 'should construct successfully when both fields are set via secrets.X'() {
        when:
        def config = new FovusAuthConfig(VALID_CONFIG, VALID_EMAIL_REF, VALID_PAT_REF)

        then:
        config.isConfigured()
        config.email == 'automation@corp.com'
        config.personalAccessToken == 'pat-value'
    }

    def 'should reject email set without personalAccessToken'() {
        when:
        new FovusAuthConfig([email: 'automation@corp.com'], VALID_EMAIL_REF, null)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('personalAccessToken')
    }

    def 'should reject personalAccessToken set without email'() {
        when:
        new FovusAuthConfig([personalAccessToken: 'pat-value'], null, VALID_PAT_REF)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('email')
    }

    def 'should reject a literal email'() {
        when:
        new FovusAuthConfig(VALID_CONFIG, 'automation@corp.com', VALID_PAT_REF)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('fovus.auth.email')
        e.message.contains('secrets')
    }

    def 'should reject an email sourced from System.getenv()'() {
        when:
        // A System.getenv() call is left untouched by the secrets-stripping parser and evaluates to
        // its real (or null) value, which never happens to match the `secrets.X` marker shape.
        new FovusAuthConfig(VALID_CONFIG, null, VALID_PAT_REF)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('fovus.auth.email')
    }

    def 'should reject a literal personalAccessToken'() {
        when:
        new FovusAuthConfig(VALID_CONFIG, VALID_EMAIL_REF, 'pat-value')

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('fovus.auth.personalAccessToken')
        e.message.contains('secrets')
    }

    def 'should reject a personalAccessToken sourced from System.getenv()'() {
        when:
        new FovusAuthConfig(VALID_CONFIG, VALID_EMAIL_REF, null)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('fovus.auth.personalAccessToken')
    }

    def 'should reject a stripped value that is not a bare secrets.X reference'() {
        when:
        new FovusAuthConfig(VALID_CONFIG, VALID_EMAIL_REF, strippedPat)

        then:
        thrown(IllegalArgumentException)

        where:
        strippedPat << ['secrets.FOVUS_PAT.toUpperCase()', ' secrets.FOVUS_PAT', 'secrets.', 'secrets.1BAD']
    }

    // -- buildEnvironment (pure function)

    def 'buildEnvironment should leave the base environment untouched when auth is not configured'() {
        given:
        def config = new FovusAuthConfig([:], null, null)
        def baseEnv = [PATH: '/usr/bin']

        expect:
        FovusAuthConfig.buildEnvironment(config, baseEnv) == [PATH: '/usr/bin']
    }

    def 'buildEnvironment should leave the base environment untouched when auth is null'() {
        expect:
        FovusAuthConfig.buildEnvironment(null, [PATH: '/usr/bin']) == [PATH: '/usr/bin']
    }

    def 'buildEnvironment should add FOVUS_EMAIL and FOVUS_PAT on top of the base environment'() {
        given:
        def config = new FovusAuthConfig(VALID_CONFIG, VALID_EMAIL_REF, VALID_PAT_REF)
        def baseEnv = [PATH: '/usr/bin']

        expect:
        FovusAuthConfig.buildEnvironment(config, baseEnv) ==
                [PATH: '/usr/bin', FOVUS_EMAIL: 'automation@corp.com', FOVUS_PAT: 'pat-value']
    }

    def 'buildEnvironment should not mutate the base environment map'() {
        given:
        def config = new FovusAuthConfig(VALID_CONFIG, VALID_EMAIL_REF, VALID_PAT_REF)
        def baseEnv = [PATH: '/usr/bin']

        when:
        FovusAuthConfig.buildEnvironment(config, baseEnv)

        then:
        baseEnv == [PATH: '/usr/bin']
    }

    def 'buildEnvironment should tolerate a null base environment'() {
        given:
        def config = new FovusAuthConfig(VALID_CONFIG, VALID_EMAIL_REF, VALID_PAT_REF)

        expect:
        FovusAuthConfig.buildEnvironment(config, null) == [FOVUS_EMAIL: 'automation@corp.com', FOVUS_PAT: 'pat-value']
    }

    // -- resolve (LOCAL-vs-REMOTE orchestration point)

    def 'resolve should skip validation entirely in hosted mode, even for an invalid config'() {
        when:
        def config = FovusAuthConfig.resolve([email: 'automation@corp.com'], null, true)

        then:
        !config.isConfigured()
    }

    def 'resolve should apply real secrets.X enforcement in non-hosted mode'() {
        given:
        def configFile = Files.write(tempDir.resolve('nextflow.config'), """\
            fovus {
                pipelineName = 'test-pipeline'
                auth {
                    email               = secrets.FOVUS_EMAIL
                    personalAccessToken = secrets.FOVUS_PAT
                }
            }
            """.stripIndent().bytes)

        when:
        def config = FovusAuthConfig.resolve(VALID_CONFIG, [configFile], false)

        then:
        config.isConfigured()
        config.email == 'automation@corp.com'
        config.personalAccessToken == 'pat-value'
    }

    def 'resolve should reject a literal personalAccessToken in non-hosted mode'() {
        given:
        def configFile = Files.write(tempDir.resolve('nextflow.config'), """\
            fovus {
                pipelineName = 'test-pipeline'
                auth {
                    email               = secrets.FOVUS_EMAIL
                    personalAccessToken = 'literal-and-not-allowed'
                }
            }
            """.stripIndent().bytes)

        when:
        FovusAuthConfig.resolve([email: 'automation@corp.com', personalAccessToken: 'literal-and-not-allowed'],
                                 [configFile], false)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('fovus.auth.personalAccessToken')
    }

    def 'resolve should treat an unconfigured fovus.auth block as inactive in non-hosted mode'() {
        given:
        def configFile = Files.write(tempDir.resolve('nextflow.config'), """\
            fovus {
                pipelineName = 'test-pipeline'
            }
            """.stripIndent().bytes)

        when:
        def config = FovusAuthConfig.resolve([:], [configFile], false)

        then:
        !config.isConfigured()
    }
}
