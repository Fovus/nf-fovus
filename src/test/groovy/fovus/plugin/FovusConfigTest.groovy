package fovus.plugin

import spock.lang.Specification

class FovusConfigTest extends Specification {

    def 'a single-arg config should have an inert, unconfigured auth block'() {
        when:
        def config = new FovusConfig([pipelineName: 'test-pipeline'])

        then:
        config.auth != null
        !config.auth.isConfigured()
    }

    def 'the two-arg constructor should carry the given auth config through'() {
        given:
        def authConfig = new FovusAuthConfig([email: 'automation@corp.com', personalAccessToken: 'pat-value'],
                                              'secrets.FOVUS_EMAIL', 'secrets.FOVUS_PAT')

        when:
        def config = new FovusConfig([pipelineName: 'test-pipeline'], authConfig)

        then:
        config.auth.is(authConfig)
        config.auth.isConfigured()
    }

    def 'a null auth argument should fall back to an inert auth block'() {
        when:
        def config = new FovusConfig([pipelineName: 'test-pipeline'], null)

        then:
        config.auth != null
        !config.auth.isConfigured()
    }

    def 'pipelineName is still required regardless of auth'() {
        when:
        new FovusConfig([:], null)

        then:
        thrown(IllegalArgumentException)
    }
}
