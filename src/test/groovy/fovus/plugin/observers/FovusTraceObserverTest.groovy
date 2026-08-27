package fovus.plugin.observers

import nextflow.Session
import spock.lang.Specification

class FovusTraceObserverTest extends Specification {

    def 'collectResourceConfigurations should return an empty list when the config declares no process scope'() {
        given:
        def session = Mock(Session) { getConfig() >> config }

        expect:
        FovusTraceObserver.collectResourceConfigurations(session) == []

        where:
        config << [[:], [process: 'not-a-map'], [process: [cpus: 4]], [process: [ext: [maxvCpu: 8]]]]
    }

    def 'collectResourceConfigurations should read the global process ext config'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [process: [ext: [benchmarkingProfileName: 'global-profile', maxvCpu: 8]]]
        }

        when:
        def configurations = FovusTraceObserver.collectResourceConfigurations(session)

        then:
        configurations.size() == 1
        configurations[0].benchmarkingProfileName == 'global-profile'
        configurations[0].maxvCpu == 8
    }

    def 'collectResourceConfigurations should merge a per-process override over the global config'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [process: [
                    ext                   : [benchmarkingProfileName: 'global-profile', maxvCpu: 8, minvCpu: 2],
                    'withName:alignReads' : [ext: [benchmarkingProfileName: 'align-profile', maxvCpu: 64]]
            ]]
        }

        when:
        def configurations = FovusTraceObserver.collectResourceConfigurations(session)

        then:
        configurations.size() == 2
        configurations[0].benchmarkingProfileName == 'global-profile'
        configurations[0].maxvCpu == 8

        and: 'the override wins on maxvCpu but inherits minvCpu from the global config'
        configurations[1].benchmarkingProfileName == 'align-profile'
        configurations[1].maxvCpu == 64
        configurations[1].minvCpu == 2
    }

    def 'collectResourceConfigurations should read a per-process config when there is no global one'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [process: ['withName:alignReads': [ext: [benchmarkingProfileName: 'align-profile']]]]
        }

        when:
        def configurations = FovusTraceObserver.collectResourceConfigurations(session)

        then:
        configurations.size() == 1
        configurations[0].benchmarkingProfileName == 'align-profile'
    }

    def 'collectResourceConfigurations should ignore a process entry without a benchmarking profile'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [process: [
                    'withName:alignReads': [ext: [maxvCpu: 64]],
                    'withName:callVars'  : [ext: [benchmarkingProfileName: 'call-profile']]
            ]]
        }

        when:
        def configurations = FovusTraceObserver.collectResourceConfigurations(session)

        then:
        configurations.size() == 1
        configurations[0].benchmarkingProfileName == 'call-profile'
    }

    def 'collectResourceConfigurations should carry the storage connectors of the global config'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [process: [ext: [benchmarkingProfileName: 'global-profile',
                                            storageConnectors      : ['reference-genomes', 'reference-genomes']]]]
        }

        when:
        def configurations = FovusTraceObserver.collectResourceConfigurations(session)

        then: 'duplicates are dropped, exactly as they are for a job'
        configurations.size() == 1
        configurations[0].storageConnectors == ['reference-genomes']
    }

    def 'collectResourceConfigurations should let a per-process override replace the global connectors'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [process: [
                    ext                    : [benchmarkingProfileName: 'global-profile',
                                              storageConnectors      : ['reference-genomes']],
                    'withName:publishResults': [ext: [benchmarkingProfileName: 'publish-profile',
                                                      storageConnectors      : ['results-archive']]],
                    'withName:alignReads'    : [ext: [benchmarkingProfileName: 'align-profile']]
            ]]
        }

        when:
        def configurations = FovusTraceObserver.collectResourceConfigurations(session)

        then:
        configurations.size() == 3
        configurations[0].storageConnectors == ['reference-genomes']

        and: 'the override wins'
        configurations[1].storageConnectors == ['results-archive']

        and: 'a process that declares none inherits the global connectors'
        configurations[2].storageConnectors == ['reference-genomes']
    }

    def 'collectResourceConfigurations should leave the connectors unset when none are declared'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [process: [ext: [benchmarkingProfileName: 'global-profile']]]
        }

        expect:
        FovusTraceObserver.collectResourceConfigurations(session)[0].storageConnectors == null
    }

    def 'collectResourceConfigurations should reject a malformed storage connector name'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [process: [ext: [benchmarkingProfileName: 'global-profile',
                                            storageConnectors      : ['team_shared']]]]
        }

        when:
        FovusTraceObserver.collectResourceConfigurations(session)

        then:
        def error = thrown(Error)
        error.message.contains('team_shared')
    }
}
