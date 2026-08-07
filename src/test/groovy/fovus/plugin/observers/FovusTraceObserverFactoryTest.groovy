package fovus.plugin.observers

import nextflow.Session
import nextflow.trace.TraceFileObserver
import spock.lang.Specification

import java.nio.file.Files

class FovusTraceObserverFactoryTest extends Specification {

    def 'should enable the trace file when the run was launched without -with-trace'() {
        given:
        def session = Mock(Session) { getConfig() >> [:] }

        when:
        def observer = new FovusTraceObserverFactory().createTraceFileObserver(session)

        then:
        observer instanceof TraceFileObserver
    }

    def 'should not create a trace file observer when tracing is already enabled'() {
        given:
        def session = Mock(Session) { getConfig() >> [trace: [enabled: true]] }

        when:
        def observer = new FovusTraceObserverFactory().createTraceFileObserver(session)

        then:
        observer == null
    }

    def 'should honour the trace config options when enabling the trace file'() {
        given:
        def session = Mock(Session) { getConfig() >> [trace: [fields: 'task_id,name,status']] }

        when:
        def observer = new FovusTraceObserverFactory().createTraceFileObserver(session) as TraceFileObserver

        then:
        observer.fields == ['task_id', 'name', 'status']
    }

    def 'should name the trace file after the first free version'() {
        given:
        def launchDir = Files.createTempDirectory('fovus-trace')

        expect:
        new FovusTraceObserverFactory().nextTraceFileName(launchDir) == 'trace_1.txt'

        when:
        Files.createFile(launchDir.resolve('trace_1.txt'))
        Files.createFile(launchDir.resolve('trace_2.txt'))

        then:
        new FovusTraceObserverFactory().nextTraceFileName(launchDir) == 'trace_3.txt'

        cleanup:
        launchDir.deleteDir()
    }

    def 'should enable the trace file under a versioned name'() {
        given:
        def session = Mock(Session) { getConfig() >> [:] }

        when:
        def config = new FovusTraceObserverFactory().createTraceConfig(session)

        then:
        config.file ==~ /trace_\d+\.txt/
    }

    def 'should keep the trace file name given in the config'() {
        given:
        def session = Mock(Session) { getConfig() >> [trace: [file: 'my-trace.txt']] }

        when:
        def config = new FovusTraceObserverFactory().createTraceConfig(session)

        then:
        config.file == 'my-trace.txt'
    }
}
