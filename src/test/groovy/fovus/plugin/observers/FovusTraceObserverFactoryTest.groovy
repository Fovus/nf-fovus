package fovus.plugin.observers

import nextflow.Session
import nextflow.trace.TraceFileObserver
import spock.lang.Specification

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
}
