package fovus.plugin

import fovus.plugin.observers.FovusTraceObserver
import fovus.plugin.pipeline.FovusPipeline
import fovus.plugin.pipeline.FovusPipelineClient
import fovus.plugin.pipeline.FovusPipelineStatus
import nextflow.Session
import nextflow.trace.event.TaskEvent
import spock.lang.Specification

class FovusObserverTest extends Specification {
    private static final FovusConfig TEST_CONFIG = new FovusConfig([pipelineName: 'test-pipeline'])
    private static final FovusPipeline TEST_PIPELINE = new FovusPipeline('test-pipeline', 'p-123')
    private static final String RUN_COMMAND = 'nextflow run hello -plugins nf-fovus'

    def 'onFlowBegin should send RUNNING status'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)
        def session = Mock(Session) { getCommandLine() >> RUN_COMMAND }
        def observer = new FovusTraceObserver(session, TEST_CONFIG, pipelineClient)

        when:
        observer.onFlowBegin()

        then:
        1 * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.RUNNING, RUN_COMMAND)
        !observer.hasFlowErrorState()
        observer.lastFlowErrorEvent == null
    }

    def 'onFlowBegin should send a null run command when the session has no command line'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(Mock(Session), TEST_CONFIG, pipelineClient)

        when:
        observer.onFlowBegin()

        then:
        1 * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.RUNNING, null)
    }

    def 'onFlowError should only store failure state in memory'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(Mock(Session), TEST_CONFIG, pipelineClient)
        def event = new TaskEvent(null, null)

        when:
        observer.onFlowError(event)

        then:
        0 * pipelineClient._
        observer.hasFlowErrorState()
        observer.lastFlowErrorEvent.is(event)
    }

    def 'onFlowComplete should send COMPLETED when no flow error was recorded'() {
        given:
        def session = Mock(Session)
        session.isAborted() >> false
        session.getCommandLine() >> RUN_COMMAND
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(session, TEST_CONFIG, pipelineClient)

        when:
        observer.onFlowComplete()

        then:
        1 * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.COMPLETED, RUN_COMMAND)
    }

    def 'onFlowComplete should send FAILED when a flow error was recorded'() {
        given:
        def session = Mock(Session)
        session.isAborted() >> false
        session.getCommandLine() >> RUN_COMMAND
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(session, TEST_CONFIG, pipelineClient)
        observer.onFlowError(new TaskEvent(null, null))

        when:
        observer.onFlowComplete()

        then:
        1 * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.FAILED, RUN_COMMAND)
    }

    def 'onFlowComplete should send FAILED when session was aborted'() {
        given:
        def session = Mock(Session)
        session.isAborted() >> true
        session.getCommandLine() >> RUN_COMMAND
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(session, TEST_CONFIG, pipelineClient)

        when:
        observer.onFlowComplete()

        then:
        1 * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.FAILED, RUN_COMMAND)
    }

    def 'onFlowComplete should update pipeline status exactly once when called multiple times'() {
        given:
        def session = Mock(Session)
        session.isAborted() >> false
        session.getCommandLine() >> RUN_COMMAND
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(session, TEST_CONFIG, pipelineClient)

        when:
        observer.onFlowComplete()
        observer.onFlowComplete()

        then:
        1 * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.COMPLETED, RUN_COMMAND)
    }
}
