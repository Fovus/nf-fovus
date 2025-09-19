package nextflow.fovus

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.fovus.pipeline.FovusPipelineClient
import nextflow.fovus.pipeline.FovusPipelineStatus
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.TaskEvent

@Slf4j
@CompileStatic
class FovusTraceObserver implements TraceObserverV2 {

    private final Session session
    private final FovusConfig fovusConfig
    private final FovusPipelineClient pipelineClient
    volatile boolean isPipelineFailed = false

    FovusTraceObserver(Session session) {
        this.session = session
        this.fovusConfig = new FovusConfig(session.config.navigate('fovus') as Map);
        this.pipelineClient = new FovusPipelineClient();
    }

    @Override
    void onFlowCreate(Session session) {
        // TODO: Check if this is needed or not. Should we create pipeline here on in FovusExecutor?
        log.info "Pipeline is starting! 🚀"
        log.info "Get or create pipeline from session config pipeline name..."
    }

    @Override
    void onFlowBegin() {
        // TODO: Update pipeline status to running
        pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.RUNNING)
    }

    @Override
    void onFlowComplete() {
        // TODO: Update pipeline status to COMPLETED only if all tasks are completed successfully
        if (!isPipelineFailed) {
            pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.COMPLETED)
        }
    }

    @Override
    void onFlowError(TaskEvent event) {
        // TODO: Update pipeline status to failed
        isPipelineFailed = true
        pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.FAILED)
    }
}
