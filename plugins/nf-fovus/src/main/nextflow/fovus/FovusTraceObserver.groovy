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
        log.info "Pipeline is starting! 🚀"
        FovusPipelineCache.getOrCreatePipelineId(this.pipelineClient, fovusConfig, fovusConfig.getPipelineName())
    }

    @Override
    void onFlowBegin() {
        pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.RUNNING)
    }

    @Override
    void onFlowComplete() {
        if (!isPipelineFailed) {
            pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.COMPLETED)
        }
    }

    @Override
    void onFlowError(TaskEvent event) {
        isPipelineFailed = true
        pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.FAILED)
    }
}
