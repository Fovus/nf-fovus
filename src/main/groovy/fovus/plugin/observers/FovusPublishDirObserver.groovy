package fovus.plugin.observers

import fovus.plugin.util.FovusEnvironment
import fovus.plugin.util.MountS3Adapter
import fovus.plugin.util.PublishDirResolver
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.TaskEvent

@Slf4j
@CompileStatic
class FovusPublishDirObserver implements TraceObserverV2 {

    private final PublishDirResolver resolver

    FovusPublishDirObserver(Session session) {
        this(buildResolver())
    }

    FovusPublishDirObserver(PublishDirResolver resolver) {
        this.resolver = resolver
    }

    private static PublishDirResolver buildResolver() {
        final String bucket = FovusEnvironment.getFovusUserBucket()
        final String pipelineId = FovusEnvironment.getPipelineId()
        if (!bucket) {
            log.warn "[FOVUS] FovusUserBucket is not set — publishDir mounts will be skipped"
        }
        if (!pipelineId) {
            log.warn "[FOVUS] PIPELINE_ID is not set — publishDir mounts will be skipped"
        }
        return new PublishDirResolver(new MountS3Adapter(), bucket ?: '', pipelineId ?: '')
    }

    @Override
    void onTaskPending(TaskEvent event) {
        if (!FovusEnvironment.isHostedMode()) return
        final task = event?.handler?.task
        if (!task) return
        try {
            resolver.resolve(task.config)
        } catch (Exception e) {
            log.error "[FOVUS] Failed to mount publishDir for pending task ${task.lazyName()}: ${e.message}", e
            throw e
        }
    }

    @Override
    void onTaskCached(TaskEvent event) {
        if (!FovusEnvironment.isHostedMode()) return
        final task = event?.handler?.task
        if (!task) return
        try {
            resolver.resolve(task.config)
        } catch (Exception e) {
            log.error "[FOVUS] Failed to mount publishDir for cached task ${task.lazyName()}: ${e.message}", e
            throw e
        }
    }
}
