package fovus.plugin.observers

import fovus.plugin.FovusTaskHandler
import fovus.plugin.util.FovusEnvironment
import fovus.plugin.util.PublishDirResolver
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.TaskEvent

@Slf4j
@CompileStatic
class FovusPublishDirObserver implements TraceObserverV2 {

    private final Session session

    FovusPublishDirObserver(Session session) {
        this.session = session
    }

    @Override
    void onTaskPending(TaskEvent event) {
        if (!FovusEnvironment.isHostedMode()) return
        final resolver = resolverFrom(event)
        if (!resolver) return
        final task = event.handler.task
        try {
            resolver.resolve(task.config)
        } catch (Exception e) {
            log.error "[FOVUS] Failed to mount publishDir for pending task ${task.lazyName()}: ${e.message}", e
            // Do not abort — FovusTaskHandler.submit() re-validates and throws ProcessException
            // so Nextflow's configured errorStrategy applies instead of killing all running tasks.
        }
    }

    @Override
    void onTaskCached(TaskEvent event) {
        if (!FovusEnvironment.isHostedMode()) return
        final resolver = resolverFrom(event)
        if (!resolver) return
        final task = event.handler.task
        try {
            resolver.resolve(task.config)
        } catch (Exception e) {
            log.error "[FOVUS] Failed to mount publishDir for cached task ${task.lazyName()}: ${e.message}", e
            // Cached tasks bypass submit(), so there is no TaskHandler path to re-validate.
            // Abort the session directly.
            session.abort(e)
        }
    }

    /**
     * Returns the {@link PublishDirResolver} owned by the {@link FovusExecutor} for this event,
     * or {@code null} if the handler is not a {@link FovusTaskHandler} (e.g. a different executor).
     */
    private static PublishDirResolver resolverFrom(TaskEvent event) {
        final handler = event?.handler
        if (!(handler instanceof FovusTaskHandler)) return null
        return (handler as FovusTaskHandler).getPublishDirResolver()
    }
}
