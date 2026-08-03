package fovus.plugin.observers

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.trace.TraceFileObserver
import nextflow.trace.TraceObserverFactoryV2
import nextflow.trace.TraceObserverV2
import nextflow.trace.config.TraceConfig

@Slf4j
@CompileStatic
class FovusTraceObserverFactory implements TraceObserverFactoryV2 {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        final List<TraceObserverV2> observers = new ArrayList<>()
        observers.add(new FovusTraceObserver(session))
        observers.add(new FovusPublishDirObserver(session))

        final traceFileObserver = createTraceFileObserver(session)
        if (traceFileObserver) {
            observers.add(traceFileObserver)
        }

        return observers
    }

    /**
     * The execution trace file is always required for a Fovus run, so it is enabled even when the
     * pipeline was launched without `-with-trace`.
     *
     * Returns `null` when tracing is already enabled - in that case Nextflow's own
     * `DefaultObserverFactory` creates the observer.
     *
     * Note the trace options are intentionally *not* written back into the session config: that
     * config is what `DefaultObserverFactory` inspects, and there is no guaranteed ordering between
     * the observer factories, so enabling it there would add a second trace file observer whenever
     * the default factory happens to run after this one.
     */
    @PackageScope
    TraceObserverV2 createTraceFileObserver(Session session) {
        final opts = new LinkedHashMap<String, Object>(session.config.navigate('trace') as Map ?: [:])
        if (opts.enabled) {
            return null
        }

        opts.enabled = true
        final config = new TraceConfig(opts)
        log.debug "[FOVUS] `-with-trace` not specified - enabling the execution trace file: ${config.file}"
        return new TraceFileObserver(config)
    }
}
