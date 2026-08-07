package fovus.plugin.observers

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.file.FileHelper
import nextflow.trace.TraceFileObserver
import nextflow.trace.TraceObserverFactoryV2
import nextflow.trace.TraceObserverV2
import nextflow.trace.config.TraceConfig

import java.nio.file.Files
import java.nio.file.Path

@Slf4j
@CompileStatic
class FovusTraceObserverFactory implements TraceObserverFactoryV2 {

    private static final String TRACE_FILE_PREFIX = 'trace_'
    private static final String TRACE_FILE_SUFFIX = '.txt'
    private static final int MAX_TRACE_FILE_VERSION = 1000

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
        final config = createTraceConfig(session)
        return config ? new TraceFileObserver(config) : null
    }

    @PackageScope
    TraceConfig createTraceConfig(Session session) {
        final opts = new LinkedHashMap<String, Object>(session.config.navigate('trace') as Map ?: [:])
        if (opts.enabled) {
            return null
        }

        opts.enabled = true
        // Tracing was not requested by the user, so no existing file may be clobbered: pick a name
        // that is not taken yet instead of the default one, which would abort the run whenever the
        // launch directory already holds a trace file from a previous execution.
        if (!opts.file) {
            opts.file = nextTraceFileName()
        }

        final config = new TraceConfig(opts)
        log.debug "[FOVUS] `-with-trace` not specified - enabling the execution trace file: ${config.file}"
        return config
    }

    /**
     * Returns the first `trace_<version>.txt` name that is not used yet in the launch directory,
     * so repeated (or resumed) runs each get their own trace file.
     */
    @PackageScope
    String nextTraceFileName(Path launchDir = FileHelper.asPath('.')) {
        for (int version = 1; version <= MAX_TRACE_FILE_VERSION; version++) {
            final name = "${TRACE_FILE_PREFIX}${version}${TRACE_FILE_SUFFIX}".toString()
            if (!Files.exists(launchDir.resolve(name))) {
                return name
            }
        }
        // Too many trace files in the launch directory - fall back to Nextflow's own timestamped name
        return TraceConfig.defaultFileName()
    }
}
