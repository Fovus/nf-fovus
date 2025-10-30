package fovus.plugin

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.trace.TraceObserverFactoryV2
import nextflow.trace.TraceObserverV2

@CompileStatic
class FovusTraceObserverFactory implements TraceObserverFactoryV2 {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        final result = new ArrayList()
        result.add(new FovusTraceObserver(session))
        return result
    }
}
