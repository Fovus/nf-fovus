package fovus.plugin.observers

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.trace.TraceObserverFactoryV2
import nextflow.trace.TraceObserverV2

@CompileStatic
class FovusTraceObserverFactory implements TraceObserverFactoryV2 {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        final List<TraceObserverV2> observers = new ArrayList<>()
        observers.add(new FovusTraceObserver(session))
        observers.add(new FovusPublishDirObserver(session))
        return observers
    }
}
