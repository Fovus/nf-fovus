package fovus.plugin

import groovy.util.logging.Slf4j
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskBean


@Slf4j
class FovusScriptLauncher extends BashWrapperBuilder {
    
    FovusScriptLauncher(TaskBean bean, FovusExecutor executor) {
        super(bean, new FovusFileCopyStrategy(bean, executor))
    }
}
