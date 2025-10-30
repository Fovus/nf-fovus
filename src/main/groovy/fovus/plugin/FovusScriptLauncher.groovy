package fovus.plugin

import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskBean

class FovusScriptLauncher extends BashWrapperBuilder {
    
    FovusScriptLauncher(TaskBean bean, FovusExecutor executor) {
        super(bean, new FovusFileCopyStrategy(bean, executor))
    }
}
