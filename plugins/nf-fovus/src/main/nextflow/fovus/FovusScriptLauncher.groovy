package nextflow.fovus

import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskBean
import nextflow.processor.TaskRun

import java.nio.file.Path

class FovusScriptLauncher extends BashWrapperBuilder {
    
    FovusScriptLauncher(TaskBean bean, FovusExecutor executor) {
        super(bean, new FovusFileCopyStrategy(bean, executor))
    }
}
