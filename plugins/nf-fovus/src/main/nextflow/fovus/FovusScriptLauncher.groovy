package nextflow.fovus

import nextflow.container.ContainerBuilder
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskBean
import nextflow.processor.TaskRun

import java.nio.file.Path

class FovusScriptLauncher extends BashWrapperBuilder {

    FovusScriptLauncher(TaskBean bean, FovusExecutor executor) {
        super(bean, new FovusFileCopyStrategy(bean, executor))
//        // enable the copying of output file to the S3 work dir
//        if( scratch==null )
//            scratch = true
        // include task script as an input to force its staging in the container work directory
        bean.inputFiles[TaskRun.CMD_SCRIPT] = bean.workDir.resolve(TaskRun.CMD_SCRIPT)
        // add the wrapper file when stats are enabled
        // NOTE: this must match the logic that uses the run script in BashWrapperBuilder
        if (isTraceRequired()) {
            bean.inputFiles[TaskRun.CMD_RUN] = bean.workDir.resolve(TaskRun.CMD_RUN)
        }
        // include task stdin file
        if (bean.input != null) {
            bean.inputFiles[TaskRun.CMD_INFILE] = bean.workDir.resolve(TaskRun.CMD_INFILE)
        }
    }

//    @Override
//    ContainerBuilder createContainerBuilder(String changeDir) {
//        ContainerBuilder builder = super.createContainerBuilder(changeDir)
//        builder.addMount(Path.of("/fovus/archive"))
//        builder.addMount(Path.of("/fovus-storage"))
//
//        builder.build()
//        return builder
//    }

    @Override
    protected boolean fixOwnership() {
        return containerConfig?.fixOwnership
    }
}


