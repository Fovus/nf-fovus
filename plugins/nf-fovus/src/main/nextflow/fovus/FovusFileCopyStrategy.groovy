package nextflow.fovus

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.processor.TaskBean
import nextflow.util.Escape

import java.nio.file.Path

/**
 * Defines the script operation to handle remote files staging on Fovus
 *
 */
@Slf4j
@CompileStatic
class FovusFileCopyStrategy extends SimpleFileCopyStrategy {

    private FovusExecutor executor;

    FovusFileCopyStrategy(TaskBean task, FovusExecutor executor) {
        super(task)
        this.executor = executor
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String stageInputFile(Path path, String targetName) {
        def fovusRemotePath = FovusUtil.getFovusRemotePath(executor, this.workDir, path)
        if (fovusRemotePath == null) {
            return 'true'
        }

        return super.stageInputFile(fovusRemotePath, targetName)
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String touchFile(Path file) {
        "touch ${Escape.path(file.getFileName())}"
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String fileStr(Path path) {
        Escape.path(path.getFileName())
    }

    /**
     * {@inheritDoc}
     */
    String exitFile(Path path) {
        "> ${Escape.path(path.getFileName())}"
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String copyFile(String name, Path target) {
        final targetWorkDir = FovusUtil.getWorkDirOfFile(executor.getWorkDir(), target)
        final remoteTargetPath = target.toString().replace(targetWorkDir.parent.toString(),
                "/compute_workspace")

        return "cp ${Escape.path(name)} ${Escape.path(Path.of(remoteTargetPath))}"
    }


    /**
     * {@inheritDoc}
     */
    @Override
    String pipeInputFile(Path path) {
        " < ${Escape.path(path.getFileName())}"
    }


}
