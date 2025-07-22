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
        def fovusRemotePath = FovusUtil.getFovusRemotePath(executor, path)
        if (fovusRemotePath == null) {
            return 'true'
        }

        return super.stageInputFile(fovusRemotePath, targetName)
    }

    @Override
    String getStageInputFilesScript(Map<String,Path> inputFiles) {
        assert inputFiles != null

        def len = inputFiles.size()
        def delete = []
        def links = []
        for( Map.Entry<String,Path> entry : inputFiles ) {
            final stageName = entry.key
            final storePath = entry.value

            final inputWorkDir = FovusUtil.getWorkDirOfFile(executor.getWorkDir(), storePath)
            final jobId = FovusUtil.getJobId(executor, inputWorkDir);

            if(jobId == null){
                continue
            }
            // Delete all previous files with the same name
            // Note: the file deletion is only needed to prevent
            // file name collisions when re-running the runner script
            // for debugging purpose. However, this can cause the creation
            // of a very big runner script when a large number of files is
            // given due to the file name duplication. Therefore the rationale
            // here is to keep the deletion only when a file input number is
            // given (which is more likely during pipeline development) and
            // drop in any case  when they are more than 100
            if( len<100 )
                delete << "rm -f ${Escape.path(stageName)}"

            // link them
            links << stageInputFile( storePath, stageName )
        }

        // return a big string containing the command
        return (delete + links).join(separatorChar)
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
