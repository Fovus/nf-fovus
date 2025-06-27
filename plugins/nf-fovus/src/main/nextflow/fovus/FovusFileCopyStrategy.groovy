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
        def fovusRemotePath = getFovusRemotePath(path)
        if (fovusRemotePath == path) {
            return 'true'
        }

        return super.stageInputFile(fovusRemotePath, targetName)
    }

    /**
     * Get the workDir of a file (eg, an output of a previous task) based on the workDir of the
     * current task
     *
     */
    private Path getWorkDirOfFile(Path file) {
        final sessionWorkDir = executor.getWorkDir()
        final relativePath = sessionWorkDir.relativize(file);
        final fileWorkDir = sessionWorkDir.resolve(relativePath.subpath(0, 2))

        return fileWorkDir
    }

    private Path getFovusRemotePath(Path path) {
        final jobIdMap = executor.getJobIdMap()
        final inputWorkDir = getWorkDirOfFile(path)

        final jobId = jobIdMap.get(inputWorkDir.toString())

        if (!jobId) {
            // This could be a local input files, return the original path
            return path
        }

        // Replace the workDir in path with /fovus-storage/jobs/jobId
        final fovusStorageRemotePath = path.toString().replace(inputWorkDir.parent.toString(),
                "/fovus-storage/jobs/$jobId")

        log.trace "[FOVUS] Fovus remote path for file ${path} is ${fovusStorageRemotePath}"
        return Path.of(fovusStorageRemotePath)
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
        final targetWorkDir = getWorkDirOfFile(target)
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
