package nextflow.fovus

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.processor.TaskBean
import nextflow.processor.TaskProcessor
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

    @Override
    String getEnvScript(Map environment, boolean container) {
        final result = new StringBuilder()
        final copy = environment ? new LinkedHashMap<String, String>(environment) : Collections.<String, String> emptyMap()

        final path = copy.containsKey('PATH')
        // remove any external PATH
        if (path)
            copy.remove('PATH')

        if (!executor.remoteBinDir) return super.getEnvScript(copy, container)

        // create the *bash* environment script
        if( !container ) {
            result << "export PATH=${executor.remoteBinDir}:\$PATH\n"

            final envScript = super.getEnvScript(copy, false)
            if (envScript) result << envScript

            return result.toString()
        }

        final wrapper = new StringBuilder()
        wrapper << "nxf_container_env() {\n"
        wrapper << 'cat << EOF\n'
        wrapper << TaskProcessor.bashEnvironmentScript(environment, true)
        wrapper << "export PATH=${executor.remoteBinDir}:${Escape.variable("\$PATH")}\n"
        wrapper << 'EOF\n'
        wrapper << '}\n'
        return wrapper.toString()
    }

    /**
     * {@inheritDoc}
     *
     * Additionally, change the permission of the file to 777 so the remote mount point can access the input file
     */
    @Override
    String stageInputFile(Path path, String targetName) {
        Path remotePath = executor.getRemotePath(path)
        def stageCmd = super.stageInputFile(remotePath, targetName)

        // Change file permission to 777 in background so they can be executable on the compute node
        "chmod 777 ${Escape.path(path.toAbsolutePath().toString())}".execute()
        return stageCmd
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
