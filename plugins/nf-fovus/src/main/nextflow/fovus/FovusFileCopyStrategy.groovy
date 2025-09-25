package nextflow.fovus

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.processor.TaskBean
import nextflow.processor.TaskProcessor
import nextflow.util.Escape

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

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
        if( !environment )
            return null

        // create the *bash* environment script
        if( !container ) {
            return TaskProcessor.bashEnvironmentScript(environment)
        }
        else {
            log.debug("environment --> ${environment}")
            final wrapper = new StringBuilder()
            wrapper << "nxf_container_env() {\n"
            wrapper << 'cat << EOF\n'
            wrapper << TaskProcessor.bashEnvironmentScript(environment, true)
            wrapper << "chmod +x \$PWD/nextflow-bin/* || true\n"
            wrapper << "export PATH=\"\$PWD/nextflow-bin:\\\$PATH\" \n"
            wrapper << 'EOF\n'
            wrapper << '}\n'
            return wrapper.toString()
        }
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
