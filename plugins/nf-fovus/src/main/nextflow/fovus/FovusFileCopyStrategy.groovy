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


    String stageInputFile(Path path, Path inputWorkDir, String targetName) {
        def fovusRemotePath = FovusUtil.getFovusRemotePath(executor, path);

        log.debug("fovusRemotePath: $fovusRemotePath");
        log.debug("targetName: $targetName");

        if (fovusRemotePath == null) {

            try {
                // Ensure the path is relative to inputWorkDir
                def relativePath = inputWorkDir.relativize(path).toString()

                // Safely drop the first segment (if present)
                def parts = relativePath.split("/", 2)
                def relative = (parts.size() > 1) ? parts[1] : parts[0]

                if (relative != targetName) {
                    return stageInputFileHelper("../${relative}", targetName)
                }
                return "true"   // return string for consistency
            } catch(Exception e) {
                // Log and fail gracefully instead of throwing
                log.error("stageInputFile error: path=${path}, inputWorkDir=${inputWorkDir}, targetName=${targetName}, ex=${e.message}")
                return "true"
            }
        }
        return stageFovusRemoteInputFile(fovusRemotePath, targetName)
    }

    String stageInputFileHelper(String source, String targetName) {
        def cmd = ''
        def p = targetName.lastIndexOf('/')
        if( p>0 ) {
            cmd += "mkdir -p ${Escape.path(targetName.substring(0,p))} && "
        }
        log.debug("stageinMode --> $stageinMode")
        cmd += stageInCommand(source, targetName, stageinMode)

        return cmd
    }

    String stageFovusRemoteInputFile(Path path, String targetName) {
        def cmd = ''
        def p = targetName.lastIndexOf('/')
        if( p>0 ) {
            cmd += "mkdir -p ${Escape.path(targetName.substring(0,p))} && "
        }
        log.debug("stageinMode --> $stageinMode --> path -- $path")
        if (Files.isDirectory(path)) {
            log.debug("It's a directory - $path")
        } else if (Files.isRegularFile(path)) {
            log.debug( "It's a file - $path")
        }
        cmd += "cp ${path.toAbsolutePath().toString()} ${targetName} -r"
        return cmd
    }

    @Override
    protected String stageInCommand( String source, String target, String mode ) {
        if( !target || target.startsWith('/') )
            throw new IllegalArgumentException("Process input file target path must be relative: $target")

        if( mode == 'symlink' || !mode )
            return "ln -s ${Escape.path(source)} ${Escape.path(target)} || true"

        if( mode == 'rellink' ) {
            // GNU ln has the '-r' flag, but BSD ln doesn't, so we have to
            // manually resolve the relative path.

            def targetPath = workDir.resolve(target)
            def sourcePath = workDir.resolve(source)
            source = targetPath.getParent().relativize(sourcePath).toString()

            return "ln -sf ${Escape.path(source)} ${Escape.path(target)}"
        }

        if( mode == 'link' )
            return "ln ${Escape.path(source)} ${Escape.path(target)}"

        if( mode == 'copy' || !mode )
            return "cp -fRL ${Escape.path(source)} ${Escape.path(target)}"

        throw new IllegalArgumentException("Unknown stage-in strategy: $mode")
    }

    @Override
    String getStageInputFilesScript(Map<String,Path> inputFiles) {
        assert inputFiles != null

        def len = inputFiles.size()
        def links = []
        for( Map.Entry<String,Path> entry : inputFiles ) {
            final stageName = entry.key
            final storePath = entry.value

            log.debug("stageName: $stageName");
            log.debug("storePath: $storePath");
            log.debug("stageName: $stageName");

            final inputWorkDir = FovusUtil.getWorkDirOfFile(executor.getWorkDir(), storePath)
            final jobId = FovusUtil.getJobId(executor, inputWorkDir);

            log.debug("inputWorkDir: $inputWorkDir");
            log.debug("jobId: $jobId");

            // link them
            links << stageInputFile(storePath, inputWorkDir, stageName )
        }

        // return a big string containing the command
        return (links).join(separatorChar)
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
