package fovus.plugin

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

    String getBeforeStartScript() {
        return fovusLink()
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
        if (copy) {
            wrapper << TaskProcessor.bashEnvironmentScript(environment, true)
        }
        wrapper << "export PATH=\"${executor.remoteBinDir}:${Escape.variable("\$PATH")}\"\n"
        wrapper << 'EOF\n'
        wrapper << '}\n'
        return wrapper.toString()
    }

    String getStageInputFilesScript(Map<String, Path> inputFiles) {
        assert inputFiles != null

        def len = inputFiles.size()
        def delete = []
        def links = []
        for (Map.Entry<String, Path> entry : inputFiles) {
            final stageName = entry.key
            final storePath = entry.value

            // link them
            links << stageInputFile(storePath, stageName)
        }

        // return a big string containing the command
        return (delete + links).join(separatorChar)
    }

    /**
     * {@inheritDoc}
     *
     * Additionally, change the permission of the file to 777 so the remote mount point can access the input file
     */
    @Override
    String stageInputFile(Path path, String targetName) {
        Path remotePath = executor.getRemotePath(path)
        def stageCmd = "fovus_link ${remotePath} ${targetName}"
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

    protected String fovusLink() {
        """
        fovus_link() {
            local src="\$1"
            local target="\$2"
    
            if [[ -z "\$src" || -z "\$target" ]]; then
                echo "Usage: fovus_link <src-file-or-dir> <target-file-or-dir>" >&2
                return 1
            fi
    
            # If src is a file → link to target path in current dir
            if [[ -f "\$src" ]]; then
                # Ensure target's parent dir exists (if target contains '/')
                mkdir -p "\$(dirname "\$target")" 2>/dev/null || true
    
                ln -s "\$(realpath "\$src")" "\$PWD/\$target" || true
                return 0
            fi
    
            # If src is a directory → mirror structure under target dir
            if [[ -d "\$src" ]]; then
                mkdir -p "\$PWD/\$target" || true
    
                find "\$src" -type f | while IFS= read -r file; do
                    rel="\${file#\$src/}"                      # path relative to src root
                    dest="\$PWD/\$target/\$rel"               # full destination path
    
                    mkdir -p "\$(dirname "\$dest")"
                    ln -s "\$(realpath "\$file")" "\$dest" || true
                done
                return 0
            fi
    
            echo "Not a file or directory: \$src" >&2
            return 1
        }
    """.stripIndent(true)
    }

}
