/*
 * Copyright 2013-2024, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.fovus

import nextflow.fovus.nio.FovusS3Path
import nextflow.processor.TaskProcessor

import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.fovus.util.S3BashLib
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.processor.TaskBean
import nextflow.util.Escape

/**
 * Defines the script operation to handle file when running in the Cirrus cluster
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class FovusFileCopyStrategy extends SimpleFileCopyStrategy {


    private Map<String, String> environment
    private FovusExecutor executor

    FovusFileCopyStrategy(TaskBean task, FovusExecutor executor) {
        super(task)
        this.executor = executor
        this.environment = task.environment
    }

    /**
     * @return A script snippet that download from S3 the task scripts:
     * {@code .command.env}, {@code .command.sh}, {@code .command.in},
     * etc.
     */
    String getBeforeStartScript() {
        S3BashLib.script()
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String getEnvScript(Map environment, boolean container) {
        final result = new StringBuilder()
        final copy = environment ? new LinkedHashMap<String, String>(environment) : Collections.<String, String> emptyMap()
        final path = copy.containsKey('PATH')
        // remove any external PATH
        if (path)
            copy.remove('PATH')

        if (!executor.remoteBinDir) return super.getEnvScript(copy, container)

        result << "[ -d \$PWD/nextflow-bin ] || cp -r /${getFovusMappingPath(executor.remoteBinDir)} \$PWD/nextflow-bin && chmod +x \$PWD/nextflow-bin/* || true\n"

        if (!container) {
            result << "export PATH=\$PWD/nextflow-bin:\$PATH\n"

            final envScript = super.getEnvScript(copy, false)
            if (envScript) result << envScript

            return result.toString()
        }

        final wrapper = new StringBuilder()
        wrapper << "nxf_container_env() {\n"
        wrapper << 'cat << EOF\n'
        if (copy) {
            wrapper << TaskProcessor.bashEnvironmentScript(copy, true)
        }
        wrapper << "chmod +x ${Escape.variable("\$PWD/nextflow-bin/*")} || true \n"
        wrapper << "export PATH=${Escape.variable("\$PWD/nextflow-bin:\$PATH")}\n"
        wrapper << 'EOF\n'
        wrapper << '}\n'

        result << wrapper.toString()
        return result.toString()
    }

    @Override
    String getStageInputFilesScript(Map<String, Path> inputFiles) {
        def result = 'downloads=(true)\n'
        result += getStageInputFilesScriptHelper(inputFiles) + '\n'
        result += 'nxf_parallel "${downloads[@]}"\n'
        return result
    }

    String getStageInputFilesScriptHelper(Map<String, Path> inputFiles) {
        assert inputFiles != null

        def len = inputFiles.size()
        def delete = []
        def links = []
        for (Map.Entry<String, Path> entry : inputFiles) {
            final stageName = entry.key
            final storePath = entry.value

            // Delete all previous files with the same name
            // Note: the file deletion is only needed to prevent
            // file name collisions when re-running the runner script
            // for debugging purpose. However, this can cause the creation
            // of a very big runner script when a large number of files is
            // given due to the file name duplication. Therefore the rationale
            // here is to keep the deletion only when a file input number is
            // given (which is more likely during pipeline development) and
            // drop in any case  when they are more than 100
            if (len < 100)
                delete << "rm -f ${Escape.path(stageName)}"

            // link them
            links << stageInputFile(storePath, stageName)
        }

        // return a big string containing the command
        return (delete + links).join(separatorChar)
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String stageInputFile(Path path, String targetName) {
        // third param should not be escaped, because it's used in the grep match rule
        def cmd = ''
        def p = targetName.lastIndexOf('/')
        if (p > 0) {
            cmd += "mkdir -p ${Escape.path(targetName.substring(0, p))} && "
        }
        cmd += "downloads+=(\"nxf_s3_download /${Escape.path(getFovusMappingPath(path))} ${Escape.path(targetName)}\")"
        return cmd
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String getUnstageOutputFilesScript(List<String> outputFiles, Path targetDir) {

        final patterns = normalizeGlobStarPaths(outputFiles)
        // create a bash script that will copy the out file to the working directory
        log.trace "[AWS BATCH] Unstaging file path: $patterns"

        if (!patterns)
            return null

        final escape = new ArrayList(outputFiles.size())
        for (String it : patterns)
            escape.add(Escape.path(it))

        return """\
            uploads=()
            IFS=\$'\\n'
            for name in \$(eval "ls -1d ${escape.join(' ')}" | sort | uniq); do
                uploads+=("nxf_s3_upload '\$name' /${Escape.path(getFovusMappingPath(targetDir))}")
            done
            unset IFS
            nxf_parallel "\${uploads[@]}"
            """.stripIndent(true)
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String touchFile(Path file) {
        "echo start | nxf_s3_upload - /${Escape.path(getFovusMappingPath(file))}"
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
    @Override
    String copyFile(String name, Path target) {
        "nxf_s3_upload ${Escape.path(name)} /${Escape.path(getFovusMappingPath(target.getParent()))}"
    }

    static String uploadCmd(String source, Path target) {
        "nxf_s3_upload ${Escape.path(source)} /${Escape.path(getFovusMappingPath(target))}"
    }

    /**
     * {@inheritDoc}
     */
    String exitFile(Path path) {
        // Write exit code to current working directory
//        "| nxf_s3_upload - /${Escape.path(getFovusMappingPath(path))} || true"
        final exitCodeFileName = path.getFileName().toString()
        " > ${Escape.path(exitCodeFileName)} || true"
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String pipeInputFile(Path path) {
        " < ${Escape.path(path.getFileName())}"
    }

    static String getFovusMappingPath(Path path) {
        FovusS3Path fovusPath = (FovusS3Path) path;
        String key = fovusPath.getKey();
        def result = ""
        if (fovusPath.isJobFile()) {
            result = "fovus-storage/jobs/${key.substring(key.indexOf('/') + 1)}"
        } else {
            result = "fovus-storage/files/${key}"
        }

        return result
    }
}
