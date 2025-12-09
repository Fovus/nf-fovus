package fovus.plugin

import groovy.util.logging.Slf4j
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskBean


@Slf4j
class FovusScriptLauncher extends BashWrapperBuilder {
    
    FovusScriptLauncher(TaskBean bean, FovusExecutor executor) {
        super(bean, new FovusFileCopyStrategy(bean, executor))
    }

    @Override
    protected String getLaunchCommand(String interpreter, String env) {
        String launcher = super.getLaunchCommand(interpreter, env)
        return patchDockerCommand(launcher);
    }

    String patchDockerCommand(String cmd) {
        // Must contain 'docker run '
        if (!cmd.contains("docker run")) {
            return cmd
        }

        // List of required fovus options
        Map<String, String> required = [
                "envFile"   : "--env-file /compute_workspace/.fovus_env",
                "fovusMount": "-v /fovus-storage-cached:/fovus-storage-cached",
                "pwdMount"  : "-v \$PWD:\$PWD",
                "fovusStorageMount": "-v /fovus-storage:/fovus-storage",
                "fovusArchive": "-v /fovus/archive:/fovus/archive",
                "memory": "--memory \${FovusOptVcpuMem}g"
        ]

        // Add missing fovus options
        required.each { key, option ->
            if (!cmd.contains(option)) {  // check only flag part
                cmd = cmd.replace("docker run", "docker run ${option}")
            }
        }

        return cmd
    }
}
