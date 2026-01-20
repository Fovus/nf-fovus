package fovus.plugin

import fovus.plugin.job.FovusJobConfig
import groovy.util.logging.Slf4j
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskBean


@Slf4j
class FovusScriptLauncher extends BashWrapperBuilder {
    private final boolean isMemoryCheckpointingEnabled;
    private FovusJobConfig jobConfig;

    FovusScriptLauncher(TaskBean bean, FovusExecutor executor, FovusJobConfig jobConfig, boolean isMemoryCheckpointingEnabled) {
        super(bean, new FovusFileCopyStrategy(bean, executor))
        this.isMemoryCheckpointingEnabled = isMemoryCheckpointingEnabled;
        this.jobConfig = jobConfig;
    }

    @Override
    protected String getLaunchCommand(String interpreter, String env) {
        final originalLaunchCommand = super.getLaunchCommand(interpreter, env);

        if (!isMemoryCheckpointingEnabled || containerConfig.engine !== 'docker' ||
            !originalLaunchCommand.contains('docker')) {
            return originalLaunchCommand;
        }

        // The launch command will contain 2 parts: the docker run part and the actual command to run. We will extract them and add our memory checkpointing wrapper logic. We can do this by splitting by finding the index of docker image name.
        def dockerImageIndex = originalLaunchCommand.indexOf(containerImage);

        if (dockerImageIndex == -1) {
            log.debug "[FOVUS] Docker image name not found in the launch command. Disabling memory checkpointing..."
            jobConfig.constraints.jobConstraints.isMemoryAutoRetryEnabled = false;
            return originalLaunchCommand;
        }

        def dockerRunCommand = originalLaunchCommand.substring(0, dockerImageIndex + containerImage.length());
        def commandToRun = originalLaunchCommand.substring(dockerImageIndex + containerImage.length() + 1).trim();

        // The commandToRun could be a string of the following format:
        // 1. /bin/bash -c "eval $(nxf_container_env); /bin/bash -C -e -u -o pipefail .command.run
        // 2. Or, just /bin/bash -C -e -u -o pipefail .command.run
        // We need to separate it into 2 parts: the eval part and the bash part as follows:
        // 1. eval $(nxf_container_env) (could be empty)
        // 2. bash -C -e -u -o pipefail .command.run
        String containerEnvCommand = 'eval $(nxf_container_env);'
        def containerEnvCommandIndex = commandToRun.indexOf(containerEnvCommand);
        if (containerEnvCommandIndex == -1) {
            containerEnvCommand = "";
        } else {
            commandToRun = commandToRun.substring(containerEnvCommandIndex + containerEnvCommand.length() + 1,
                                                  commandToRun.length()).trim();
            if (commandToRun.endsWith('"')) {
                commandToRun = commandToRun.substring(0, commandToRun.length() - 1).trim();
            }
        }

        def memCheckpointWrapperCommand = """
source "/script/\${FOVUS_JOB_ID}/checkpoint_template_helper.sh"

container_id=\$(${dockerRunCommand} bash -lc "sleep infinity" )
register_checkpoint_runtime --container-id \${container_id}

if ! try_restore_and_wait "\$container_id"; then

    docker exec \${container_id} bash -lc '
        '"${containerEnvCommand}"'
        setsid bash -lc "
            (
                set +e
                ${commandToRun}
                rc=\$?
                echo \$rc > .app.exit
            ) >> .app.stdout 2>> .app.stderr
        " &
        pid=\$!
        echo \$pid > .app.pid
    '
    PID=\$(cat .app.pid)
    register_checkpoint_runtime --pid \${PID}
    wait_for_pid_exit_in_container \${container_id} \${PID}
fi

if [[ -s .app.exit ]]; then
    exit_code=\$(cat .app.exit)
    rm -f .app.exit .app.pid
    exit \$exit_code
fi

if [[ -f .app.stdout ]]; then
    cat .app.stdout
fi

if [[ -f .app.stderr ]]; then
    cat .app.stderr >&2
fi
        """

        return memCheckpointWrapperCommand;
    }
}
