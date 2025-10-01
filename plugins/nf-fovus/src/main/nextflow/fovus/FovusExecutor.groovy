package nextflow.fovus

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.executor.TaskArrayExecutor
import nextflow.fovus.juicefs.FovusJuiceFsClient
import nextflow.fovus.pipeline.FovusPipelineClient
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint

import java.nio.file.Path

@Slf4j
@ServiceName('fovus')
@CompileStatic
class FovusExecutor extends Executor implements ExtensionPoint, TaskArrayExecutor {

    protected FovusConfig config

    protected FovusPipelineClient pipelineClient;
    protected FovusJuiceFsClient juiceFsClient;

    /**
     * Map the local work directory with Fovus job id
     */
    volatile Map<String, String> jobIdMap = [:]

    Map<String, String> getJobIdMap() { jobIdMap }

    /**
     * @return The monitor instance that monitor submitted Fovus jobs
     */
    @Override
    protected TaskMonitor createTaskMonitor() {
        return TaskPollingMonitor.create(session, name, 1000, Duration.of("10 sec"))
    }

    @Override
    protected void register() {
        super.register()

        config = new FovusConfig(session.config.navigate('fovus') as Map);
        log.debug "[FOVUS] Creating fovus pipeline."
        this.pipelineClient = new FovusPipelineClient();

        FovusPipelineCache.getOrCreatePipelineId(this.pipelineClient, config, this.config.getPipelineName())

        juiceFsClient = new FovusJuiceFsClient(config)
        validateWorkDir()
    }

    private void validateWorkDir() {
        // Or should we auto map to session.workDir/pipelines?
        assert session.workDir.endsWith("pipelines"), "[FOVUS] Working directory must end with pipelines. Current work directory: ${session.workDir}"


        // Get all JuiceFs mount points
        // Example output: JuiceFS:juiceFsName on /mnt/mountPoint
        def getJuiceFsMountsCmd = ['mount', '|', 'grep', 'JuiceFS']
        def result = getJuiceFsMountsCmd.execute().text

        def juiceFsMounts = result.split('\n')
        if (juiceFsMounts.length == 0) {
            throw new RuntimeException("No JuiceFS mount was found")
        }

        final juiceFsStatus = juiceFsClient.getJuiceFsStatus()
        for (def mount : juiceFsMounts) {
            if (mount.isEmpty()) {
                continue
            }
            final parts = mount.split(" ")
            final fsName = parts[0].split(":")[1]
            final mountPoint = parts[2]

            final matchedFsName = juiceFsStatus.setting.name == fsName
            final hasMountSession = juiceFsStatus.sessions.any { it.mountPoint == mountPoint }
            final isValidaWorkDir = session.workDir.parent.toAbsolutePath().toString() == mountPoint

            if (matchedFsName && hasMountSession && isValidaWorkDir) {
                return
            }

            throw new RuntimeException("[FOVUS] Invalid working directory: ${session.workDir}")
        }

    }

    @Override
    boolean isForeignFile(Path path) {
        // TODO: update dynamic mount point from configuration
        if(path.toAbsolutePath().startsWith("/mnt/juicefs/")){
            return false
        }
        return super.isForeignFile(path)
    }

    /**
     * Create as task handler for each of Fovus job
     *
     * @param task The {@link TaskRun} instance to be executed
     * @return A {@FovusTaskHandler} for the given task
     */
    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir

        if(task.inputs.size() > 0){
            log.debug "[FOVUS] Moving local files > ${task}"
        }

        log.debug "[FOVUS] Launching process > ${task.name} -- work folder: ${task.workDir}"
        return new FovusTaskHandler(task, this)
    }

    @Override
    String getArrayIndexName() {
        return "FOVUS_TASK_ARRAY"
    }

    @Override
    int getArrayIndexStart() {
        return 0
    }

    @Override
    String getArrayTaskId(String jobId, int index) {
        return "${jobId}:${index}"
    }

    @Override
    String getArrayLaunchCommand(String taskDir) {
        return TaskArrayExecutor.super.getArrayLaunchCommand(taskDir);
    }
}
