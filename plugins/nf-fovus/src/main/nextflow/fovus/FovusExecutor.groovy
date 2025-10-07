package nextflow.fovus

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.executor.TaskArrayExecutor
import nextflow.extension.FilesEx
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
    private static final String REMOTE_JUICEFS_MOUNT_POINT = '/fovus-juicefs'
    protected FovusConfig config

    protected FovusPipelineClient pipelineClient;
    protected FovusJuiceFsClient juiceFsClient;
    protected Path juiceFsMountDir;
    protected Path remoteBinDir;

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
        uploadBinDir()
    }

    private void validateWorkDir() {
        // Or should we auto map to session.workDir/pipelines?
        assert session.workDir.endsWith("pipelines"), "[FOVUS] Working directory must end with pipelines. Current work directory: ${session.workDir}"
        juiceFsClient.validateOrMountJuiceFs(session.workDir.parent)
        juiceFsMountDir = session.workDir.parent
    }

    protected void uploadBinDir() {
        /*
         * upload local binaries
         */
        if (session.binDir && !session.binDir.empty() && !session.disableRemoteBinDir) {
            def tempDir = getTempDir()
            def copyBinDir = FilesEx.copyTo(session.binDir, tempDir)
            remoteBinDir = getRemotePath(copyBinDir)

            // Change permission to executable in background
            def changePermissionCmd = "chmod -R 755 ${copyBinDir}"
            changePermissionCmd.execute()
        }
    }

    @PackageScope
    Path getRemoteBinDir() {
        return remoteBinDir
    }

    @Override
    Path getWorkDir() {
        return session.workDir.resolve(this.pipelineClient.getPipeline().pipelineId)
    }

    @Override
    boolean isForeignFile(Path path) {
        return true
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

    Path getRemotePath(Path file) {
        // Replace the juicefs mount point part with the REMOTE_JUICEFS_MOUNT_POINT
        return Path.of(REMOTE_JUICEFS_MOUNT_POINT, file.toString().replace(juiceFsMountDir.toString(), ""))
    }

}
