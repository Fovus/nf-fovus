package fovus.plugin

import fovus.plugin.pipeline.FovusPipelineClient
import fovus.plugin.storage.WorkDirStorage
import fovus.plugin.storage.WorkDirStorageFactory
import fovus.plugin.util.FovusEnvironment
import fovus.plugin.util.PublishDirResolver
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.executor.TaskArrayExecutor
import nextflow.extension.FilesEx
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
    protected FovusConfig fovusConfig

    protected FovusPipelineClient pipelineClient;
    /** Where the work directory lives: a Fovus storage mount, or Fovus storage itself (direct mode) */
    protected WorkDirStorage workDirStorage
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
        return TaskPollingMonitor.create(session, config, name, Duration.of("10 sec"))
    }

    @Override
    protected void register() {
        super.register()

        final isHostedMode = FovusEnvironment.isHostedMode()
        fovusConfig = FovusConfig.fromSession(session);
        // Pick and validate the storage mode from workDir before anything is created
        workDirStorage = WorkDirStorageFactory.create(session.workDir, isHostedMode, fovusConfig)

        if (!isHostedMode && fovusConfig.auth.isConfigured()) {
            warmUpAuth(fovusConfig)
        }

        log.debug "[FOVUS] Creating fovus pipeline."
        this.pipelineClient = new FovusPipelineClient();

        final pipelineId = FovusPipelineCache.getOrCreatePipelineId(this.pipelineClient, fovusConfig,
                                                                    this.fovusConfig.getPipelineName(),
                                                                    session?.getCommandLine())

        workDirStorage.prepare(pipelineId)
        uploadBinDir()

        if (isHostedMode) {
            PublishDirResolver.initialize(
                FovusEnvironment.getFovusUserBucket() ?: '',
                FovusEnvironment.getPipelineId() ?: ''
            )
        }
    }

    /**
     * One `fovus auth user` call, single-threaded, before any Nextflow task runs, so the Fovus CLI's
     * per-PAT cache is warm before task fan-out, and a bad configured credential fails clearly here
     * rather than on whichever task happens to run first.
     */
    private void warmUpAuth(FovusConfig config) {
        log.debug "[FOVUS] Warming up Fovus CLI authentication"
        final result = FovusUtil.executeCommand([config.getCliPath(), 'auth', 'user'], config.cliEnv())

        if (result.exitCode != 0) {
            throw new RuntimeException("[FOVUS] Failed to authenticate with Fovus using the configured " +
                    "fovus.auth credentials: ${config.redactSecret(result.error)}")
        }
    }

    protected void uploadBinDir() {
        /*
         * upload local binaries
         */
        if (session.binDir && !session.binDir.empty() && !session.disableRemoteBinDir) {
            def tempDir = getTempDir()
            def copyBinDir = FilesEx.copyTo(session.binDir, tempDir)
            // No chmod: Fovus storage mounts fix file modes at mount time (0770), so the scripts are executable
            remoteBinDir = getRemotePath(copyBinDir)
        }
    }

    @PackageScope
    Path getRemoteBinDir() {
        return remoteBinDir
    }

    @Override
    Path getWorkDir() {
        return session.workDir
                .resolve(this.pipelineClient.getPipeline().pipelineId)
                .resolve("fovus-work")
    }

    /**
     * Not literally true -- fovus has no native secrets provider -- but it stops Nextflow
     * from invoking the global {@code SecretsProvider} when building the task wrapper, which
     * can otherwise fail task submission depending on what other plugins are loaded.
     */
    @Override
    boolean isSecretNative() {
        return true
    }

    @Override
    boolean isForeignFile(Path path) {
        return workDirStorage.isForeignFile(path)
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

    /** The path as the compute node sees it, under /fovus-storage */
    Path getRemotePath(Path file) {
        return workDirStorage.remotePath(file)
    }

}
