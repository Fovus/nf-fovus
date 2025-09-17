package nextflow.fovus

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.executor.TaskArrayExecutor
import nextflow.extension.FilesEx
import nextflow.fovus.nio.FovusS3Path
import nextflow.fovus.nio.util.FovusJobCache
import nextflow.fovus.pipeline.FovusPipelineClient
import nextflow.processor.TaskArrayRun
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint

import java.nio.file.FileSystemNotFoundException
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.Paths

@Slf4j
@ServiceName('fovus')
@CompileStatic
class FovusExecutor extends Executor implements ExtensionPoint, TaskArrayExecutor {

    protected FovusConfig config

    protected FovusPipelineClient pipelineClient;

    /**
     * Map the local work directory with Fovus job id
     */
    volatile Map<String, String> jobIdMap = [:]

    Map<String, String> getJobIdMap() { jobIdMap }

    /**
     * A S3 path where executable scripts need to be uploaded
     */
    private Path remoteBinDir = null

    /**
     * @return The monitor instance that monitor submitted Fovus jobs
     */
    @Override
    protected TaskMonitor createTaskMonitor() {
        return TaskPollingMonitor.create(session, name, 1000, Duration.of("10 sec"))
    }

    @Override
    Path getWorkDir() {
        def pipelineId = pipelineClient.getPipeline().getPipelineId();
        // Need to use /// to match the expected FovusS3FileSystem uri schema

        def workDirUri = URI.create("fovus:///fovus-storage/$pipelineId")
        Path workDir = null
        try {
            workDir = Paths.get(workDirUri);
        } catch (FileSystemNotFoundException e) {
            FileSystems.newFileSystem(workDirUri, session.config)
            workDir = Paths.get(workDirUri)
        }

        return workDir
    }

    @PackageScope
    Path getRemoteBinDir() {
        remoteBinDir
    }

    @Override
    protected void register() {
        super.register()

        config = new FovusConfig(session.config.navigate('fovus') as Map);
        log.debug "[FOVUS] Creating fovus pipeline"
        this.pipelineClient = new FovusPipelineClient();
        log.debug("session --> ${this.session}")
        log.debug("name --> ${this.name}")
        log.debug("pipelineName --> ${config.getPipelineName()}")

        FovusJobCache.getOrCreatePipelineId(this.pipelineClient, this.config, this.config.getPipelineName())

        uploadBinDir()
    }

    protected void uploadBinDir() {
        /*
         * upload local binaries
         */
        if (session.binDir && !session.binDir.empty() && !session.disableRemoteBinDir) {
            def s3 = getTempDir()
            log.info "Uploading local `bin` scripts folder to ${s3.toUriString()}/bin"
            remoteBinDir = FilesEx.copyTo(session.binDir, s3)
        }
    }

    @Override
    boolean isContainerNative() {
        return false;
    }

    @Override
    String containerConfigEngine() {
        return 'docker'
    }

    /**
     * @return {@code true} whenever the secrets handling is managed by the executing platform itself
     */
    @Override
    final boolean isSecretNative() {
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
