package nextflow.fovus

import groovy.util.logging.Slf4j
import nextflow.exception.ProcessException
import nextflow.executor.BashWrapperBuilder
import nextflow.fovus.job.FovusJobClient
import nextflow.fovus.job.FovusJobConfig
import nextflow.fovus.job.FovusJobStatus
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus

import java.nio.file.Path

import static nextflow.processor.TaskStatus.*

/**
 * Implements a task handler for Fovus jobs
 */
@Slf4j
class FovusTaskHandler extends TaskHandler {
    private final Path exitFile

    private final Path wrapperFile

    private final Path outputFile

    private final Path errorFile

    private final Path logFile

    private final Path scriptFile

    private final Path inputFile

    private final Path traceFile

    private FovusExecutor executor

    protected volatile String jobId;

    protected FovusJobConfig jobConfig;

    protected FovusJobClient jobClient;

    private List<FovusJobStatus> RUNNING_STATUSES = [
            FovusJobStatus.CREATED,
            FovusJobStatus.PENDING,
            FovusJobStatus.RUNNING,
            FovusJobStatus.REQUEUED,
            FovusJobStatus.PROVISIONING_INFRASTRUCTURE,
    ]

    FovusTaskHandler(TaskRun task, FovusExecutor executor) {
        super(task)
        this.executor = executor
        this.logFile = task.workDir.resolve(TaskRun.CMD_LOG)
        this.scriptFile = task.workDir.resolve(TaskRun.CMD_SCRIPT)
        this.inputFile = task.workDir.resolve(TaskRun.CMD_INFILE)
        this.outputFile = task.workDir.resolve(TaskRun.CMD_OUTFILE)
        this.errorFile = task.workDir.resolve(TaskRun.CMD_ERRFILE)
        this.exitFile = task.workDir.resolve(TaskRun.CMD_EXIT)
        this.wrapperFile = task.workDir.resolve(TaskRun.CMD_RUN)
        this.traceFile = task.workDir.resolve(TaskRun.CMD_TRACE)

        this.jobConfig = new FovusJobConfig(task)
        this.jobClient = new FovusJobClient(executor.config, jobConfig)
    }

    /**
     * {@inheritDoc}
     */
    @Override
    boolean checkIfRunning() {
        if (!jobId || !isSubmitted()) {
            return false
        }

        final jobStatus = jobClient.getJobStatus(jobId)
        final isRunning = jobStatus in RUNNING_STATUSES

        if (isRunning) {
            status = TaskStatus.RUNNING
        }

        return isRunning
    }


    @Override
    boolean checkIfCompleted() {
        assert jobId

        if (isCompleted()) {
            return true
        }

        if (!isRunning()) {
            return false
        }

        final jobStatus = jobClient.getJobStatus(jobId)
        final isJobTerminated = jobStatus in [FovusJobStatus.COMPLETED, FovusJobStatus.FAILED, FovusJobStatus.WALLTIME_REACHED, FovusJobStatus.TERMINATED]

        if (!isJobTerminated) {
            return false
        }

        task.stdout = outputFile

        // TODO: Download and read the exit file. Assuming successful exit for now
        // task.exitStatus = readExitFile()
        task.exitStatus = 0

        if (jobStatus != FovusJobStatus.COMPLETED) {
            task.stderr = errorFile

            switch (jobStatus) {
                case FovusJobStatus.FAILED:
                    task.error = new ProcessException("Job ${jobId} failed")
                    break
                case FovusJobStatus.WALLTIME_REACHED:
                    task.error = new ProcessException("Job ${jobId} walltime reached")
                    break;
                case FovusJobStatus.TERMINATED:
                    task.error = new ProcessException("Job ${jobId} terminated")
                    break
            }
        }

        status = TaskStatus.COMPLETED

        final jobDirectoryPath = task.workDir.getParent().toString()
        jobClient.downloadJobOutputs(jobDirectoryPath)
        return true
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void killTask() {
        assert jobId

        log.trace "[FOVUS] Terminating job > $task"
        jobClient.terminateJob(jobId)
    }

    @Override
    void prepareLauncher() {
        createTaskWrapper().build()
    }

    protected BashWrapperBuilder createTaskWrapper() {
        return new BashWrapperBuilder(task)
    }

    @Override
    void submit() {
        final jobConfigFilePath = jobConfig.toJson()
        final jobDirectory = task.workDir.getParent().toString()

        log.trace "[FOVUS] Submitting job > $task"
        jobId = jobClient.createJob(jobConfigFilePath, jobDirectory, jobConfig.jobName)

        status = TaskStatus.SUBMITTED
    }

    private int readExitFile() {
        try {
            exitFile.text as Integer
        }
        catch (Exception e) {
            log.debug "[FOVUS] Cannot read exit status for task: `${task.lazyName()}` | ${e.message}"
            return Integer.MAX_VALUE
        }
    }

    boolean isNew() { return status == NEW }

    boolean isSubmitted() { return status == SUBMITTED }

    boolean isRunning() { return status == RUNNING }

    boolean isCompleted() { return status == COMPLETED }

    boolean isActive() { status == SUBMITTED || status == RUNNING }
}
