package nextflow.fovus

import groovy.util.logging.Slf4j
import nextflow.exception.ProcessException
import nextflow.executor.BashWrapperBuilder
import nextflow.fovus.job.FovusJobClient
import nextflow.fovus.job.FovusJobConfig
import nextflow.fovus.job.FovusJobStatus
import nextflow.processor.TaskArrayRun
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
        jobConfig.skipRemoteInputSync(executor)

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
        jobClient.downloadJobOutputs(jobDirectoryPath, jobId)
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
        return new FovusScriptLauncher(task.toTaskBean(), executor)
    }

    @Override
    void submit() {
        final jobConfigFilePath = jobConfig.toJson()
        final isTaskArrayRun = task instanceof TaskArrayRun;
        def jobDirectory = task.workDir.getParent().toString();

        if(isTaskArrayRun){
            jobDirectory = task.workDir.toString();
        }
        log.trace "[FOVUS] Submitting job > $task"
        jobId = jobClient.createJob(jobConfigFilePath, jobDirectory, jobConfig.jobName, isTaskArrayRun)
        updateStatus(jobId)

        executor.jobIdMap.put(task.workDir.toString(), jobId);
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

    protected void updateStatus(String jobId) {
        if( task instanceof TaskArrayRun ) {
            // update status for children tasks
            for( int i=0; i<task.children.size(); i++ ) {
                final handler = task.children[i] as FovusTaskHandler
                //TODO: pass task id after adding check task status endpoint
                handler.updateStatus(jobId)
            }
        }
        else {
            this.jobId = jobId
            this.status = TaskStatus.SUBMITTED
        }
    }

    boolean isNew() { return status == NEW }

    boolean isSubmitted() { return status == SUBMITTED }

    boolean isRunning() { return status == RUNNING }

    boolean isCompleted() { return status == COMPLETED }

    boolean isActive() { status == SUBMITTED || status == RUNNING }

    protected String normalizeJobName(String name) {
        def result = name.replaceAll(' ','_').replaceAll(/[^a-zA-Z0-9_-]/,'')
        result.size()>128 ? result.substring(0,128) : result
    }

    protected String getJobName(TaskRun task) {
        final result = prependWorkflowPrefix(task.name, environment)
        return normalizeJobName(result)
    }
}
