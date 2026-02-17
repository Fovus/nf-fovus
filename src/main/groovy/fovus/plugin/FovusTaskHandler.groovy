package fovus.plugin

import fovus.plugin.job.ContainerizedEnvironment
import groovy.util.logging.Slf4j
import nextflow.container.DockerConfig
import nextflow.exception.ProcessException
import nextflow.executor.BashWrapperBuilder
import fovus.plugin.job.FovusJobClient
import fovus.plugin.job.FovusJobConfig
import fovus.plugin.job.FovusJobStatus
import fovus.plugin.task.FovusTaskClient
import fovus.plugin.task.FovusTaskStatus
import nextflow.processor.TaskArrayRun
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import nextflow.util.Escape

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

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
    protected FovusTaskClient taskClient;

    private List<FovusJobStatus> RUNNING_JOB_STATUSES = [
            FovusJobStatus.PENDING,
            FovusJobStatus.PROVISIONING_INFRASTRUCTURE,
            FovusJobStatus.CREATED,
            FovusJobStatus.RUNNING,
            FovusJobStatus.REQUEUED,
    ]

    private List<FovusJobStatus> COMPLETED_JOB_STATUSES = [
            FovusJobStatus.COMPLETED,
            FovusJobStatus.FAILED,
            FovusJobStatus.WALLTIME_REACHED,
            FovusJobStatus.TERMINATED
    ]

    private List<FovusTaskStatus> RUNNING_RUN_STATUSES = [
            FovusTaskStatus.CREATED,
            FovusTaskStatus.RUNNING,
            FovusTaskStatus.REQUEUED,
    ]

    private List<FovusTaskStatus> COMPLETED_RUN_STATUSES = [
            FovusTaskStatus.COMPLETED,
            FovusTaskStatus.FAILED,
            FovusTaskStatus.WALLTIME_REACHED,
            FovusTaskStatus.TERMINATED,
            FovusTaskStatus.UNCOMPLETE
    ]

    final static FOVUS_JOB_CONFIG_FOLDER = "./work/.nextflow/fovus/job_config"

    FovusJobConfig getJobConfig() {
        return this.jobConfig
    }

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

        this.jobClient = new FovusJobClient(executor.fovusConfig)
        this.taskClient = new FovusTaskClient(executor.fovusConfig)

        if (task instanceof TaskArrayRun) {
            def children = task.getChildren() as List<FovusTaskHandler>;
            def firstTask = children.first();
            this.jobConfig = firstTask.getJobConfig();
        } else {
            this.jobConfig = new FovusJobConfig(this.jobClient, task)
        }

        this.jobClient.setJobConfig(this.jobConfig)
    }

    /**
     * {@inheritDoc}
     */
    @Override
    boolean checkIfRunning() {
        if (!jobId || !isSubmitted()) {
            return false
        }

        if (this.task instanceof TaskArrayRun) {
            log.debug("TaskArrayRun is detected: ${this.task} jobId: --> $jobId")

            final jobStatus = jobClient.getJobStatus(jobId)
            // Include completed statuses for very quick job that completed before running status is detected
            final isRunning = (jobStatus in RUNNING_JOB_STATUSES) || (jobStatus in COMPLETED_JOB_STATUSES)

            if (isRunning) {
                status = TaskStatus.RUNNING
            }

            return isRunning
        }
        final taskName = this.task.workDirStr.split("/")[-1];
        final taskStatus = taskClient.getTaskStatus(jobId, taskName)
        // Include completed statuses for very quick tasks that completed before running status is detected
        final isRunning = (taskStatus in RUNNING_RUN_STATUSES) || (taskStatus in COMPLETED_RUN_STATUSES)

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

        def taskStatus
        if (this.task instanceof TaskArrayRun) {
            log.debug("TaskArrayRun is detected: ${this.task} jobId: --> $jobId")
            taskStatus = jobClient.getJobStatus(jobId)
            final isJobTerminated = taskStatus in COMPLETED_JOB_STATUSES

            if (!isJobTerminated) {
                return false
            }
        } else {
            final taskName = this.task.workDirStr.split("/")[-1];
            taskStatus = taskClient.getTaskStatus(jobId, taskName)
            final isRunTerminated = taskStatus in COMPLETED_RUN_STATUSES

            if (!isRunTerminated) {
                return false
            }
        }

        task.stdout = outputFile

        task.exitStatus = readExitFile()

        if (taskStatus != FovusJobStatus.COMPLETED || taskStatus != FovusTaskStatus.COMPLETED) {
            task.stderr = errorFile

            switch (taskStatus) {
                case FovusJobStatus.FAILED:
                case FovusTaskStatus.FAILED:
                    task.error = new ProcessException("Job ${jobId} failed")
                    break
                case FovusJobStatus.WALLTIME_REACHED:
                case FovusTaskStatus.WALLTIME_REACHED:
                    task.error = new ProcessException("Job ${jobId} walltime reached")
                    break;
                case FovusJobStatus.TERMINATED:
                case FovusTaskStatus.TERMINATED:
                    task.error = new ProcessException("Job ${jobId} terminated")
                    break
                case FovusTaskStatus.UNCOMPLETE:
                    task.error = new ProcessException("Job ${jobId} uncomplete")
                    break
            }
        }

        status = TaskStatus.COMPLETED
        return true
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void killTask() {
        assert jobId

        // Job deletion will be handled by backend
        log.debug "[FOVUS] Terminated job > $task"
    }

    @Override
    void prepareLauncher() {
        createTaskWrapper().build()
    }

    protected BashWrapperBuilder createTaskWrapper() {
        final isMemoryCheckpointingEnabled = jobConfig.constraints.jobConstraints.isMemoryCheckpointingEnabled;
        final isContainerizedWorkload = jobConfig.environment instanceof ContainerizedEnvironment;
        final isDockerUsed = task.containerConfig.engine === 'docker' && task.containerConfig instanceof DockerConfig;
        final isMemoryCheckpointCompatible = isMemoryCheckpointingEnabled && isContainerizedWorkload && isDockerUsed;

        if (isContainerizedWorkload && isDockerUsed) {
            log.debug "[FOVUS] Docker is used for the workload. Adding Fovus Docker options..."
            def fovusDockerOptions = [
                    "--env-file /compute_workspace/.fovus_env",
                    "-v /fovus-fs:/fovus-fs",
                    "-v /fovus-storage-cached:/fovus-storage-cached", // TODO: Check if this is needed
                    "-v \$PWD:\$PWD",
                    "-v /fovus-storage:/fovus-storage",
                    "-v /fovus/archive:/fovus/archive",
                    "--memory \${FovusOptVcpuMem}g",
            ];

            if (isMemoryCheckpointCompatible) {
                log.debug "[FOVUS] Memory checkpointing is enabled. Adding Fovus Memory checkpointing options..."
                fovusDockerOptions += [
                        "--privileged",
                        "--cap-add=SYS_PTRACE",
                        "--init",
                        "--security-opt seccomp=unconfined",
                        "--detach", // Run in background
                        "-v fovus-libs:/opt/fovus-libs",
                        "-v \$PWD/.fovus-tmp:/tmp"
                ];
            }

            def currentDockerOptions = task.config.getContainerOptions() ?: "";
            def optionsToAdd = fovusDockerOptions.findAll { !currentDockerOptions.contains(it) }.join(" ");
            task.config.setProperty("containerOptions", currentDockerOptions + " " + optionsToAdd);
        }

        return new FovusScriptLauncher(task.toTaskBean(), executor, jobConfig, isMemoryCheckpointCompatible)
    }

    @Override
    void submit() {
        def runCommand
        final isTaskArrayRun = task instanceof TaskArrayRun

        if (isTaskArrayRun) {
            prepareArrayTasks(task as TaskArrayRun)
            runCommand = "./run.sh"
        } else {
            final remoteRunScript = executor.getRemotePath(wrapperFile)
            final remoteWorkDir = remoteRunScript.getParent()
            runCommand = "cd ${remoteWorkDir} && ./${TaskRun.CMD_RUN}"
        }
        jobConfig.setRunCommand(runCommand)

        // Save to config to JSON
        final jobConfigFolder = new File(FOVUS_JOB_CONFIG_FOLDER)
        if (!jobConfigFolder.exists()) {
            jobConfigFolder.mkdirs()
        }

        final jobConfigFile = File.createTempFile("${jobConfig.jobName}_", ".json", new File(FOVUS_JOB_CONFIG_FOLDER))
        final jobConfigFilePath = jobConfig.toJson(jobConfigFile.toPath())

        def jobDirectory = task.workDir.getParent().toString();

        if (isTaskArrayRun) {
            jobDirectory = task.workDir.toString();
        }
        List<String> includeList = []
        if (isTaskArrayRun) {
            for (TaskHandler taskHandler : (task as TaskArrayRun).getChildren()) {
                log.debug "[FOVUS] List of directory > ${taskHandler.getTask().workDir.toString()}"
                includeList.add("${taskHandler.getTask().workDir.toString().tokenize("/")[-1]}/");
            }
        } else {
            includeList.add("${this.getTask().workDir.toString().tokenize("/")[-1]}/");
        }

        log.debug "[FOVUS] Submitting job > $task"
        def pipelineId = this.executor.pipelineClient.getPipeline().getPipelineId();

        jobId = jobClient.createJob(jobConfigFilePath, jobDirectory, pipelineId, includeList, jobConfig.jobName, isTaskArrayRun)
        updateStatus(jobId)

        executor.jobIdMap.put(task.workDir.toString(), jobId);

        // Change the run scripts permission in background
        "chmod +x ${Escape.path(wrapperFile)} ${Escape.path(scriptFile)}".execute()
        // Allow creating new files in work directory
        "chmod 777 ${Escape.path(task.workDir)}".execute()
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
        if (task instanceof TaskArrayRun) {
            // update status for children tasks
            for (int i = 0; i < task.children.size(); i++) {
                final handler = task.children[i] as FovusTaskHandler
                //TODO: pass task id after adding check task status endpoint
                handler.updateStatus(jobId)
            }
        } else {
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
        def result = name.replaceAll(' ', '_').replaceAll(/[^a-zA-Z0-9_-]/, '')
        result.size() > 128 ? result.substring(0, 128) : result
    }

    protected String getJobName(TaskRun task) {
        final result = prependWorkflowPrefix(task.name, environment)
        return normalizeJobName(result)
    }

    private void prepareArrayTasks(TaskArrayRun task) {
        task.children.eachWithIndex { handler, int i ->
            handler = handler as FovusTaskHandler
            def subTaskName = handler.task.workDir.getName()
            def subTaskFolder = task.workDir.resolve(subTaskName)
            Files.createDirectories(subTaskFolder)
            log.trace "[FOVUS] Creating subtask ${i} for ${task.name}> ${subTaskFolder}"

            final remoteTaskWorkDir = executor.getRemotePath(handler.getTask().workDir.toAbsolutePath())
            final runScript = """
            #!/bin/bash
            cd "${remoteTaskWorkDir}"
            ./${TaskRun.CMD_RUN}
            """.stripIndent().leftTrim()

            // Save script as run.sh
            final runScriptPath = subTaskFolder.resolve("run.sh")
            Files.write(
                    runScriptPath,
                    runScript.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            )

            "chmod +x ${Escape.path(runScriptPath)}".execute()
            "chmod +x ${Escape.path(handler.wrapperFile)} ${Escape.path(handler.scriptFile)}".execute()
            "chmod 777 ${Escape.path(handler.task.workDir)}".execute()
        }
    }
}
