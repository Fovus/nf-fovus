package fovus.plugin

import fovus.plugin.task.FovusTaskClient
import fovus.plugin.task.FovusTaskStatus
import nextflow.exception.ProcessException
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import java.nio.file.Path

class FovusTaskHandlerTest extends Specification {

    @TempDir
    Path workDir

    /** A handler for a submitted, running task whose Fovus task reports {@code fovusStatus}. */
    private FovusTaskHandler runningHandler(FovusTaskStatus fovusStatus) {
        final jobConfigFile = workDir.resolve('job_config.json')
        jobConfigFile.text = '''{
            "environment": {"containerized": {"container": "Docker", "version": "20.10.18", "imagePath": "ubuntu"}},
            "constraints": {"jobConstraints": {}, "taskConstraints": {}},
            "objective": {},
            "workload": {}
        }'''
        final task = new TaskRun(name: 'align (1)', workDir: workDir, processor: Stub(TaskProcessor),
                config: new TaskConfig([ext: [jobConfigFile: jobConfigFile.toString()]]))
        final executor = new FovusExecutor()
        executor.fovusConfig = new FovusConfig()

        final handler = new FovusTaskHandler(task, executor)
        handler.taskClient = Stub(FovusTaskClient) { getTaskStatus('job-1', _) >> fovusStatus }
        handler.jobId = 'job-1'
        handler.status = TaskStatus.RUNNING
        return handler
    }

    @Unroll
    def 'a finished task should pass or fail on its exit code (#exitCode) alone, whatever Fovus reports (#fovusStatus)'() {
        given:
        def handler = runningHandler(fovusStatus)
        workDir.resolve(TaskRun.CMD_EXIT).text = exitCode

        when:
        def completed = handler.checkIfCompleted()

        then:
        completed
        handler.task.exitStatus == exitStatus
        handler.task.error == null

        where:
        fovusStatus                      | exitCode | exitStatus
        FovusTaskStatus.COMPLETED        | '0'      | 0
        FovusTaskStatus.COMPLETED        | '1'      | 1
        FovusTaskStatus.FAILED           | '0'      | 0
        FovusTaskStatus.FAILED           | '137'    | 137
        FovusTaskStatus.WALLTIME_REACHED | '143'    | 143
        FovusTaskStatus.TERMINATED       | '143'    | 143
        FovusTaskStatus.UNCOMPLETE       | '1'      | 1
    }

    @Unroll
    def 'a task that finished without an exit code should fail with the reason Fovus gives (#fovusStatus)'() {
        given:
        def handler = runningHandler(fovusStatus)

        when:
        def completed = handler.checkIfCompleted()

        then:
        completed
        handler.task.exitStatus == Integer.MAX_VALUE
        handler.task.error instanceof ProcessException
        handler.task.error.message.contains(reason)

        where:
        fovusStatus                      | reason
        FovusTaskStatus.FAILED           | 'failed'
        FovusTaskStatus.WALLTIME_REACHED | 'walltime reached'
        FovusTaskStatus.TERMINATED       | 'terminated'
        FovusTaskStatus.UNCOMPLETE       | 'uncomplete'
    }

    def 'a task should not finish while Fovus still reports it running, even once its exit code is written'() {
        given:
        def handler = runningHandler(FovusTaskStatus.RUNNING)
        workDir.resolve(TaskRun.CMD_EXIT).text = '0'

        expect:
        !handler.checkIfCompleted()
    }
}
