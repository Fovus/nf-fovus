package fovus.plugin

import fovus.plugin.nio.StorageTestSupport
import fovus.plugin.pipeline.FovusPipeline
import fovus.plugin.pipeline.FovusPipelineClient
import fovus.plugin.storage.WorkDirStorage
import nextflow.Session
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path

class FovusExecutorTest extends Specification {

    def 'the executor should leave path decisions to the work directory storage'() {
        given:
        def storage = Mock(WorkDirStorage)
        def executor = new FovusExecutor()
        executor.workDirStorage = storage
        def path = Path.of('/x')

        when:
        def remote = executor.getRemotePath(path)
        def foreign = executor.isForeignFile(path)

        then:
        1 * storage.remotePath(path) >> Path.of('/fovus-storage/x')
        1 * storage.isForeignFile(path) >> true
        remote == Path.of('/fovus-storage/x')
        foreign
    }

    @Unroll
    def 'the array launch command should be the local-work-dir one for a #mode work dir'() {
        given:
        def executor = new FovusExecutor()
        executor.session = Stub(Session) {
            getWorkDir() >> workDir
            getConfig() >> [:]
        }
        executor.pipelineClient = Stub(FovusPipelineClient) { getPipeline() >> new FovusPipeline('test-pipeline', 'p-1-user') }

        expect:
        executor.getArrayLaunchCommand(taskDir) == "bash ${taskDir}/.command.run 2>&1 > ${taskDir}/.command.log".toString()

        where:
        mode     | workDir                                                             | taskDir
        'direct' | StorageTestSupport.fileSystem().getPath('/fovus-storage/pipelines') | '/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef'
        'direct' | StorageTestSupport.fileSystem().getPath('/fovus-storage/pipelines') | '$nxf_array_task_dir'
        'mount'  | Path.of('/mnt/fovus/pipelines')                                     | '/mnt/fovus/pipelines/p-1-user/fovus-work/ab/cdef'
        'mount'  | Path.of('/mnt/fovus/pipelines')                                     | '$nxf_array_task_dir'
    }
}
