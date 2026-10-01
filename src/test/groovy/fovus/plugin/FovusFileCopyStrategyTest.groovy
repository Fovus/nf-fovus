package fovus.plugin

import fovus.plugin.nio.StorageTestSupport
import nextflow.processor.TaskBean
import spock.lang.Specification

class FovusFileCopyStrategyTest extends Specification {

    def 'an output of a direct-mode task should be copied into the compute workspace'() {
        given:
        def workDir = StorageTestSupport.fileSystem().getPath('/fovus-storage/pipelines/p-1-user/fovus-work')
        def executor = Stub(FovusExecutor) { getWorkDir() >> workDir }
        def bean = new TaskBean()
        bean.workDir = workDir.resolve('ab/cdef')

        expect:
        new FovusFileCopyStrategy(bean, executor).copyFile('out.txt', workDir.resolve('ab/cdef/out.txt')) ==
                'cp out.txt /compute_workspace/cdef/out.txt'
    }
}
