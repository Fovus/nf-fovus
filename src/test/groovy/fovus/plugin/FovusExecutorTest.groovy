package fovus.plugin

import fovus.plugin.storage.WorkDirStorage
import spock.lang.Specification

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
}
