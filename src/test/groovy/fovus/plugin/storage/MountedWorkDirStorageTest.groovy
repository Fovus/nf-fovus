package fovus.plugin.storage

import fovus.plugin.nio.PipelinesTestSupport
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

class MountedWorkDirStorageTest extends Specification {

    @TempDir
    Path tempDir

    def 'prepare should mount the parent of the work directory'() {
        given:
        def client = Mock(FovusStorageClient)
        def storage = new MountedWorkDirStorage(client, tempDir.resolve('mnt/pipelines'))

        when:
        storage.prepare('p-1-user')

        then:
        1 * client.validateOrMountFovusStorage(tempDir.resolve('mnt'))
    }

    def 'paths should be rewritten to /fovus-storage as today'() {
        given:
        def storage = new MountedWorkDirStorage(Mock(FovusStorageClient), tempDir.resolve('mnt/pipelines'))

        expect:
        storage.remotePath(tempDir.resolve('mnt/pipelines/p-1/fovus-work/ab/cdef/.command.run')) ==
                Path.of('/fovus-storage/pipelines/p-1/fovus-work/ab/cdef/.command.run')
    }

    def 'files outside the mount or on another file system should be foreign'() {
        given:
        def storage = new MountedWorkDirStorage(Mock(FovusStorageClient), tempDir.resolve('mnt/pipelines'))

        expect:
        !storage.isForeignFile(tempDir.resolve('mnt/files/input.txt'))
        storage.isForeignFile(tempDir.resolve('elsewhere/input.txt'))
        storage.isForeignFile(PipelinesTestSupport.fileSystem().getPath('/fovus-storage/files/input.txt'))
    }
}
