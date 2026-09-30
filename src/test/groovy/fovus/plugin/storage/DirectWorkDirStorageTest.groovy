package fovus.plugin.storage

import fovus.plugin.nio.FovusPath
import fovus.plugin.nio.PipelinesTestSupport
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusStorageCredentialsSource
import fovus.plugin.s3.S3Entry
import fovus.plugin.s3.StorageCredentialsException
import nextflow.exception.AbortOperationException
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

class DirectWorkDirStorageTest extends Specification {

    def 'prepare should attach the S3 client for the pipeline'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()
        def client = Stub(FovusS3Client) {
            head('pipelines/p-1-user/x') >> new S3Entry('pipelines/p-1-user/x', 3L, null, false)
        }
        def connector = Mock(S3Connector)
        def storage = new DirectWorkDirStorage((FovusPath) fs.getPath('/fovus-storage/pipelines'), connector)

        when:
        storage.prepare('p-1-user')

        then:
        1 * connector.connect('p-1-user') >> client
        Files.size(fs.getPath('/fovus-storage/pipelines/p-1-user/x')) == 3
    }

    def 'a second prepare should keep the attached client and not fetch credentials again'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()
        def connector = Mock(S3Connector)
        def root = (FovusPath) fs.getPath('/fovus-storage/pipelines')

        when: 'the trace observer prepares at flow creation, then the executor when it registers'
        new DirectWorkDirStorage(root, connector).prepare('p-1-user')
        new DirectWorkDirStorage(root, connector).prepare('p-1-user')

        then:
        1 * connector.connect('p-1-user') >> Stub(FovusS3Client)
        fs.hasS3Client()
    }

    def 'a credentials failure should stop the run with its message'() {
        given:
        def connector = Stub(S3Connector) {
            connect(_) >> { throw new StorageCredentialsException(FovusStorageCredentialsSource.NOT_SIGNED_IN, false) }
        }
        def root = (FovusPath) PipelinesTestSupport.fileSystem().getPath('/fovus-storage/pipelines')

        when:
        new DirectWorkDirStorage(root, connector).prepare('p-1-user')

        then:
        def e = thrown(AbortOperationException)
        e.message == "[FOVUS] ${FovusStorageCredentialsSource.NOT_SIGNED_IN}".toString()
    }

    def 'Fovus paths are local to direct mode and are already compute-node paths'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()
        def storage = new DirectWorkDirStorage((FovusPath) fs.getPath('/fovus-storage/pipelines'), Stub(S3Connector))

        expect:
        storage.isForeignFile(Path.of('/home/me/input.txt'))
        !storage.isForeignFile(fs.getPath('/fovus-storage/files/input.txt'))
        storage.remotePath(fs.getPath('/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef/.command.run')) ==
                Path.of('/fovus-storage/pipelines/p-1-user/fovus-work/ab/cdef/.command.run')
    }
}
