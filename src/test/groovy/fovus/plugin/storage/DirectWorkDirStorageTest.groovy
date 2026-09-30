package fovus.plugin.storage

import fovus.plugin.nio.FovusPath
import fovus.plugin.nio.PipelinesTestSupport
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusStorageCredentialsSource
import fovus.plugin.s3.S3Entry
import fovus.plugin.s3.StorageCredentialsException
import nextflow.exception.AbortOperationException
import spock.lang.Specification

import java.nio.file.AccessDeniedException
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

    def 'prepare should check read and write access at start-up'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()
        def client = Mock(FovusS3Client)
        def connector = Stub(S3Connector) { connect('p-1-user') >> client }

        when:
        new DirectWorkDirStorage((FovusPath) fs.getPath('/fovus-storage/pipelines'), connector).prepare('p-1-user')

        then: 'one listing with the read token, then the work folder marker with the write token'
        1 * client.hasChildren('pipelines/p-1-user/')

        then:
        1 * client.putDirectoryMarker('pipelines/p-1-user/fovus-work/')
        0 * client._
        fs.hasS3Client()
    }

    def 'a failed start-up check should stop the run with its message and attach nothing'() {
        given:
        def fs = PipelinesTestSupport.fileSystem()
        def client = Stub(FovusS3Client) {
            putDirectoryMarker(_) >> { throw new AccessDeniedException('fovus:///fovus-storage/pipelines/p-1-user/fovus-work/', null,
                                                                      "Fovus storage credentials don't allow write on pipelines/p-1-user/fovus-work/ (write token)") }
        }
        def connector = Stub(S3Connector) { connect('p-1-user') >> client }

        when:
        new DirectWorkDirStorage((FovusPath) fs.getPath('/fovus-storage/pipelines'), connector).prepare('p-1-user')

        then:
        def e = thrown(AbortOperationException)
        e.message.startsWith('[FOVUS] ')
        e.message.contains("Fovus storage credentials don't allow write on pipelines/p-1-user/fovus-work/ (write token)")
        !fs.hasS3Client()
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
