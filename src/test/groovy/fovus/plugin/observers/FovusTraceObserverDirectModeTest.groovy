package fovus.plugin.observers

import fovus.plugin.nio.FovusPath
import fovus.plugin.nio.S3Storage
import fovus.plugin.nio.StorageTestSupport
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusS3ClientTest
import fovus.plugin.storage.DirectWorkDirStorage
import fovus.plugin.storage.S3Connector
import fovus.plugin.storage.WorkDirStorage
import nextflow.exception.AbortOperationException
import nextflow.extension.FilesEx
import nextflow.file.FileHelper
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Path
import java.util.function.Function

/** Direct mode attaches the S3 client when the flow is created, before any operator or process runs. */
class FovusTraceObserverDirectModeTest extends Specification {

    private static Function<Path, WorkDirStorage> factoryOf(S3Connector connector) {
        return { Path workDir -> new DirectWorkDirStorage((FovusPath) workDir, connector) } as Function<Path, WorkDirStorage>
    }

    def 'a fovus:// work dir should get its S3 client when the flow is created'() {
        given:
        def fs = StorageTestSupport.fileSystem()
        def connector = Mock(S3Connector)

        when:
        FovusTraceObserver.prepareDirectModeStorage(fs.getPath('/fovus-storage/pipelines'), false, 'p-1-user', factoryOf(connector))

        then:
        1 * connector.connect('p-1-user') >> Stub(FovusS3Client)
        fs.hasS3Client()
    }

    @Unroll
    def 'a #kind run should be left to the executor'() {
        given:
        def factory = Mock(Function)

        when:
        FovusTraceObserver.prepareDirectModeStorage(workDir, hosted, 'p-1-user', factory)

        then:
        0 * factory._

        where:
        kind                      | workDir                                                             | hosted
        'mount mode'              | Path.of('/mnt/fovus/pipelines')                                     | false
        'mount mode hosted'       | Path.of('/fovus-storage/pipelines')                                 | true
        'hosted fovus:// workDir' | StorageTestSupport.fileSystem().getPath('/fovus-storage/pipelines') | true
        'missing workDir'         | null                                                                | false
    }

    def 'a work dir the factory rejects should fail the flow creation'() {
        given:
        def factory = { Path workDir -> throw new AbortOperationException('[FOVUS] In direct mode, workDir must be ...') } as Function<Path, WorkDirStorage>

        when:
        FovusTraceObserver.prepareDirectModeStorage(StorageTestSupport.fileSystem().getPath('/fovus-storage/pipelines/x'), false, 'p-1-user', factory)

        then:
        def e = thrown(AbortOperationException)
        e.message.startsWith('[FOVUS] In direct mode')
    }

    def "collectFile's scratch folders should be writable before the first process starts the executor"() {
        given:
        def s3 = Mock(S3Client)
        def client = new FovusS3Client(s3, s3, 'bucket', 'pipelines/p-1-user/', null)
        def fs = StorageTestSupport.fileSystem()
        def root = fs.getPath('/fovus-storage/pipelines')
        def connector = Stub(S3Connector) { connect('p-1-user') >> client }
        s3.headObject(_ as HeadObjectRequest) >> { throw FovusS3ClientTest.s3Error(404, 'NotFound') }
        s3.listObjectsV2(_ as ListObjectsV2Request) >> ListObjectsV2Response.builder().keyCount(0).build()

        when: 'the flow is created, then collectFile makes its temp folder and the folder for its cached file lists'
        FovusTraceObserver.prepareDirectModeStorage(root, false, 'p-1-user', factoryOf(connector))
        def temp = FileHelper.createTempFolder(root)
        def collected = FilesEx.mkdirs(root.resolve('collect-file'))

        then:
        1 * s3.putObject({ PutObjectRequest r -> r.key() ==~ /pipelines\/tmp\/[0-9a-f]{2}\/[0-9a-f]+\// }, _ as RequestBody)
        1 * s3.putObject({ PutObjectRequest r -> r.key() == 'pipelines/collect-file/' }, _ as RequestBody)
        S3Storage.keyOf((FovusPath) temp).startsWith('pipelines/tmp/')
        collected
    }
}
