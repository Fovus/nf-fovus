package fovus.plugin.storage

import fovus.plugin.FovusConfig
import fovus.plugin.nio.FovusFileSystemProvider
import fovus.plugin.nio.StorageTestSupport
import fovus.plugin.util.FovusPathFactory
import nextflow.exception.AbortOperationException
import nextflow.file.FileHelper
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.FileSystems
import java.nio.file.Path

class WorkDirStorageFactoryTest extends Specification {

    static final FovusConfig CONFIG = new FovusConfig([pipelineName: 'test-pipeline'])

    @TempDir
    Path tempDir

    def setupSpec() {
        // FileHelper.getOrInstallProvider needs --add-opens java.base/java.nio.file.spi, which the unit-test JVM
        // does not have. Register the provider in the cache FileHelper.getProviderFor consults instead, so
        // FileHelper.asPath(URI) and FovusPathFactory still resolve fovus:// paths as they do under Nextflow.
        FileHelper.providersMap['fovus'] = new FovusFileSystemProvider()
    }

    def cleanupSpec() {
        FileHelper.providersMap.remove('fovus')
    }

    def 'a local workDir ending in pipelines should select mount mode'() {
        expect:
        WorkDirStorageFactory.create(tempDir.resolve('mnt/pipelines'), false, CONFIG) instanceof MountedWorkDirStorage
    }

    def 'a local workDir not ending in pipelines should be rejected as today'() {
        when:
        WorkDirStorageFactory.create(tempDir.resolve('work'), false, CONFIG)

        then:
        def e = thrown(AbortOperationException)
        e.message.startsWith('[FOVUS] Working directory must end with pipelines.')
    }

    def 'the triple-slash URI Nextflow resolves should select direct mode'() {
        given:
        def workDir = FileHelper.asPath(URI.create(uri))

        expect:
        WorkDirStorageFactory.create(workDir, false, CONFIG) instanceof DirectWorkDirStorage

        where:
        uri << ['fovus:///fovus-storage/pipelines', 'fovus:///fovus-storage/pipelines/']
    }

    def 'the two-slash spelling users type after -w should select direct mode'() {
        given:
        def workDir = new FovusPathFactory().parseUri(spelling)

        expect:
        WorkDirStorageFactory.create(workDir, false, CONFIG) instanceof DirectWorkDirStorage

        where:
        spelling << ['fovus://fovus-storage/pipelines', 'fovus://fovus-storage/pipelines/']
    }

    def 'a fovus workDir of #workDir, other than the pipelines area, should be rejected'() {
        given:
        def path = StorageTestSupport.fileSystem().getPath(workDir)

        when:
        WorkDirStorageFactory.create(path, false, CONFIG)

        then:
        def e = thrown(AbortOperationException)
        e.message.startsWith('[FOVUS] In direct mode, workDir must be fovus:///fovus-storage/pipelines.')

        where:
        workDir << ['/fovus-storage/pipelines/p-1-user', '/fovus-storage/files', '/fovus-storage/jobs']
    }

    def 'direct mode should be refused on a Fovus-hosted run'() {
        given:
        def workDir = StorageTestSupport.fileSystem().getPath('/fovus-storage/pipelines')

        when:
        WorkDirStorageFactory.create(workDir, true, CONFIG)

        then:
        def e = thrown(AbortOperationException)
        e.message.startsWith('[FOVUS] Direct mode (a fovus:// workDir) is only for pipelines launched on your own machine')
    }

    def 'any other workDir scheme should be rejected'() {
        given:
        def zip = FileSystems.newFileSystem(tempDir.resolve('work.zip'), [create: 'true'])

        when:
        WorkDirStorageFactory.create(zip.getPath('/pipelines'), false, CONFIG)

        then:
        def e = thrown(AbortOperationException)
        e.message.startsWith('[FOVUS] The Fovus executor needs workDir to be a Fovus storage mount')

        cleanup:
        zip.close()
    }
}
