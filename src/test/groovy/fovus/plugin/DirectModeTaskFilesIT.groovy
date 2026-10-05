package fovus.plugin

import fovus.plugin.nio.FovusPath
import fovus.plugin.nio.StorageTestSupport
import fovus.plugin.s3.MinioSupport
import fovus.plugin.s3.TransferManagerTransfers
import nextflow.file.FileHelper
import nextflow.processor.TaskBean
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.services.s3.S3Client
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Tag
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

@Tag('integration')
class DirectModeTaskFilesIT extends Specification {

    @Shared MinIOContainer minio
    @Shared S3Client s3
    @Shared TransferManagerTransfers transfers

    @TempDir
    Path tempDir

    FovusPath workDir

    def setupSpec() {
        minio = MinioSupport.start()
        s3 = MinioSupport.s3Client(minio)
        transfers = MinioSupport.transfers(minio)
    }

    def cleanupSpec() {
        transfers?.close()
        s3?.close()
        minio?.stop()
    }

    def setup() {
        final fs = StorageTestSupport.fileSystem(MinioSupport.fovusClient(s3, transfers))
        workDir = (FovusPath) fs.getPath("/fovus-storage/pipelines/p-1-user/fovus-work/ab/${UUID.randomUUID()}")
        Files.createDirectories(workDir)
    }

    def 'a direct-mode task can be prepared, read back and published over the SDK'() {
        given: 'the task Nextflow hands to the plugin'
        def bean = new TaskBean()
        bean.name = 'hello'
        bean.workDir = workDir
        bean.targetDir = workDir
        bean.script = 'echo hello > out.txt'
        bean.shell = ['bash']
        bean.environment = [GREETING: 'hello']
        bean.inputFiles = [:]
        bean.outputFiles = ['out.txt']
        def executor = Stub(FovusExecutor) {
            getRemoteBinDir() >> null
            getWorkDir() >> workDir.parent.parent
        }

        when: 'the plugin prepares it'
        new FovusScriptLauncher(bean, executor, null, false).build()

        then: 'the scripts are in Fovus storage'
        workDir.resolve('.command.sh').text.contains('echo hello > out.txt')
        workDir.resolve('.command.run').text.contains('.command.sh')
        workDir.resolve('.command.fovus.env').text == 'export GREETING="hello"\n'

        when: 'the compute node finishes and Fovus syncs its results back'
        Files.writeString(workDir.resolve('.exitcode'), '0\n')
        Files.writeString(workDir.resolve('out.txt'), 'hello\n')
        def exitStatus = new ExitStatusReader().read(workDir.resolve('.exitcode'), workDir)
        def outputs = []
        FileHelper.visitFiles([type: 'file'], workDir, 'out.txt') { Path p -> outputs << p }
        def published = tempDir.resolve('results/out.txt')
        FileHelper.copyPath(workDir.resolve('out.txt'), published)

        then: 'the plugin and Nextflow read them back and publish locally'
        exitStatus == 0
        outputs == [workDir.resolve('out.txt')]
        published.text == 'hello\n'
    }
}
