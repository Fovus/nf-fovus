package fovus.plugin

import nextflow.container.DockerConfig
import nextflow.processor.TaskBean
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class FovusScriptLauncherTest extends Specification {

    @TempDir
    Path tempDir

    private TaskBean taskBean(Map<String, String> environment, boolean containerEnabled = false) {
        final bean = new TaskBean()
        bean.name = 'test-task'
        bean.workDir = tempDir
        bean.environment = environment
        bean.containerEnabled = containerEnabled
        if (containerEnabled) {
            bean.containerImage = 'ubuntu:latest'
            bean.containerConfig = new DockerConfig([engine: 'docker', enabled: true])
        }
        return bean
    }

    private FovusScriptLauncher launcher(TaskBean bean, Path remoteBinDir = null) {
        final executor = Stub(FovusExecutor) {
            getRemoteBinDir() >> remoteBinDir
        }
        return new FovusScriptLauncher(bean, executor, null, false)
    }

    private Path envFile() {
        return tempDir.resolve(FovusScriptLauncher.CMD_FOVUS_ENV)
    }

    def 'writeTaskEnvFile should write one export line per environment entry'() {
        given:
        def bean = taskBean([FOO: 'bar', BAZ: 'qux'])

        when:
        launcher(bean).writeTaskEnvFile()

        then:
        envFile().text == 'export FOO="bar"\nexport BAZ="qux"\n'
    }

    def 'writeTaskEnvFile should prepend the remote bin dir to PATH and drop any external PATH'() {
        given:
        def bean = taskBean([PATH: '/should/not/leak', FOO: 'bar'])

        when:
        launcher(bean, Path.of('/fovus-storage/bin')).writeTaskEnvFile()

        then:
        envFile().text == 'export PATH=/fovus-storage/bin:$PATH\nexport FOO="bar"\n'
    }

    def 'writeTaskEnvFile should write an empty file when the task has no environment'() {
        given:
        def bean = taskBean(environment as Map<String, String>)

        when:
        launcher(bean).writeTaskEnvFile()

        then:
        Files.exists(envFile())
        envFile().text == ''

        where:
        environment << [null, [:]]
    }

    def 'writeTaskEnvFile should write plain export lines for a containerized task'() {
        given:
        def bean = taskBean([FOO: 'bar'], true)

        when:
        launcher(bean, Path.of('/fovus-storage/bin')).writeTaskEnvFile()

        then: 'the sourceable form is used, not the nxf_container_env heredoc function'
        !envFile().text.contains('nxf_container_env')
        envFile().text == 'export PATH=/fovus-storage/bin:$PATH\nexport FOO="bar"\n'
    }

    def 'writeTaskEnvFile should overwrite a previously written file'() {
        given:
        Files.write(envFile(), 'export STALE="1"\n'.bytes)

        when:
        launcher(taskBean([FOO: 'bar'])).writeTaskEnvFile()

        then:
        envFile().text == 'export FOO="bar"\n'
    }

    def 'build should emit the env file alongside the wrapper it already writes'() {
        given:
        def bean = taskBean([FOO: 'bar'])
        bean.script = 'echo hello'

        when:
        launcher(bean).build()

        then: 'the standard Nextflow artifacts are untouched and the env file sits next to them'
        Files.exists(tempDir.resolve('.command.run'))
        Files.exists(tempDir.resolve('.command.sh'))
        envFile().text == 'export FOO="bar"\n'

        and: 'the wrapper still inlines the same environment'
        tempDir.resolve('.command.run').text.contains('export FOO="bar"')
    }

    def 'writeTaskEnvFile should not throw when the file cannot be written'() {
        given: 'a work dir that does not exist'
        def bean = taskBean([FOO: 'bar'])
        bean.workDir = tempDir.resolve('missing')

        when:
        launcher(bean).writeTaskEnvFile()

        then:
        noExceptionThrown()
        !Files.exists(bean.workDir.resolve(FovusScriptLauncher.CMD_FOVUS_ENV))
    }
}
