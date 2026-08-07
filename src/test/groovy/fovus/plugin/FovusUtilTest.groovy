package fovus.plugin

import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class FovusUtilTest extends Specification {

    @TempDir
    Path tempDir

    def 'readNextflowConfig should return null when there is no config file'() {
        expect:
        FovusUtil.readNextflowConfig(configFiles) == null

        where:
        configFiles << [null, []]
    }

    def 'readNextflowConfig should return a single config file verbatim'() {
        given:
        def content = """\
            fovus {
                pipelineName = 'test-pipeline'
            }

            // a comment that must survive
            process {
                ext.benchmarkingProfileName = 'profile'
            }
            """.stripIndent()
        def configFile = Files.write(tempDir.resolve('nextflow.config'), content.bytes)

        when:
        def result = FovusUtil.readNextflowConfig([configFile])

        then:
        result == content
    }

    def 'readNextflowConfig should concatenate several config files behind a header naming each source'() {
        given:
        def base = Files.write(tempDir.resolve('nextflow.config'), "fovus.pipelineName = 'test-pipeline'\n".bytes)
        def extra = Files.write(tempDir.resolve('extra.config'), "process.ext.maxvCpu = 8\n".bytes)

        when:
        def result = FovusUtil.readNextflowConfig([base, extra])

        then:
        result == "// ==== ${base.toAbsolutePath()} ====\n" +
                  "fovus.pipelineName = 'test-pipeline'\n" +
                  "\n" +
                  "// ==== ${extra.toAbsolutePath()} ====\n" +
                  "process.ext.maxvCpu = 8\n"
    }

    def 'readNextflowConfig should skip a config file that does not exist'() {
        given:
        def missing = tempDir.resolve('missing.config')
        def existing = Files.write(tempDir.resolve('nextflow.config'), "process.ext.maxvCpu = 8\n".bytes)

        when:
        def result = FovusUtil.readNextflowConfig([missing, existing])

        then:
        noExceptionThrown()
        result == "// ==== ${existing.toAbsolutePath()} ====\n" +
                  "process.ext.maxvCpu = 8\n"
    }

    def 'readNextflowConfig should return null when none of the config files can be read'() {
        expect:
        FovusUtil.readNextflowConfig([tempDir.resolve('missing.config')]) == null
    }
}
