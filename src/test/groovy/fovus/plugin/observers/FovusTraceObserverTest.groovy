package fovus.plugin.observers

import fovus.plugin.FovusConfig
import fovus.plugin.FovusExecutor
import fovus.plugin.pipeline.FovusPipeline
import fovus.plugin.pipeline.FovusPipelineClient
import fovus.plugin.pipeline.FovusPipelineStatus
import fovus.plugin.pipeline.ResourceConfiguration
import nextflow.Session
import nextflow.executor.Executor
import nextflow.processor.TaskConfig
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class FovusTraceObserverTest extends Specification {

    @TempDir
    Path tempDir

    /** Write a JSON job config file and return its path, as `ext.jobConfigFile` would name it. */
    private String jobConfigFile(String json) {
        final file = tempDir.resolve('job-config.json')
        Files.write(file, json.bytes)
        return file.toString()
    }

    def 'parseExtensionObject should carry the scaling strategy flags when they are booleans'() {
        when:
        def configuration = FovusTraceObserver.parseExtensionObject(
                [benchmarkingProfileName: 'A', isHybridStrategyAllowed: true, isMultiRegionScalingAllowed: false], 'p')

        then:
        configuration.isHybridStrategyAllowed
        configuration.isMultiRegionScalingAllowed == false
    }

    def 'parseExtensionObject should ignore non-boolean scaling strategy flags'() {
        when:
        def configuration = FovusTraceObserver.parseExtensionObject(
                [benchmarkingProfileName: 'A', isHybridStrategyAllowed: 'yes', isMultiRegionScalingAllowed: 1], 'p')

        then:
        configuration.isHybridStrategyAllowed == null
        configuration.isMultiRegionScalingAllowed == null
    }

    def 'parseExtensionObject should return null when the ext has no benchmarking profile'() {
        expect:
        FovusTraceObserver.parseExtensionObject([maxvCpu: 64], 'p') == null
    }

    def 'parseExtensionObject should carry the storage connectors of the ext'() {
        when:
        def configuration = FovusTraceObserver.parseExtensionObject(
                [benchmarkingProfileName: 'global-profile',
                 storageConnectors      : ['reference-genomes', 'reference-genomes']], 'alignReads')

        then: 'duplicates are dropped, exactly as they are for a job'
        configuration.storageConnectors == ['reference-genomes']
    }

    def 'parseExtensionObject should leave the connectors unset when none are declared'() {
        expect:
        FovusTraceObserver.parseExtensionObject([benchmarkingProfileName: 'global-profile'], 'alignReads')
                .storageConnectors == null
    }

    def 'parseExtensionObject should carry the storage connectors of a JSON job config file'() {
        given:
        def ext = [benchmarkingProfileName: 'global-profile',
                   jobConfigFile          : jobConfigFile('{"storageConnectors": ["from-job-config", "from-job-config"]}')]

        expect: 'duplicates are dropped, exactly as they are for a job'
        FovusTraceObserver.parseExtensionObject(ext, 'alignReads').storageConnectors == ['from-job-config']
    }

    def 'parseExtensionObject should let ext.storageConnectors override the JSON job config file'() {
        given:
        def ext = [benchmarkingProfileName: 'global-profile',
                   jobConfigFile          : jobConfigFile('{"storageConnectors": ["from-job-config"]}'),
                   storageConnectors      : ['from-process']]

        expect:
        FovusTraceObserver.parseExtensionObject(ext, 'alignReads').storageConnectors == ['from-process']
    }

    def 'parseExtensionObject should ignore a missing JSON job config file'() {
        given:
        def ext = [benchmarkingProfileName: 'global-profile', jobConfigFile: '/no/such/job-config.json']

        expect: 'the job path reports the failure with better context, so it is not raised here'
        FovusTraceObserver.parseExtensionObject(ext, 'alignReads').storageConnectors == null
    }

    def 'parseExtensionObject should ignore an unparsable JSON job config file'() {
        given:
        def ext = [benchmarkingProfileName: 'global-profile', jobConfigFile: jobConfigFile('not json at all')]

        expect:
        FovusTraceObserver.parseExtensionObject(ext, 'alignReads').storageConnectors == null
    }

    def 'parseExtensionObject should reject a malformed storage connector name'() {
        when:
        FovusTraceObserver.parseExtensionObject(
                [benchmarkingProfileName: 'global-profile', storageConnectors: ['team_shared']], 'alignReads')

        then:
        def error = thrown(Error)
        error.message.contains('team_shared')
    }

    private TaskProcessor processWithExt(Map ext, Map params = [:]) {
        def taskConfig = new TaskConfig([ext: ext])
        taskConfig.setContext([params: params])
        return Mock(TaskProcessor) {
            getName() >> 'alignReads'
            createTaskPreview() >> new TaskRun(config: taskConfig)
        }
    }

    def 'resolveFirstAttemptExt should run a closure profile name with the params of the run'() {
        given:
        def process = processWithExt([benchmarkingProfileName: { params.tier == 'tiny' ? 'A' : 'B' }], [tier: 'tiny'])

        when:
        def ext = FovusTraceObserver.resolveFirstAttemptExt(process)

        then:
        ext.benchmarkingProfileName == 'A'
        FovusTraceObserver.parseExtensionObject(ext, 'alignReads').benchmarkingProfileName == 'A'
    }

    def 'resolveFirstAttemptExt should carry a memory closure and resolve task.attempt as 1'() {
        given:
        def process = processWithExt([
                benchmarkingProfileName: 'A',
                minvCpuMemGiB          : { params.mem },
                walltimeHours          : { task.attempt * 2 }], [mem: 16])

        when:
        def configuration = FovusTraceObserver.parseExtensionObject(
                FovusTraceObserver.resolveFirstAttemptExt(process), 'alignReads')

        then:
        configuration.minvCpuMemGiB == 16
        configuration.walltimeHours == 2
    }

    def 'resolveFirstAttemptExt should leave out a key that cannot be resolved and keep the others'() {
        given:
        def process = processWithExt([
                benchmarkingProfileName: 'A',
                maxvCpu                : 8,
                walltimeHours          : { meta.hours },
                note                   : '_nf_config_4553341a$_run_closure6$_closure22$_closure38@6fde6f05'])

        when:
        def ext = FovusTraceObserver.resolveFirstAttemptExt(process)

        then:
        ext == [benchmarkingProfileName: 'A', maxvCpu: 8]
    }

    def 'resolveFirstAttemptExt should return an empty map when the process sets no ext'() {
        expect:
        FovusTraceObserver.resolveFirstAttemptExt(processWithExt([:])) == [:]
    }

    def 'parseExtensionObject should return null when the profile name is not resolved text'() {
        expect:
        FovusTraceObserver.parseExtensionObject([benchmarkingProfileName: profileName], 'alignReads') == null

        where:
        profileName << [{ 'A' }, '_nf_config_4553341a$_run_closure6$_closure22$_closure38@6fde6f05', 42]
    }

    def 'parseExtensionObject should ignore a time-to-cost ratio that is not text and store a GString as a String'() {
        expect:
        FovusTraceObserver.parseExtensionObject([benchmarkingProfileName: 'A', timeToCostPriorityRatio: 5], 'p')
                .timeToCostPriorityRatio == null
        def ratio = "${'0.5'}/${'0.5'}"
        FovusTraceObserver.parseExtensionObject([benchmarkingProfileName: 'A', timeToCostPriorityRatio: ratio], 'p')
                .timeToCostPriorityRatio instanceof String
    }

    private static final FovusConfig TEST_CONFIG = new FovusConfig([pipelineName: 'test-pipeline'])
    private static final FovusPipeline TEST_PIPELINE = new FovusPipeline('test-pipeline', 'p-123')

    private TaskProcessor fovusProcess(String name, Map ext) {
        return processOn(name, ext, Mock(FovusExecutor))
    }

    private TaskProcessor processOn(String name, Map ext, Executor executor) {
        def taskConfig = new TaskConfig([ext: ext])
        taskConfig.setContext([params: [:]])
        return Mock(TaskProcessor) {
            getName() >> name
            getExecutor() >> executor
            createTaskPreview() >> new TaskRun(config: taskConfig)
        }
    }

    def 'onFlowBegin should pre-configure only the Fovus processes once, before setting RUNNING'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(Mock(Session), TEST_CONFIG, pipelineClient)
        observer.onProcessCreate(fovusProcess('alignReads', [benchmarkingProfileName: 'A']))
        observer.onProcessCreate(processOn('localStep', [benchmarkingProfileName: 'B'], Mock(Executor)))

        when:
        observer.onFlowBegin()

        then:
        1 * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.preConfigResources(TEST_CONFIG, TEST_PIPELINE,
                [new ResourceConfiguration(benchmarkingProfileName: 'A')], _)

        then:
        1 * pipelineClient.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.RUNNING, _)
    }

    def 'onFlowBegin should send equal configurations once'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(Mock(Session), TEST_CONFIG, pipelineClient)
        observer.onProcessCreate(fovusProcess('alignReads', [benchmarkingProfileName: 'A']))
        observer.onProcessCreate(fovusProcess('callVariants', [benchmarkingProfileName: 'A']))

        when:
        observer.onFlowBegin()

        then:
        1 * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.preConfigResources(TEST_CONFIG, TEST_PIPELINE,
                [new ResourceConfiguration(benchmarkingProfileName: 'A')], _)
    }

    def 'onFlowBegin should not pre-configure again when it fires a second time'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(Mock(Session), TEST_CONFIG, pipelineClient)

        when:
        observer.onFlowBegin()
        observer.onFlowBegin()

        then:
        _ * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.preConfigResources(*_)
    }

    def 'onFlowBegin should still call preConfigResources with an empty list when no process declares a profile'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(Mock(Session), TEST_CONFIG, pipelineClient)
        observer.onProcessCreate(fovusProcess('alignReads', [maxvCpu: 8]))

        when:
        observer.onFlowBegin()

        then:
        _ * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.preConfigResources(TEST_CONFIG, TEST_PIPELINE, [], _)
    }

    def 'onFlowBegin should keep going when the pre-configuration fails'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)
        def observer = new FovusTraceObserver(Mock(Session), TEST_CONFIG, pipelineClient)
        pipelineClient.preConfigResources(*_) >> { throw new IllegalStateException('cli down') }

        when:
        observer.onFlowBegin()

        then:
        noExceptionThrown()
        _ * pipelineClient.getPipeline() >> TEST_PIPELINE
        1 * pipelineClient.updatePipelineStatus(TEST_CONFIG, TEST_PIPELINE, FovusPipelineStatus.RUNNING, _)
    }

    def 'onProcessCreate should not throw when the process cannot be read'() {
        given:
        def process = Mock(TaskProcessor) {
            getName() >> 'broken'
            getExecutor() >> Mock(FovusExecutor)
            createTaskPreview() >> { throw new IllegalStateException('no preview') }
        }
        def observer = new FovusTraceObserver(Mock(Session), TEST_CONFIG, Mock(FovusPipelineClient))

        when:
        observer.onProcessCreate(process)

        then:
        noExceptionThrown()
    }
}
