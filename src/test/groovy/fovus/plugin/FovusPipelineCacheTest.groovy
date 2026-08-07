package fovus.plugin

import fovus.plugin.pipeline.FovusPipelineClient
import spock.lang.Specification

class FovusPipelineCacheTest extends Specification {

    private static final FovusConfig TEST_CONFIG = new FovusConfig([pipelineName: 'test-pipeline'])
    private static final String RUN_COMMAND = 'nextflow run hello -plugins nf-fovus'

    def setup() {
        // The cache resolves against the working directory, which the `test` task points at a
        // scratch dir. Clear it so each spec starts without a cached pipeline.
        new File('.fovus/pipeline_cache.json').delete()
    }

    def cleanupSpec() {
        new File('.fovus/pipeline_cache.json').delete()
    }

    def 'getOrCreatePipelineId should pass the run command through to createPipeline'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)

        when:
        def pipelineId = FovusPipelineCache.getOrCreatePipelineId(pipelineClient, TEST_CONFIG, 'test-pipeline',
                                                                  RUN_COMMAND)

        then:
        1 * pipelineClient.createPipeline(TEST_CONFIG, 'test-pipeline', RUN_COMMAND) >> 'p-123'
        pipelineId == 'p-123'
    }

    def 'getOrCreatePipelineId should pass a null run command when none is given'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)

        when:
        def pipelineId = FovusPipelineCache.getOrCreatePipelineId(pipelineClient, TEST_CONFIG, 'test-pipeline')

        then:
        1 * pipelineClient.createPipeline(TEST_CONFIG, 'test-pipeline', null) >> 'p-123'
        pipelineId == 'p-123'
    }

    def 'getOrCreatePipelineId should reuse a cached pipeline without creating a new one'() {
        given:
        def pipelineClient = Mock(FovusPipelineClient)
        FovusPipelineCache.updatePipelineCache('test-pipeline', 'p-cached')

        when:
        def pipelineId = FovusPipelineCache.getOrCreatePipelineId(pipelineClient, TEST_CONFIG, 'test-pipeline',
                                                                  RUN_COMMAND)

        then:
        1 * pipelineClient.getPipeline(TEST_CONFIG, 'p-cached') >> new fovus.plugin.pipeline.FovusPipeline(
                'test-pipeline', 'p-cached', fovus.plugin.pipeline.FovusPipelineStatus.CREATED)
        1 * pipelineClient.setPipeline('test-pipeline', 'p-cached')
        0 * pipelineClient.createPipeline(_, _, _)
        pipelineId == 'p-cached'
    }
}
