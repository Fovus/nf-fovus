package fovus.plugin.util

import nextflow.exception.AbortRunException
import nextflow.processor.PublishDir
import nextflow.processor.TaskConfig
import spock.lang.Specification

import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class PublishDirResolverTest extends Specification {

    static final String HOME = System.getProperty("user.home")
    static final String PIPELINE_ID = 'pipe-123'
    static final String BUCKET = 'test-bucket'
    static final String PIPELINE_PREFIX = HOME + '/' + PIPELINE_ID + '/'
    static final String PIPELINE_DIR = HOME + '/' + PIPELINE_ID

    private PublishDirResolver resolver(MountS3Adapter adapter = null) {
        new PublishDirResolver(adapter, BUCKET, PIPELINE_ID)
    }

    // ---------------------------------------------------------------------------
    // buildPathSegments
    // ---------------------------------------------------------------------------

    def 'buildPathSegments returns ordered path prefixes'() {
        expect:
        PublishDirResolver.buildPathSegments('/tmp/my_results/subfolder') ==
            ['/tmp', '/tmp/my_results', '/tmp/my_results/subfolder']
    }

    def 'buildPathSegments handles single-segment path'() {
        expect:
        PublishDirResolver.buildPathSegments('/tmp') == ['/tmp']
    }

    // ---------------------------------------------------------------------------
    // computeLocalPath (pipeline-dir case only)
    // ---------------------------------------------------------------------------

    def 'computeLocalPath: null path returns null'() {
        expect:
        resolver().computeLocalPath(null) == null
    }

    def 'computeLocalPath: path under pipeline dir is truncated to first segment'() {
        expect:
        resolver().computeLocalPath(Paths.get(PIPELINE_PREFIX + 'process1/subdir/file.txt')) == PIPELINE_PREFIX + 'process1'
    }

    def 'computeLocalPath: path directly at first segment under pipeline dir'() {
        expect:
        resolver().computeLocalPath(Paths.get(PIPELINE_PREFIX + 'outputs')) == PIPELINE_PREFIX + 'outputs'
    }

    // ---------------------------------------------------------------------------
    // computeMountPrefix
    // ---------------------------------------------------------------------------

    def 'computeMountPrefix: pipeline-dir path uses first segment as suffix'() {
        expect:
        resolver().computeMountPrefix(PIPELINE_PREFIX + 'process1') == "pipelines/${PIPELINE_ID}/fovus-output/process1"
    }

    def 'computeMountPrefix: other absolute path strips leading slash'() {
        expect:
        resolver().computeMountPrefix('/tmp/my_results') == "pipelines/${PIPELINE_ID}/fovus-output/tmp/my_results"
    }

    // ---------------------------------------------------------------------------
    // Mode validation
    // ---------------------------------------------------------------------------

    def 'resolve: non-copy mode throws AbortRunException'() {
        given:
        def publishDir = Mock(PublishDir)
        publishDir.path >> Paths.get(PIPELINE_PREFIX + 'results')
        publishDir.mode >> PublishDir.Mode.SYMLINK

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir]

        when:
        resolver().resolve(config)

        then:
        thrown(AbortRunException)
    }

    def 'resolve: null mode (default symlink) throws AbortRunException'() {
        given:
        def publishDir = Mock(PublishDir)
        publishDir.path >> Paths.get(PIPELINE_PREFIX + 'results')
        publishDir.mode >> null

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir]

        when:
        resolver().resolve(config)

        then:
        thrown(AbortRunException)
    }

    // ---------------------------------------------------------------------------
    // Pipeline dir guard
    // ---------------------------------------------------------------------------

    def 'resolve: bare pipeline dir path throws AbortRunException'() {
        given:
        def publishDir = Mock(PublishDir)
        publishDir.path >> Paths.get(PIPELINE_DIR)
        publishDir.mode >> PublishDir.Mode.COPY

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir]

        when:
        resolver().resolve(config)

        then:
        thrown(AbortRunException)
    }

    // ---------------------------------------------------------------------------
    // Pipeline-dir case: first-segment mount
    // ---------------------------------------------------------------------------

    def 'resolve: pipeline-dir path mounts at first segment'() {
        given:
        def adapter = Mock(MountS3Adapter)
        adapter.mount(_, _, _) >> true
        def publishDir = Mock(PublishDir)
        publishDir.path >> Paths.get(PIPELINE_PREFIX + 'process1/sub')
        publishDir.mode >> PublishDir.Mode.COPY

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir]

        when:
        resolver(adapter).resolve(config)

        then:
        1 * adapter.mount(_, _, PIPELINE_PREFIX + 'process1') >> true
    }

    def 'resolve: pipeline-dir mount failure throws AbortRunException'() {
        given:
        def adapter = Mock(MountS3Adapter)
        adapter.mount(_, _, _) >> false
        def publishDir = Mock(PublishDir)
        publishDir.path >> Paths.get(PIPELINE_PREFIX + 'results')
        publishDir.mode >> PublishDir.Mode.COPY

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir]

        when:
        resolver(adapter).resolve(config)

        then:
        thrown(AbortRunException)
    }

    def 'resolve: two pipeline-dir entries with same first segment trigger one mount'() {
        given:
        def adapter = Mock(MountS3Adapter)
        adapter.mount(_, _, _) >> true
        def resolver = resolver(adapter)

        def publishDir1 = Mock(PublishDir)
        publishDir1.path >> Paths.get(PIPELINE_PREFIX + 'process1/sub1')
        publishDir1.mode >> PublishDir.Mode.COPY

        def publishDir2 = Mock(PublishDir)
        publishDir2.path >> Paths.get(PIPELINE_PREFIX + 'process1/sub2')
        publishDir2.mode >> PublishDir.Mode.COPY

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir1, publishDir2]

        when:
        resolver.resolve(config)

        then:
        1 * adapter.mount(_, _, PIPELINE_PREFIX + 'process1') >> true
    }

    // ---------------------------------------------------------------------------
    // Absolute path: segment walking
    // ---------------------------------------------------------------------------

    def 'resolve: absolute path skips common Linux dir and mounts at first valid segment'() {
        given:
        def adapter = Mock(MountS3Adapter)
        // /tmp is skipped; /tmp/my_results succeeds
        adapter.mount(_, _, '/tmp/my_results') >> true

        def publishDir = Mock(PublishDir)
        publishDir.path >> Paths.get('/tmp/my_results/subfolder')
        publishDir.mode >> PublishDir.Mode.COPY

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir]

        when:
        resolver(adapter).resolve(config)

        then:
        0 * adapter.mount(_, _, '/tmp')
        1 * adapter.mount(_, _, '/tmp/my_results') >> true
        0 * adapter.mount(_, _, '/tmp/my_results/subfolder')
    }

    def 'resolve: absolute path falls through to deeper segment when shallower fails'() {
        given:
        def adapter = Mock(MountS3Adapter)
        adapter.mount(_, _, '/tmp/my_results') >> false
        adapter.mount(_, _, '/tmp/my_results/subfolder') >> true

        def publishDir = Mock(PublishDir)
        publishDir.path >> Paths.get('/tmp/my_results/subfolder')
        publishDir.mode >> PublishDir.Mode.COPY

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir]

        when:
        resolver(adapter).resolve(config)

        then:
        1 * adapter.mount(_, _, '/tmp/my_results') >> false
        1 * adapter.mount(_, _, '/tmp/my_results/subfolder') >> true
    }

    def 'resolve: absolute path with no mountable segment throws AbortRunException'() {
        given:
        def adapter = Mock(MountS3Adapter)
        adapter.mount(_, _, _) >> false

        def publishDir = Mock(PublishDir)
        publishDir.path >> Paths.get('/tmp/my_results')
        publishDir.mode >> PublishDir.Mode.COPY

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir]

        when:
        resolver(adapter).resolve(config)

        then:
        thrown(AbortRunException)
    }

    def 'resolve: second task reuses cached mount result without re-mounting'() {
        given:
        def adapter = Mock(MountS3Adapter)
        adapter.mount(_, _, '/tmp/my_results') >> true
        def resolver = resolver(adapter)

        def publishDir = Mock(PublishDir)
        publishDir.path >> Paths.get('/tmp/my_results/subfolder')
        publishDir.mode >> PublishDir.Mode.COPY

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir]

        when:
        resolver.resolve(config)
        resolver.resolve(config)

        then:
        // mount called once; second call hits registry and returns cached true
        1 * adapter.mount(_, _, '/tmp/my_results') >> true
    }

    // ---------------------------------------------------------------------------
    // Concurrent atomicity
    // ---------------------------------------------------------------------------

    def 'concurrent calls for the same path trigger exactly one mount'() {
        given:
        def mountCount = new java.util.concurrent.atomic.AtomicInteger(0)
        def adapter = Mock(MountS3Adapter)
        adapter.mount(_, _, _) >> { mountCount.incrementAndGet(); true }

        def resolver = resolver(adapter)

        def threads = 10
        def latch = new CountDownLatch(1)
        def pool = Executors.newFixedThreadPool(threads)
        def futures = (1..threads).collect {
            pool.submit({
                latch.await()
                def publishDir = Mock(PublishDir)
                publishDir.path >> Paths.get('/tmp/shared/output')
                publishDir.mode >> PublishDir.Mode.COPY
                def config = Mock(TaskConfig)
                config.getPublishDir() >> [publishDir]
                resolver.resolve(config)
            })
        }

        when:
        latch.countDown()
        futures.each { it.get() }
        pool.shutdown()

        then:
        mountCount.get() == 1
    }
}
