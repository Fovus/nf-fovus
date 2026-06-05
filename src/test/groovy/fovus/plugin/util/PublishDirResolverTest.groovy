package fovus.plugin.util

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

    // ---------------------------------------------------------------------------
    // computeLocalPath
    // ---------------------------------------------------------------------------

    def 'computeLocalPath: null path returns null'() {
        given:
        def resolver = new PublishDirResolver(null, BUCKET, PIPELINE_ID)

        expect:
        resolver.computeLocalPath(null, PIPELINE_ID) == null
    }

    def 'computeLocalPath: path under /fovus-storage is skipped'() {
        given:
        def resolver = new PublishDirResolver(null, BUCKET, PIPELINE_ID)

        expect:
        resolver.computeLocalPath(Paths.get('/fovus-storage/pipelines/x/out'), PIPELINE_ID) == null
    }

    def 'computeLocalPath: path under pipeline dir is truncated to first segment'() {
        given:
        def resolver = new PublishDirResolver(null, BUCKET, PIPELINE_ID)

        expect:
        resolver.computeLocalPath(Paths.get(PIPELINE_PREFIX + 'process1/subdir/file.txt'), PIPELINE_ID) == PIPELINE_PREFIX + 'process1'
    }

    def 'computeLocalPath: path directly at first segment under pipeline dir'() {
        given:
        def resolver = new PublishDirResolver(null, BUCKET, PIPELINE_ID)

        expect:
        resolver.computeLocalPath(Paths.get(PIPELINE_PREFIX + 'outputs'), PIPELINE_ID) == PIPELINE_PREFIX + 'outputs'
    }

    def 'computeLocalPath: other absolute path is returned as-is'() {
        given:
        def resolver = new PublishDirResolver(null, BUCKET, PIPELINE_ID)

        expect:
        resolver.computeLocalPath(Paths.get('/mnt/outputs/sample1'), PIPELINE_ID) == '/mnt/outputs/sample1'
    }

    // ---------------------------------------------------------------------------
    // computeSubpath
    // ---------------------------------------------------------------------------

    def 'computeSubpath: pipeline-dir path uses first segment as suffix'() {
        given:
        def resolver = new PublishDirResolver(null, BUCKET, PIPELINE_ID)

        expect:
        resolver.computeSubpath(PIPELINE_PREFIX + 'process1', PIPELINE_ID) == "pipelines/${PIPELINE_ID}/fovus-output/process1"
    }

    def 'computeSubpath: other absolute path strips leading slash'() {
        given:
        def resolver = new PublishDirResolver(null, BUCKET, PIPELINE_ID)

        expect:
        resolver.computeSubpath('/mnt/outputs/sample1', PIPELINE_ID) == "pipelines/${PIPELINE_ID}/fovus-output/mnt/outputs/sample1"
    }

    // ---------------------------------------------------------------------------
    // prefix dedup (via resolve)
    // ---------------------------------------------------------------------------

    def 'resolve: child path is skipped when parent already mounted'() {
        given:
        def adapter = Mock(MountS3Adapter)
        def resolver = new PublishDirResolver(adapter, BUCKET, PIPELINE_ID)

        def parentPublishDir = Mock(PublishDir)
        parentPublishDir.path >> Paths.get('/mnt/outputs/sample1')

        def childPublishDir = Mock(PublishDir)
        childPublishDir.path >> Paths.get('/mnt/outputs/sample1/subfolder')

        def config = Mock(TaskConfig)
        config.getPublishDir() >>> [
            [parentPublishDir],
            [childPublishDir],
        ]

        when:
        resolver.resolve(config)
        resolver.resolve(config)

        then:
        1 * adapter.mount(_, _, '/mnt/outputs/sample1')
        0 * adapter.mount(_, _, '/mnt/outputs/sample1/subfolder')
    }

    def 'resolve: same normalized path from two publishDir entries triggers one mount'() {
        given:
        def adapter = Mock(MountS3Adapter)
        def resolver = new PublishDirResolver(adapter, BUCKET, PIPELINE_ID)

        def publishDir1 = Mock(PublishDir)
        publishDir1.path >> Paths.get(PIPELINE_PREFIX + 'process1/sub1')

        def publishDir2 = Mock(PublishDir)
        publishDir2.path >> Paths.get(PIPELINE_PREFIX + 'process1/sub2')

        def config = Mock(TaskConfig)
        config.getPublishDir() >> [publishDir1, publishDir2]

        when:
        resolver.resolve(config)

        then:
        1 * adapter.mount(_, _, PIPELINE_PREFIX + 'process1')
    }

    // ---------------------------------------------------------------------------
    // Concurrent atomicity
    // ---------------------------------------------------------------------------

    def 'concurrent calls for the same path trigger exactly one mount'() {
        given:
        def mountCount = new java.util.concurrent.atomic.AtomicInteger(0)
        def adapter = Mock(MountS3Adapter)
        adapter.mount(_, _, _) >> { mountCount.incrementAndGet() }

        def resolver = new PublishDirResolver(adapter, BUCKET, PIPELINE_ID)
        def path = Paths.get('/mnt/shared/output')

        def threads = 10
        def latch = new CountDownLatch(1)
        def pool = Executors.newFixedThreadPool(threads)
        def futures = (1..threads).collect {
            pool.submit({
                latch.await()
                def publishDir = Mock(PublishDir)
                publishDir.path >> path
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
