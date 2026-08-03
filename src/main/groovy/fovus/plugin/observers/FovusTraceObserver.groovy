package fovus.plugin.observers

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Session
import fovus.plugin.FovusConfig
import fovus.plugin.FovusPipelineCache
import fovus.plugin.FovusUtil
import fovus.plugin.pipeline.FovusPipelineClient
import fovus.plugin.pipeline.FovusPipelineStatus
import fovus.plugin.pipeline.ResourceConfiguration
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.TaskEvent

import java.util.concurrent.atomic.AtomicBoolean

@Slf4j
@CompileStatic
class FovusTraceObserver implements TraceObserverV2 {

    private final Session session
    private final FovusConfig fovusConfig
    private final FovusPipelineClient pipelineClient
    private volatile boolean hasFlowError = false
    private volatile TaskEvent lastFlowErrorEvent
    // session.abort() can cause onFlowComplete to fire from two threads concurrently;
    // this guard ensures the pipeline status update and headnode cleanup run exactly once.
    private final AtomicBoolean isCompletionReported = new AtomicBoolean(false)

    FovusTraceObserver(Session session) {
        this(
                session,
                new FovusConfig(session.config.navigate('fovus') as Map),
                new FovusPipelineClient()
        )
    }

    @PackageScope
    FovusTraceObserver(Session session, FovusConfig fovusConfig, FovusPipelineClient pipelineClient) {
        this.session = session
        this.fovusConfig = fovusConfig
        this.pipelineClient = pipelineClient
    }

    @Override
    void onFlowCreate(Session session) {
        log.info "Pipeline is starting! 🚀"
        FovusPipelineCache.getOrCreatePipelineId(this.pipelineClient, fovusConfig, fovusConfig.getPipelineName())


        try {
            final configurations = collectResourceConfigurations(session)
            final nextflowConfig = FovusUtil.readNextflowConfig(session?.getConfigFiles())

            pipelineClient.preConfigResources(fovusConfig, pipelineClient.getPipeline(), configurations, nextflowConfig)
        } catch (Exception e) {
            log.trace "[FOVUS] Cannot configure pipeline resources: ${e.message}"
        }
    }

    /**
     * Collect the resource configurations declared through `process.ext`, both the global
     * one and each per-process override merged over it.
     *
     * @return The resource configurations, empty when the config declares none
     */
    @PackageScope
    static List<ResourceConfiguration> collectResourceConfigurations(Session session) {
        def configurations = new LinkedHashSet<ResourceConfiguration>()
        def processConfig = session.config.navigate('process')
        if (!processConfig || !(processConfig instanceof Map)) {
            return []
        }

        // Handle the global configuration
        processConfig = processConfig as Map
        ResourceConfiguration globalConfig = processConfig.ext instanceof Map ?
                                             parseExtensionObject(processConfig.ext as Map) : null;

        if (globalConfig) {
            configurations.add(globalConfig)
        }

        // Look for each benchmark overriding
        processConfig.entrySet().findAll { it.value instanceof Map }.each { entry ->
            def key = entry.key
            def value = entry.value

            ResourceConfiguration config = null;
            if (key == "ext" && (value instanceof Map)) {
                // Skip the global ext config
                return
            }

            def ext = (value as Map).get("ext")
            if (!ext || !(ext instanceof Map)) return

            config = parseExtensionObject(ext as Map)

            if (config && globalConfig) {
                config = globalConfig.mergeWith(config)
            }

            if (config) {
                configurations.add(config)
            }
        }

        return configurations.toList()
    }

    @Override
    void onFlowBegin() {
        hasFlowError = false
        lastFlowErrorEvent = null
        pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.RUNNING)
    }

    @Override
    void onFlowComplete() {
        if (!isCompletionReported.compareAndSet(false, true)) return

        final boolean isFailed = hasFlowError || session.isAborted()
        log.trace "[FOVUS] Pipeline completed with status ${isFailed ? 'FAILED' : 'COMPLETED'}"

        final status = isFailed ? FovusPipelineStatus.FAILED : FovusPipelineStatus.COMPLETED
        pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), status)
    }

    @Override
    void onFlowError(TaskEvent event) {
        hasFlowError = true
        lastFlowErrorEvent = event
        log.trace "[FOVUS] Failure detected for task `${event?.handler?.task?.lazyName() ?: 'unknown'}`"
    }

    @PackageScope
    boolean hasFlowErrorState() {
        return hasFlowError
    }

    @PackageScope
    TaskEvent getLastFlowErrorEvent() {
        return lastFlowErrorEvent
    }

    static ResourceConfiguration parseExtensionObject(Map ext) {
        if (!(ext instanceof Map)) return

        final benchmarkingProfileName = ext.get("benchmarkingProfileName")
        if (!benchmarkingProfileName) return

        def resourceConfig = new ResourceConfiguration(benchmarkingProfileName: benchmarkingProfileName)

        ext.forEach { key, value ->
            switch (key) {
                case "allowPreemptible":
                    if (value instanceof Boolean) {
                        resourceConfig.allowPreemptible = value
                    }
                    break
                case "computingDevice":
                    if (value instanceof String) {
                        if (value.toLowerCase().contains("gpu"))
                            resourceConfig.computingDevice = "cpu + gpu"
                        else
                            resourceConfig.computingDevice = "cpu"
                    }
                    break
                case "enableHyperthreading":
                    if (value instanceof Boolean) {
                        resourceConfig.enableHyperthreading = value
                    }
                    break
                case "maxvCpu":
                    if (value instanceof Number) {
                        resourceConfig.maxvCpu = value
                    }
                    break
                case "maxGpu":
                    if (value instanceof Number) {
                        resourceConfig.maxGpu = value
                    }
                    break
                case "minGpu":
                    if (value instanceof Number) {
                        resourceConfig.minGpu = value
                    }
                    break
                case "minGpuMemGiB":
                    if (value instanceof Number) {
                        resourceConfig.minGpuMemGiB = value
                    }
                    break
                case "minvCpu":
                    if (value instanceof Number) {
                        resourceConfig.minvCpu = value
                    }
                    break
                case "minvCpuMemGiB":
                    if (value instanceof Number) {
                        resourceConfig.minvCpuMemGiB = value
                    }
                    break
                case "supportedCpuArchitectures":
                    try {
                        def supportedArchList = []
                        final supportedArchString = value.toString().toLowerCase()
                        if (supportedArchString.contains("x86-64"))
                            supportedArchList << "x86-64"
                        if (supportedArchString.contains("arm-64"))
                            supportedArchList << "arm-64"

                        if (supportedArchList.size() > 0)
                            resourceConfig.supportedCpuArchitectures = supportedArchList as String[]

                    } catch (Exception e) {
                        // Do nothing
                    }
                    break
                case "timeToCostPriorityRatio":
                    resourceConfig.timeToCostPriorityRatio = value
                    break
                case "isResumableWorkload":
                    if (value instanceof Boolean) {
                        resourceConfig.isResumableWorkload = value
                    }
                    break
                case "walltimeHours":
                    if (value instanceof Number) {
                        resourceConfig.walltimeHours = value
                    }
                    break
                case "storageGiB":
                    if (value instanceof Number) {
                        resourceConfig.storageGiB = value
                    }
                    break
                case "isMemoryAutoRetryEnabled":
                    if (value instanceof Boolean) {
                        resourceConfig.isMemoryAutoRetryEnabled = value
                    }
                    break;
                default:
                    break
            }
        }

        return resourceConfig
    }
}
