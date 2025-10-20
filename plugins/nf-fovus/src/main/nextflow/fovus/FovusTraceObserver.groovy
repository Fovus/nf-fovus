package nextflow.fovus


import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.fovus.pipeline.FovusPipelineClient
import nextflow.fovus.pipeline.FovusPipelineStatus
import nextflow.fovus.pipeline.ResourceConfiguration
import nextflow.script.ScriptMeta
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.TaskEvent

@Slf4j
@CompileStatic
class FovusTraceObserver implements TraceObserverV2 {

    private final Session session
    private final FovusConfig fovusConfig
    private final FovusPipelineClient pipelineClient
    volatile boolean isPipelineFailed = false

    FovusTraceObserver(Session session) {
        this.session = session
        this.fovusConfig = new FovusConfig(session.config.navigate('fovus') as Map);
        this.pipelineClient = new FovusPipelineClient();
    }

    @Override
    void onFlowCreate(Session session) {
        log.info "Pipeline is starting! 🚀"
        FovusPipelineCache.getOrCreatePipelineId(this.pipelineClient, fovusConfig, fovusConfig.getPipelineName())


        try {
            def configurations = new LinkedHashSet<ResourceConfiguration>()
            def processConfig = session.config.navigate('process')
            if (!processConfig || !(processConfig instanceof Map)) {
                return
            }

            ((Map) processConfig).entrySet().findAll { it.value instanceof Map }.each { entry ->
                def key = entry.key
                def value = entry.value

                ResourceConfiguration config;
                if (key == "ext" && (value instanceof Map)) {
                    config = parseExtensionObject(value)
                } else {
                    def ext = (value as Map).get("ext")
                    if (!ext || !(ext instanceof Map)) return

                    config = parseExtensionObject(ext as Map)
                }

                if (config !== null) {
                    configurations.add(config)
                }
            }
            pipelineClient.preConfigResources(fovusConfig, pipelineClient.getPipeline(), configurations.toList())
        } catch (Exception e) {
            log.trace "[FOVUS] Cannot configure pipeline resources: ${e.message}"
        }
    }

    @Override
    void onFlowBegin() {
        pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.RUNNING)
    }

    @Override
    void onFlowComplete() {
        log.trace "[FOVUS] FlowComplete Script Meta: ${ScriptMeta.allProcesses()}"
        if (!isPipelineFailed) {
            pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.COMPLETED)
        }
    }

    @Override
    void onFlowError(TaskEvent event) {
        isPipelineFailed = true
        pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), FovusPipelineStatus.FAILED)
        def processDefinitions = ScriptMeta.allProcesses()
        processDefinitions.each { processDef ->
            log.trace "[FOVUS] Process Config for ${processDef.getName()}: ${processDef.getProcessConfig()}"
        }
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
                    if (value instanceof Float) {
                        resourceConfig.maxvCpu = value
                    }
                    break
                case "maxGpu":
                    if (value instanceof Float) {
                        resourceConfig.maxGpu = value
                    }
                    break
                case "minGpu":
                    if (value instanceof Float) {
                        resourceConfig.minGpu = value
                    }
                    break
                case "minGpuMemGiB":
                    if (value instanceof Float) {
                        resourceConfig.minGpuMemGiB = value
                    }
                    break
                case "minvCpu":
                    if (value instanceof Float) {
                        resourceConfig.minvCpu = value
                    }
                    break
                case "minvCpuMemGiB":
                    if (value instanceof Float) {
                        resourceConfig.minvCpuMemGiB = value
                    }
                    break
                case "supportedCpuArchitectures":
                    try {
                        String[] supportedArchList = []
                        final supportedArchString = value.toString().toLowerCase()
                        if (supportedArchString.contains("x86-64"))
                            supportedArchList << "x86-64"
                        if (supportedArchString.contains("arm-64"))
                            supportedArchList << "arm-64"

                        if (supportedArchList.size() > 0)
                            resourceConfig.supportedCpuArchitectures = supportedArchList

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
                    if (value instanceof Float) {
                        resourceConfig.walltimeHours = value
                    }
                    break
                case "storageGiB":
                    if (value instanceof Float) {
                        resourceConfig.storageGiB = value
                    }
                    break
                default:
                    break
            }
        }

        return resourceConfig
    }
}
