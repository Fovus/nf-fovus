package fovus.plugin.observers

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskProcessor
import fovus.plugin.FovusConfig
import fovus.plugin.FovusExecutor
import fovus.plugin.FovusPipelineCache
import fovus.plugin.FovusUtil
import fovus.plugin.job.FovusJobConfig
import fovus.plugin.job.FovusJobConfigBuilder
import fovus.plugin.pipeline.FovusPipeline
import fovus.plugin.pipeline.FovusPipelineClient
import fovus.plugin.pipeline.FovusPipelineStatus
import fovus.plugin.pipeline.ResourceConfiguration
import fovus.plugin.nio.FovusPath
import fovus.plugin.storage.WorkDirStorage
import fovus.plugin.storage.WorkDirStorageFactory
import fovus.plugin.util.FovusEnvironment
import nextflow.trace.TraceObserverV2
import nextflow.trace.event.TaskEvent

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Function

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
    private final AtomicBoolean isPreConfigSent = new AtomicBoolean(false)
    private final Map<String, ResourceConfiguration> resourceConfigurations = new ConcurrentHashMap<>()

    FovusTraceObserver(Session session) {
        this(
                session,
                FovusConfig.fromSession(session),
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
        final pipelineId = FovusPipelineCache.getOrCreatePipelineId(this.pipelineClient, fovusConfig,
                                                                    fovusConfig.getPipelineName(), session?.getCommandLine())

        final isHostedMode = FovusEnvironment.isHostedMode()
        prepareDirectModeStorage(session?.getWorkDir(), isHostedMode, pipelineId,
                { Path workDir -> WorkDirStorageFactory.create(workDir, isHostedMode, fovusConfig) } as Function<Path, WorkDirStorage>)

    }

    /**
     * Direct mode: connect the {@code fovus://} work directory now, before the script runs, so that an operator
     * evaluated before the first process (e.g. {@code collectFile}, which writes under {@code workDir}) can use
     * it. The executor prepares the same storage again when it registers, which is then a no-op. Mount mode and
     * Fovus-hosted runs are left to the executor. A failure (a work directory the factory rejects, credentials
     * that cannot be fetched) is thrown, but Nextflow only logs an observer's exception at debug level: nothing is
     * attached, and the run stops later -- when the executor registers and hits the same failure, or earlier, when
     * an operator finds no S3 client attached.
     */
    @PackageScope
    static void prepareDirectModeStorage(Path workDir, boolean isHostedMode, String pipelineId,
                                         Function<Path, WorkDirStorage> storageFactory) {
        if (isHostedMode || !(workDir instanceof FovusPath)) return
        storageFactory.apply(workDir).prepare(pipelineId)
    }

    @Override
    void onProcessCreate(TaskProcessor process) {
        try {
            if (!(process.getExecutor() instanceof FovusExecutor)) return

            final configuration = parseExtensionObject(resolveFirstAttemptExt(process), process.getName())
            if (configuration) {
                resourceConfigurations.put(process.getName(), configuration)
            }
        } catch (Exception e) {
            log.warn "[FOVUS] Cannot read the resource configuration of process ${process?.getName()}: ${e.message}"
        }
    }

    @Override
    void onFlowBegin() {
        hasFlowError = false
        lastFlowErrorEvent = null
        final pipeline = pipelineClient.getPipeline()
        sendPreConfigOnce(pipeline)
        pipelineClient.updatePipelineStatus(fovusConfig, pipeline, FovusPipelineStatus.RUNNING,
                                            session?.getCommandLine())
    }

    private void sendPreConfigOnce(FovusPipeline pipeline) {
        if (!isPreConfigSent.compareAndSet(false, true)) return

        try {
            final nextflowConfig = FovusUtil.readNextflowConfig(session?.getConfigFiles())
            pipelineClient.preConfigResources(fovusConfig, pipeline,
                                              new ArrayList<>(new LinkedHashSet<>(resourceConfigurations.values())),
                                              nextflowConfig)
        } catch (Exception e) {
            log.warn "[FOVUS] Cannot configure pipeline resources: ${e.message}"
        }
    }

    @Override
    void onFlowComplete() {
        if (!isCompletionReported.compareAndSet(false, true)) return

        final boolean isFailed = hasFlowError || session.isAborted()
        log.trace "[FOVUS] Pipeline completed with status ${isFailed ? 'FAILED' : 'COMPLETED'}"

        final status = isFailed ? FovusPipelineStatus.FAILED : FovusPipelineStatus.COMPLETED
        pipelineClient.updatePipelineStatus(fovusConfig, pipelineClient.getPipeline(), status,
                                            session?.getCommandLine())
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

    /**
     * Read the {@code ext} of a process the way its first task attempt sees it: closures run with the
     * script's {@code params} and functions and with {@code task.attempt == 1}. A key whose closure
     * throws, or whose value is not resolved (for instance it needs a task's input), is left out so
     * the job resolves it at submission.
     *
     * @return the resolved keys, empty when the process sets no ext
     */
    @PackageScope
    static Map<String, Object> resolveFirstAttemptExt(TaskProcessor process) {
        final Map<String, Object> lazyExt = process.createTaskPreview().getConfig().get('ext') as Map<String, Object>
        final resolvedExt = new LinkedHashMap<String, Object>()
        for (final String key : lazyExt.keySet().toList()) {
            try {
                final value = lazyExt.get(key)
                if (ResourceConfiguration.isResolvedValue(value)) {
                    resolvedExt.put(key, value)
                } else {
                    logLeftOut(key, process.getName(), "its value is not resolved")
                }
            } catch (Exception e) {
                logLeftOut(key, process.getName(), e.message)
            }
        }
        return resolvedExt
    }

    private static void logLeftOut(String key, String processName, String reason) {
        log.warn "[FOVUS] Leaving ext.${key} of process ${processName} out of the pre-configuration: " +
                 "${reason}; the job resolves it at submission"
    }

    /**
     * @param processName the process the ext belongs to, named in warnings; null for the global ext
     * @return the configuration, or null when {@code ext} has no resolved benchmarking profile name
     */
    static ResourceConfiguration parseExtensionObject(Map ext, String processName) {
        if (!(ext instanceof Map)) return

        final benchmarkingProfileName = ext.get("benchmarkingProfileName")
        if (!benchmarkingProfileName) return
        if (!(benchmarkingProfileName instanceof CharSequence) ||
                !ResourceConfiguration.isResolvedValue(benchmarkingProfileName)) {
            log.warn "[FOVUS] Skipping the pre-configuration of process ${processName ?: 'defaults'}: " +
                     "ext.benchmarkingProfileName is not resolved text"
            return
        }

        def resourceConfig = new ResourceConfiguration(benchmarkingProfileName: benchmarkingProfileName.toString())
        // A JSON job config file is one of the sources a job takes its connectors from, so the
        // payload has to carry those as well; an `ext.storageConnectors` entry below still
        // overrides them, exactly as it does for the job itself.
        resourceConfig.storageConnectors = readJobConfigStorageConnectors(ext.get("jobConfigFile"))

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
                    if (value instanceof CharSequence) {
                        resourceConfig.timeToCostPriorityRatio = value.toString()
                    } else {
                        log.warn "[FOVUS] Ignoring ext.timeToCostPriorityRatio of process ${processName ?: 'defaults'} " +
                                 "because it is not text"
                    }
                    break
                case "isResumableWorkload":
                    if (value instanceof Boolean) {
                        resourceConfig.isResumableWorkload = value
                    }
                    break
                case "isHybridStrategyAllowed":
                    if (value instanceof Boolean) {
                        resourceConfig.isHybridStrategyAllowed = value
                    }
                    break
                case "isMultiRegionScalingAllowed":
                    if (value instanceof Boolean) {
                        resourceConfig.isMultiRegionScalingAllowed = value
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
                case "storageConnectors":
                    // Connectors are resolved and validated exactly as they are for a job, so the
                    // pre-config-resources payload carries the same names the jobs will be created
                    // with. The resolver warns about a value that is not a list and that value is
                    // then left alone, so the job config file connectors above stay in place.
                    final connectors = FovusJobConfig.resolveStorageConnectors(value, null)
                    if (value instanceof List) {
                        // An empty list is assigned too: `ext.storageConnectors = []` clears the
                        // pipeline-wide connectors for this process and has to survive the merge.
                        resourceConfig.storageConnectors = connectors
                    }
                    break
                default:
                    break
            }
        }

        return resourceConfig
    }

    /**
     * Read the storage connectors declared in the JSON job config a process points at.
     *
     * The connectors are validated and de-duplicated exactly as they are for a job, so a malformed
     * name fails the run here rather than at submission time. A file that cannot be read or parsed
     * is ignored: the job path reports that failure with far better context.
     *
     * @return the connectors, or null when the process names no job config file or none are declared
     */
    private static List<String> readJobConfigStorageConnectors(Object jobConfigFilePath) {
        if (!jobConfigFilePath) return null

        FovusJobConfig jobConfig
        try {
            jobConfig = FovusJobConfigBuilder.fromJsonFile(jobConfigFilePath as String)
        } catch (Exception e) {
            log.trace "[FOVUS] Cannot read storage connectors from job config file " +
                      "${jobConfigFilePath}: ${e.message}"
            return null
        }

        return FovusJobConfig.resolveStorageConnectors(null, jobConfig.storageConnectors) ?: null
    }
}
