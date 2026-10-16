package fovus.plugin.pipeline

import groovy.transform.Canonical
import groovy.transform.EqualsAndHashCode
import groovy.transform.MapConstructor

@Canonical
@MapConstructor
@EqualsAndHashCode
class ResourceConfiguration {
    String benchmarkingProfileName
    String timeToCostPriorityRatio
    Number minvCpu
    Number maxvCpu
    Number minvCpuMemGiB
    Number minGpu
    Number maxGpu
    Number minGpuMemGiB
    String computingDevice
    Boolean enableHyperthreading
    Boolean allowPreemptible
    String[] supportedCpuArchitectures
    Boolean isResumableWorkload
    Boolean isHybridStrategyAllowed
    Boolean isMultiRegionScalingAllowed
    Number walltimeHours
    Number storageGiB
    Boolean isMemoryAutoRetryEnabled
    List<String> storageConnectors

    /**
     * Tells whether a config value holds real data. Nextflow hands over a closure, or a ConfigObject for
     * an unknown name, when it could not evaluate an entry; those must never be sent to Fovus. A closure
     * that was coerced into a String leaves its class name behind, which is what the text check looks for.
     */
    static boolean isResolvedValue(Object value) {
        if (value instanceof Closure || value instanceof ConfigObject) {
            return false
        }
        if (value instanceof CharSequence) {
            final text = value.toString()
            return !(text.contains('_nf_config_') || text.contains('$_closure') || text.contains('$_run_closure'))
        }
        if (value instanceof Map) {
            return value.values().every { isResolvedValue(it) }
        }
        if (value instanceof Collection || value instanceof Object[]) {
            return value.every { isResolvedValue(it) }
        }
        return true
    }

    boolean hasOnlyResolvedValues() {
        return properties.every { name, value -> name == 'class' || isResolvedValue(value) }
    }

    /**
     * Merges this configuration with another configuration.
     * Values from the provided configuration take precedence over this instance's values.
     * @param other The configuration to merge with
     * @return A new ResourceConfiguration with merged values
     */
    ResourceConfiguration mergeWith(ResourceConfiguration other) {
        if (!other) {
            return this
        }

        return new ResourceConfiguration(
                benchmarkingProfileName: other.benchmarkingProfileName ?: this.benchmarkingProfileName,
                timeToCostPriorityRatio: other.timeToCostPriorityRatio ?: this.timeToCostPriorityRatio,
                minvCpu: other.minvCpu ?: this.minvCpu,
                maxvCpu: other.maxvCpu ?: this.maxvCpu,
                minvCpuMemGiB: other.minvCpuMemGiB ?: this.minvCpuMemGiB,
                minGpu: other.minGpu ?: this.minGpu,
                maxGpu: other.maxGpu ?: this.maxGpu,
                minGpuMemGiB: other.minGpuMemGiB ?: this.minGpuMemGiB,
                computingDevice: other.computingDevice ?: this.computingDevice,
                enableHyperthreading: other.enableHyperthreading != null ? other.enableHyperthreading :
                                      this.enableHyperthreading,
                allowPreemptible: other.allowPreemptible != null ? other.allowPreemptible : this.allowPreemptible,
                supportedCpuArchitectures: other.supportedCpuArchitectures ?: this.supportedCpuArchitectures,
                isResumableWorkload: other.isResumableWorkload != null ? other.isResumableWorkload :
                                     this.isResumableWorkload,
                isHybridStrategyAllowed: other.isHybridStrategyAllowed != null ? other.isHybridStrategyAllowed :
                                         this.isHybridStrategyAllowed,
                isMultiRegionScalingAllowed: other.isMultiRegionScalingAllowed != null ?
                                             other.isMultiRegionScalingAllowed : this.isMultiRegionScalingAllowed,
                walltimeHours: other.walltimeHours ?: this.walltimeHours,
                storageGiB: other.storageGiB ?: this.storageGiB,
                isMemoryAutoRetryEnabled: other.isMemoryAutoRetryEnabled != null ? other.isMemoryAutoRetryEnabled :
                                          this.isMemoryAutoRetryEnabled,
                // An explicit empty list is an override, not an absent value: a process that
                // clears the pipeline-wide connectors must not have them merged back in.
                storageConnectors: other.storageConnectors != null ? other.storageConnectors :
                                   this.storageConnectors
        )
    }
}
