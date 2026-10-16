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
}
