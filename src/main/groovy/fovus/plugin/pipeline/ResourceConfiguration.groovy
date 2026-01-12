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
    Float minvCpu
    Float maxvCpu
    Float minvCpuMemGiB
    Float minGpu
    Float maxGpu
    Float minGpuMemGiB
    String computingDevice
    Boolean enableHyperthreading
    Boolean allowPreemptible
    String[] supportedCpuArchitectures
    Boolean isResumableWorkload
    Float walltimeHours
    Float storageGiB
    Boolean isMemoryAutoRetryEnabled

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
                walltimeHours: other.walltimeHours ?: this.walltimeHours,
                storageGiB: other.storageGiB ?: this.storageGiB,
                isMemoryAutoRetryEnabled: other.isMemoryAutoRetryEnabled != null ? other.isMemoryAutoRetryEnabled :
                                          this.isMemoryAutoRetryEnabled
        )
    }
}
