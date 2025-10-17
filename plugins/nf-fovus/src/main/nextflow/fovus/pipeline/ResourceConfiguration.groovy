package nextflow.fovus.pipeline

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
}
