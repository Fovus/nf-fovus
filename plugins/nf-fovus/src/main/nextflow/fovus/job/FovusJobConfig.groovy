package nextflow.fovus.job

import groovy.json.JsonOutput
import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.util.logging.Slf4j
import nextflow.processor.TaskRun

import java.nio.file.Files

/**
 * Configurations for Fovus job.
 *
 * Responsible for mapping NextFlow task configs to Fovus job configs.
 */
@Slf4j
@CompileStatic
class FovusJobConfig {
    Environment environment
    Constraints constraints
    Objective objective
    Workload workload
    String jobName

    private final TaskRun task

    FovusJobConfig(TaskRun task) {
        this.task = task
        this.environment = createEnvironment()

        def jobConstraints = createJobConstraints()
        def taskConstraints = createTaskConstraints(jobConstraints.enableHyperthreading)

        this.objective = createObjective()

        this.constraints = new Constraints(jobConstraints: jobConstraints, taskConstraints: taskConstraints)
        this.workload = createWorkload()
        this.jobName = normalizeJobName(task.name)
    }

    private Environment createEnvironment() {

        def Containerized containerized = new Containerized()

        if (task.container) {
            def imagePath = task.container ?: ""
            def engine = task.containerConfig.engine

            if (engine != 'docker' && engine != 'apptainer') {
                throw new IllegalArgumentException("Unsupported container engine: ${engine}")
            }

            if (engine == 'apptainer') {
                containerized = new Containerized(container: 'Apptainer', imagePath: imagePath, version: "1.3.4")
            } else {
                // Default to docker
                containerized = new Containerized(container: 'Docker', imagePath: imagePath, version: "20.10.8")
            }
        }

        // TODO: Add support for adding monolithic software
        return new ContainerizedEnvironment(containerized: containerized)
    }

    private JobConstraints createJobConstraints() {
        final extension = task.config.get('ext') as Map<String, Object>
        final accelerator = task.config.getAccelerator()

        def isGpuUsed = false
        if (accelerator) {
            if (accelerator.type) {
                log.warn "Ignoring task ${task.lazyName()} accelerator type ${accelerator.type} - " + "Fovus only supports GPU accelerators"
            }
            isGpuUsed = true
        }

        def benchmarkingProfileName = extension?.benchmarkingProfileName

        if (!benchmarkingProfileName) {
            if (isGpuUsed) {
                benchmarkingProfileName = "Default GPU"
            } else {
                benchmarkingProfileName = "Default CPU"
            }
        }

        def computingDevice = isGpuUsed ? "cpu+gpu" : "cpu"
        // TODO: Add support for other configurations such as spot instances, supported architectures, etc.
        return new JobConstraints(benchmarkingProfileName: benchmarkingProfileName, computingDevice: computingDevice)
    }

    private TaskConstraints createTaskConstraints(Boolean isHyperthreadingEnabled = false) {
        final cpus = task.config.getCpus()
        final memory = task.config.getMemory()
        final extension = task.config.get('ext') as Map<String, Object>
        final storage = task.config.getDisk()

        final accelerator = task.config.getAccelerator()

        def taskConstraints = new TaskConstraints(
                minvCpu: cpus,
                maxvCpu: cpus,
                minvCpuMemGiB: memory?.toGiga()?.toInteger() ?: 8,
                minGpu: accelerator?.request ?: 0,
                maxGpu: accelerator?.limit ?: accelerator?.request ?: 0,
                minGpuMemGiB: (extension?.minGpuMemGiB ?: 8) as Integer,
                storageGiB: storage?.toGiga()?.toInteger() ?: 100,
                walltimeHours: (extension?.walltimeHours ?: 3) as Integer
        ).overrideCpuConstraints(isHyperthreadingEnabled)

        return taskConstraints
    }

    private Objective createObjective() {
        def extension = task.config.get('ext') as Map<String, Object>
        def timeToCostPriorityRatio = extension?.timeToCostPriorityRatio ?: "0.5/0.5"

        return new Objective(timeToCostPriorityRatio: timeToCostPriorityRatio)
    }

    private Workload createWorkload() {
        // TODO: Add support for remote inputs (both user-specified and Nextflow generated)
        def remoteInputsForAllTasks = []
        def parallelismConfigFiles = []

        // Get the script file and run it
        final runCommand = "./${TaskRun.CMD_RUN}"

        return new Workload(
                runCommand: runCommand,
                remoteInputsForAllTasks: remoteInputsForAllTasks,
                parallelismConfigFiles: parallelismConfigFiles
        )
    }

    /**
     * Save the job config to a JSON file and return the file path.
     */
    String toJson() {
        final workDir = task.workDir
        final jobConfigFile = workDir.resolve("job_config.json")

        // Write the job config to a file
        def jsonString = JsonOutput.prettyPrint(JsonOutput.toJson(this))
        Files.write(jobConfigFile, jsonString.bytes)

        log.trace "[FOVUS] Job config file for ${task.name} saved to ${jobConfigFile.toString()}"

        return jobConfigFile.toString()
    }
    /**
     * Remove invalid characters from a job name string
     *
     * @param name A job name containing possible invalid character
     * @return A job name without invalid characters
     */
    protected String normalizeJobName(String name) {
        def result = name.replaceAll(' ','_').replaceAll(/[^a-zA-Z0-9_-]/,'')
        result.size()>128 ? result.substring(0,128) : result
    }

}

interface Environment {}

@Canonical
@MapConstructor
class ContainerizedEnvironment implements Environment {
    Containerized containerized
}

@Canonical
@MapConstructor
class MonolithicEnvironment implements Environment {
    List<MonolithicSoftware> monolithicList = []
}

@Canonical
@MapConstructor
class Containerized {
    String container = "Docker"
    String version = "20.10.8"
    String imagePath = ""
}

@Canonical
@MapConstructor
class MonolithicSoftware {
    // Required fields
    String softwareName
    String vendorName
    String softwareVersion
    String licenseFeature

    // Optional fields
    String licenseAddress
    String licenseName
    String licenseConsumptionProfileName
    String licenseId
    int licenseCountPerTask
}

@Canonical
@MapConstructor
class Constraints {
    JobConstraints jobConstraints
    TaskConstraints taskConstraints
}

@Canonical
@MapConstructor
class JobConstraints {
    String computingDevice = "cpu"
    String benchmarkingProfileName = "Default CPU"
    List<String> supportedCpuArchitectures = ["x86-64", "arm-64"]
    boolean isHybridStrategyAllowed = false
    boolean enableHyperthreading = false
    boolean allowPreemptible = false
    boolean isResumableWorkload = false
}

@Canonical
@MapConstructor
class TaskConstraints {
    int minvCpu = 1
    int maxvCpu = 192
    int minvCpuMemGiB = 1
    int minGpu = 0
    int maxGpu = 0
    int minGpuMemGiB = 0
    int storageGiB = 100
    int walltimeHours = 3
    boolean isSingleThreadedTask = false
    boolean scalableParallelism = false
    boolean parallelismOptimization = false

    /**
     * Override the configurations based on whether GPU is used and hyperthreading is enabled
     */
    TaskConstraints overrideCpuConstraints(Boolean isHyperthreadingEnabled) {
        def requiredMinCpu = minGpu > 0 ? 2 : 1
        if (isHyperthreadingEnabled) {
            requiredMinCpu = requiredMinCpu * 2
        }

        if (minvCpu >= requiredMinCpu) return this;

        minvCpu = requiredMinCpu
        if (maxvCpu < requiredMinCpu) {
            maxvCpu = requiredMinCpu
        }

        return this
    }
}

@Canonical
@MapConstructor
class Objective {
    String timeToCostPriorityRatio = "0.5/0.5"
}

@Canonical
@MapConstructor
class Workload {
    List<String> remoteInputsForAllTasks = []
    List<String> parallelismConfigFiles = []
    String runCommand
    String outputFileOption = "include"
    List<String> outputFileList = ["*"]
}



