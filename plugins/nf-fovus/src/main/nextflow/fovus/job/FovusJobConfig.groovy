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
        def taskConstraints = createTaskConstraints()

        this.objective = createObjective()

        this.constraints = new Constraints(jobConstraints: jobConstraints, taskConstraints: taskConstraints)
        this.workload = createWorkload()
        this.jobName = task.name
    }

    private Environment createEnvironment() {
        if (task.container) {
            def imagePath = task.container
            def engine = task.containerConfig.engine

            if (engine != 'docker' && engine != 'apptainer') {
                throw new IllegalArgumentException("Unsupported container engine: ${engine}")
            }

            if (engine == 'docker') {
                return new Containerized(container: engine, imagePath: imagePath, version: "20.10.8")
            } else {
                return new Containerized(container: engine, imagePath: imagePath, version: "1.3.4")
            }
        }

        // TODO: Add support for adding monolithic software
        return new Containerized(container: "Docker", imagePath: "", version: "20.10.8")
    }

    private JobConstraints createJobConstraints() {
        final extension = task.config.get('ext') as Map<String, Object>
        final accelerator = task.config.getAccelerator()

        def isGpuUsed = false
        if (accelerator && accelerator.type) {
            log.warn "Ignoring task ${task.lazyName()} accelerator type ${accelerator.type} - Fovus only supports GPU accelerators"
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

        // TODO: Add support for other configurations such as spot instances
        return new JobConstraints(benchmarkingProfileName: benchmarkingProfileName)
    }

    private TaskConstraints createTaskConstraints() {
        final cpus = task.config.getCpus()
        final memory = task.config.getMemory()
        final extension = task.config.get('ext') as Map<String, Object>
        final storage = task.config.getDisk()

        final accelerator = task.config.getAccelerator()

        return new TaskConstraints(
                minvCpu: cpus,
                maxvCpu: cpus,
                minvCpuMemGiB: memory?.toGiga()?.toInteger() ?: 8,
                minGpu: accelerator?.request ?: 0,
                maxGpu: accelerator?.request ?: 0,
                minGpuMemGiB: (extension?.minGpuMemGiB ?: 8) as Integer,
                storageGiB: storage?.toGiga()?.toInteger() ?: 100,
                walltimeHours: (extension?.walltimeHours ?: 3) as Integer
        )
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
        final runCommand = "./${TaskRun.CMD_SCRIPT}"

        return new Workload(
                runCommand: runCommand,
                remoteInputsForAllTasks: remoteInputsForAllTasks,
                parallelismConfigFiles: parallelismConfigFiles
        )
    }

    void toJson() {
        final workDir = task.workDir
        final jobConfigFile = workDir.resolve("${jobName}_config.json")

        // Write the job config to a file
        def jsonString = JsonOutput.prettyPrint(JsonOutput.toJson(this))
        Files.write(jobConfigFile, jsonString.bytes)

        log.trace "[FOVUS] Job config file for ${task.name} saved to ${jobConfigFile.toString()}"
    }
}

@Canonical
abstract class Environment {}

@Canonical
@MapConstructor
class Containerized extends Environment {
    String container = "Docker"
    String version = "20.10.8"
    String imagePath = ""
}

@Canonical
@MapConstructor
class Monolithic extends Environment {
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

    List<MonolithicSoftware> monolithicList = []
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
    String benchmarkingProfileName = "Default CPU"
    List<String> supportedCpuArchitectures = ["x86-64"]
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



