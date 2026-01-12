package fovus.plugin.job

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import groovy.json.JsonOutput
import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.util.logging.Slf4j
import nextflow.processor.TaskRun

import java.nio.file.Files
import java.nio.file.Path

/**
 * Configurations for Fovus job.
 *
 * Precedences of configuration (highest to lowest):
 *   <li><b>Process-level overrides</b> in {@code ext}: explicit settings on the Nextflow process.</li>
 *   <li><b>JSON job config file</b>: provide default configuration values for a single process. Usage: Specify the path to the JSON job config file via {@code ext.jobConfigFile}.</li>
 *   <li><b>Benchmark's defaults</b>: When the JSON was not specified for a process, the configuration will be loaded from the benchmarking profile name.</li>
 */
@Slf4j
@CompileStatic
class FovusJobConfig {

    @JsonDeserialize(using = EnvironmentDeserializer)
    Environment environment
    Constraints constraints
    Objective objective
    Workload workload
    String jobName
    FovusJobClient jobClient

    private final TaskRun task

    void setEnvironment(Environment environment) {
        this.environment = environment
    }

    void setConstraints(Constraints constraints) {
        this.constraints = constraints
    }

    void setWorkload(Workload workload) {
        this.workload = workload
    }

    void setObjective(Objective objective) {
        this.objective = objective
    }

    void setJobName(String jobName) {
        this.jobName = jobName
    }

    void setRunCommand(String runCmd) {
        this.workload.runCommand = runCmd
    }

    FovusJobConfig(){}

    FovusJobConfig(FovusJobClient jobClient, TaskRun task) {
        def extension = task.config.get('ext') as Map<String, Object>
        def jobConfigFilePath = (extension?.jobConfigFile != null)
                ? extension.jobConfigFile
                : task.config.get('jobConfigFile')

        def benchmarkingProfileName = (extension?.benchmarkingProfileName != null)
                ? extension.benchmarkingProfileName
                : task.config.get('benchmarkingProfileName')
        def fovusJobConfig

        if (jobConfigFilePath) {
            fovusJobConfig = FovusJobConfigBuilder.fromJsonFile(jobConfigFilePath as String)
        } else {
            def defaultConfigFromBenchmarkName = jobClient.getDefaultJobConfig((benchmarkingProfileName ?: "Default") as String)

            if (!defaultConfigFromBenchmarkName || defaultConfigFromBenchmarkName == "{}") {
                throw new Error("[Fovus] No default job config found")
            }
            fovusJobConfig = FovusJobConfigBuilder.fromJsonString(defaultConfigFromBenchmarkName)
        }

        this.task = task
        this.environment = createEnvironment(fovusJobConfig)
        def jobConstraints = createJobConstraints(fovusJobConfig)
        def taskConstraints = createTaskConstraints(fovusJobConfig)
        this.objective = createObjective(fovusJobConfig)
        this.constraints = new Constraints(jobConstraints: jobConstraints, taskConstraints: taskConstraints)
        this.workload = createWorkload(fovusJobConfig)
        this.jobName = normalizeJobName(task.name)

    }

    private Environment createEnvironment(FovusJobConfig fovusJobConfig) {
        def extension = task.config.get('ext') as Map<String, Object>;

        def isContainerizedWorkload = (extension?.container != null) ||
                                      (fovusJobConfig.getEnvironment() != null &&
                                       fovusJobConfig.getEnvironment() instanceof ContainerizedEnvironment) ||
                                      task.containerConfig.enabled
        if (isContainerizedWorkload) {
            // Create a new Containerized object, using extension values if not null
            def existingContainerizedEnv = fovusJobConfig.getEnvironment() as ContainerizedEnvironment
            def container, version, imagePath

            // Parse and get the container type
            if (extension?.container != null) {
                container = extension.container
            } else if (existingContainerizedEnv != null && existingContainerizedEnv.containerized.container != null) {
                container = existingContainerizedEnv.containerized.container
            } else if (task.containerConfig.engine == "apptainer") {
                container = "Apptainer"
            } else {
                container = "Docker"
            }

            // Parse and get the container version
            if (extension?.version != null) {
                version = extension.version
            } else if (existingContainerizedEnv != null && existingContainerizedEnv.containerized.version != null) {
                version = existingContainerizedEnv.containerized.version
            } else {
                version = "20.10.18"
            }

            // Parse and get the container image path
            if (extension?.imagePath != null) {
                imagePath = extension.imagePath
            } else if (existingContainerizedEnv != null && existingContainerizedEnv.containerized.imagePath != null) {
                imagePath = existingContainerizedEnv.containerized.imagePath
            } else {
                imagePath = task.container ?: ""
            }

            def containerized = new Containerized(
                    container: container,
                    version: version,
                    imagePath: imagePath
            )
            return new ContainerizedEnvironment(containerized: containerized)
        } else {
            // TODO: Add support for adding monolithic software
        }

    }

    private JobConstraints createJobConstraints(FovusJobConfig fovusJobConfig) {
        def extension = task.config.get('ext') as Map<String, Object>;
        def defaultJobConstraints = fovusJobConfig.getConstraints().jobConstraints;
        def cpuArchitectures = defaultJobConstraints.supportedCpuArchitectures;

        if(extension?.supportedCpuArchitectures != null){
            switch(extension?.computingDevice){
                case "x86-64":
                    cpuArchitectures = ["x86-64"]
                    break;
                case "arm64":
                    cpuArchitectures = ["arm-64"]
                    break;
                case "x86-64 + arm-64":
                    cpuArchitectures = ["x86-64", "arm-64"]
                    break;
            }
        }
        return new JobConstraints(
                benchmarkingProfileName: (extension?.benchmarkingProfileName != null)
                        ? extension.benchmarkingProfileName
                        : defaultJobConstraints.benchmarkingProfileName,
                computingDevice: (extension?.computingDevice != null)
                        ? extension.computingDevice
                        : defaultJobConstraints.computingDevice,
                allowPreemptible: (extension?.allowPreemptible != null)
                        ? extension.allowPreemptible
                        : defaultJobConstraints.allowPreemptible,
                enableHyperthreading: (extension?.enableHyperthreading != null)
                        ? extension.enableHyperthreading
                        : defaultJobConstraints.enableHyperthreading,
                isHybridStrategyAllowed: (extension?.isHybridStrategyAllowed != null)
                        ? extension.isHybridStrategyAllowed
                        : defaultJobConstraints.isHybridStrategyAllowed,
                supportedCpuArchitectures: cpuArchitectures,
                isResumableWorkload: (extension?.isResumableWorkload != null)
                        ? extension.isResumableWorkload
                        : defaultJobConstraints.isResumableWorkload,
                isSubjectToLicenseAvailability: (extension?.isSubjectToLicenseAvailability != null)
                        ? extension.isSubjectToLicenseAvailability
                        : defaultJobConstraints.isSubjectToLicenseAvailability,
                isMemoryAutoRetryEnabled: (extension?.isMemoryAutoRetryEnabled !== null)
                        ? extension.isMemoryAutoRetryEnabled
                        : defaultJobConstraints.isMemoryAutoRetryEnabled
        )
    }

    private TaskConstraints createTaskConstraints(FovusJobConfig fovusJobConfig) {
        def extension = task.config.get('ext') as Map<String, Object>;
        final nfStorage = task.config.getDisk()?.toGiga()?.toInteger()

        def defaultTaskConstraints = fovusJobConfig.constraints.getTaskConstraints();
        return new TaskConstraints(
                minvCpu: (extension?.minvCpu != null) ? extension.minvCpu as Integer : defaultTaskConstraints.minvCpu,
                maxvCpu: (extension?.maxvCpu != null) ? extension.maxvCpu as Integer : defaultTaskConstraints.maxvCpu,
                minvCpuMemGiB: (extension?.minvCpuMemGiB != null) ? extension.minvCpuMemGiB as Integer : defaultTaskConstraints.minvCpuMemGiB,
                minGpu: (extension?.minGpu != null) ? extension.minGpu as Integer : defaultTaskConstraints.minGpu,
                maxGpu: (extension?.maxGpu != null) ? extension.maxGpu as Integer : defaultTaskConstraints.maxGpu,
                minGpuMemGiB: (extension?.minGpuMemGiB != null) ? extension.minGpuMemGiB as Integer : defaultTaskConstraints.minGpuMemGiB,
                storageGiB: (extension?.storageGiB != null) ? extension.storageGiB as Integer : (nfStorage ?: defaultTaskConstraints.storageGiB),
                walltimeHours: (extension?.walltimeHours != null) ? extension.walltimeHours as Integer : defaultTaskConstraints.walltimeHours,
                isSingleThreadedTask: (extension?.isSingleThreadedTask != null) ? extension.isSingleThreadedTask : defaultTaskConstraints.isSingleThreadedTask,
                scalableParallelism: (extension?.scalableParallelism != null) ? extension.scalableParallelism : defaultTaskConstraints.scalableParallelism,
                parallelismOptimization: (extension?.parallelismOptimization != null) ? extension.parallelismOptimization : defaultTaskConstraints.parallelismOptimization
        )
    }

    private Objective createObjective(FovusJobConfig fovusJobConfig) {
        def extension = task.config.get('ext') as Map<String, Object>
        return new Objective(
                timeToCostPriorityRatio: (extension?.timeToCostPriorityRatio != null)
                        ? extension.timeToCostPriorityRatio
                        : fovusJobConfig.objective.timeToCostPriorityRatio
        )
    }

    private Workload createWorkload(FovusJobConfig fovusJobConfig) {
        def extension = task.config.get('ext') as Map<String, Object>
        def defaultWorkload = fovusJobConfig.workload;

        def remoteInputsForAllTasks = defaultWorkload.remoteInputsForAllTasks
        def parallelismConfigFiles = defaultWorkload.parallelismConfigFiles
        def outputFileList = defaultWorkload.outputFileList

        if(extension?.remoteInputsForAllTasks){
            remoteInputsForAllTasks = (extension.remoteInputsForAllTasks as String).split(",").toList();
        }
        if(extension?.parallelismConfigFiles){
            parallelismConfigFiles = (extension.parallelismConfigFiles as String).split(",").toList();
        }
        if(extension?.outputFileList){
            parallelismConfigFiles = (extension.outputFileList as String).split(",").toList();
        }

        // Get the script file and run it
        final runCommand = "./${TaskRun.CMD_RUN}"

        return new Workload(
                runCommand: runCommand,
                remoteInputsForAllTasks: remoteInputsForAllTasks,
                parallelismConfigFiles: parallelismConfigFiles,
                outputFileOption: (extension?.outputFileOption != null)
                        ? extension.outputFileOption
                        : defaultWorkload.outputFileOption,
                outputFileList: outputFileList
        )
    }


    /**
     * Save the job config to a JSON file and return the file path.
     */
    String toJson(Path jobConfigFile) {
        // Write the job config to a file
        def jsonString = JsonOutput.prettyPrint(JsonOutput.toJson(this))
        Files.write(jobConfigFile, jsonString.bytes)

        log.debug "[FOVUS] Job config file for ${task.name} saved to ${jobConfigFile.toString()}"

        return jobConfigFile.toString()
    }
    /**
     * Remove invalid characters from a job name string
     *
     * @param name A job name containing possible invalid character
     * @return A job name without invalid characters
     */
    protected String normalizeJobName(String name) {
        def result = name.replaceAll(' ','_').replaceAll(':', '-').replaceAll(/[^a-zA-Z0-9_-]/,'')
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
    boolean isSubjectToLicenseAvailability = false
    boolean isMemoryAutoRetryEnabled = false
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



