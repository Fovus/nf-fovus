package nextflow.fovus.job

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import groovy.json.JsonOutput
import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.transform.MapConstructor
import groovy.util.logging.Slf4j
import nextflow.fovus.FovusExecutor
import nextflow.fovus.FovusUtil
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

    @JsonDeserialize(using = EnvironmentDeserializer)
    Environment environment
    Constraints constraints
    Objective objective
    Workload workload
    String jobName

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

    FovusJobConfig(){}

    FovusJobConfig(TaskRun task) {
        def extension = task.config.get('ext') as Map<String, Object>
        if(extension?.jobConfigFile == null){
            throw new Error("jobConfigFile file path is missing!")
        }

        def fovusJobConfig = FovusJobConfigBuilder.fromJsonFile(extension.jobConfigFile as String)
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
        final extension = task.config.get('ext') as Map<String, Object>;
        if(extension?.container != null || fovusJobConfig.getEnvironment() instanceof ContainerizedEnvironment){
            def existingContainerizedEnv = fovusJobConfig.getEnvironment() as ContainerizedEnvironment
            def Containerized containerized = new Containerized(
                    container: extension?.container ?: existingContainerizedEnv.containerized.container,
                    version: extension?.version ?: existingContainerizedEnv.containerized.version,
                    imagePath: extension?.imagePath ?: existingContainerizedEnv.containerized.imagePath,
            );
            return new ContainerizedEnvironment(containerized: containerized)
        } else {
            // TODO: Add support for adding monolithic software
        }
    }

    private JobConstraints createJobConstraints(FovusJobConfig fovusJobConfig) {
        final extension = task.config.get('ext') as Map<String, Object>

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
                benchmarkingProfileName: extension?.benchmarkingProfileName ?: defaultJobConstraints.benchmarkingProfileName,
                computingDevice: extension?.computingDevice ?: defaultJobConstraints.computingDevice,
                allowPreemptible: extension?.allowPreemptible ?: defaultJobConstraints.allowPreemptible,
                enableHyperthreading: extension?.enableHyperthreading ?: defaultJobConstraints.enableHyperthreading,
                isHybridStrategyAllowed: extension?.isHybridStrategyAllowed ?: defaultJobConstraints.isHybridStrategyAllowed,
                supportedCpuArchitectures: cpuArchitectures,
                isResumableWorkload: extension?.isResumableWorkload ?: defaultJobConstraints.isResumableWorkload,
                isSubjectToLicenseAvailability: extension?.isSubjectToLicenseAvailability ?: defaultJobConstraints.isSubjectToLicenseAvailability,
        )
    }

    private TaskConstraints createTaskConstraints(FovusJobConfig fovusJobConfig) {
        final extension = task.config.get('ext') as Map<String, Object>
        def defaultTaskConstaints = fovusJobConfig.constraints.getTaskConstraints();
        return new TaskConstraints(
                minvCpu: extension?.minvCpu as Integer ?: defaultTaskConstaints.minvCpu,
                maxvCpu: extension?.maxvCpu as Integer ?: defaultTaskConstaints.maxvCpu,
                minvCpuMemGiB: extension?.minvCpuMemGiB as Integer ?: defaultTaskConstaints.minvCpuMemGiB,
                minGpu: extension?.minGpu as Integer ?: defaultTaskConstaints.maxvCpu,
                maxGpu: extension?.maxGpu as Integer ?: defaultTaskConstaints.maxGpu,
                minGpuMemGiB: extension?.minGpuMemGiB as Integer ?: defaultTaskConstaints.minGpuMemGiB,
                storageGiB: extension?.storageGiB as Integer ?: defaultTaskConstaints.storageGiB,
                walltimeHours: extension?.walltimeHours as Integer ?: defaultTaskConstaints.walltimeHours,
                isSingleThreadedTask: extension?.isSingleThreadedTask ?: defaultTaskConstaints.isSingleThreadedTask,
                scalableParallelism: extension?.scalableParallelism ?: defaultTaskConstaints.scalableParallelism,
                parallelismOptimization: extension?.parallelismOptimization ?: defaultTaskConstaints.parallelismOptimization,
        )
    }

    private Objective createObjective(FovusJobConfig fovusJobConfig) {
        def extension = task.config.get('ext') as Map<String, Object>
        return new Objective(timeToCostPriorityRatio: extension?.timeToCostPriorityRatio ?: fovusJobConfig.objective.timeToCostPriorityRatio)
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
                outputFileOption: extension.outputFileOption ?: defaultWorkload.outputFileOption,
                outputFileList: outputFileList
        )
    }


    /**
     * Skip syncing input files (eg, outputs from previous jobs) to remote storage
     */
    void skipRemoteInputSync(FovusExecutor executor) {
        task.getInputFilesMap().each { stageName, filePath ->
            {
                final isRemoteFile = FovusUtil.isFovusRemoteFile(executor, filePath)
                if (isRemoteFile) {
                    workload.outputFileOption = 'exclude'
                    workload.outputFileList = []
                    workload.outputFileList << stageName
                }
            }
        }

    }

    /**
     * Save the job config to a JSON file and return the file path.
     */
    String toJson() {
        final workDir = task.workDir
        final jobConfigFile = workDir.resolve("${jobName}_config.json")

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



