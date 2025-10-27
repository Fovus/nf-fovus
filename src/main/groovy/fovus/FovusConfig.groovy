package groovy.fovus

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import nextflow.config.schema.ConfigOption
import nextflow.config.schema.ConfigScope
import nextflow.config.schema.ScopeName
import nextflow.script.dsl.Description

/**
 *
 * Global configurations needed to run Fovus commands.
 *
 * User can specify the configurations in the nextflow.config file.
 * For example:
 *
 * fovus {
 *     cliPath = '/opt/miniconda3/bin/fovus'
 * }
 */
@ScopeName('fovus')
@Description('Configurations for running Nextflow pipelines on Fovus.')
@CompileStatic
class FovusConfig implements ConfigScope {
    @ConfigOption
    @Description("""
        Path to the local installation of the Fovus CLI.
        This option is useful when the Fovus CLI is not in the system PATH (e.g., the Fovus CLI is installed in a conda environment). 
        
        Defaults to `fovus`.
    """)
    final public String cliPath

    @ConfigOption
    @Description("""
        (Required) A name (e.g., `my-pipeline`) representing this Nextflow workflow on Fovus. 
        
        If an existing pipeline name is found in the local cache, the same pipeline will be used.
        Otherwise, a new pipeline will be created.
        
        The pipeline and its jobs can be found at `https://app.fovus.co/pipelines`. 
    """)
    final public String pipelineName

    @ConfigOption
    @Description('(Optional) The project name to group jobs and pipelines for budget management.')
    final public String projectName

    /** Required by extension point - DO NOT REMOVE */
    FovusConfig() {}

    FovusConfig(Map config) {
        this.cliPath = config.cliPath ?: "fovus"
        this.pipelineName = config.pipelineName

        if (pipelineName == null || pipelineName.isEmpty()) {
            throw new IllegalArgumentException("[FOVUS] Pipeline name is required.")
        }

        this.projectName = config.projectName ?: null
    }

    String getCliPath() {cliPath}

    String getPipelineName() { pipelineName }
}
