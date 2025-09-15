package nextflow.fovus

import groovy.transform.CompileStatic
import groovy.transform.PackageScope

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
@CompileStatic
class FovusConfig {

    final private String cliPath
    final private String pipelineName

    FovusConfig(Map config) {
        this.cliPath = config.cliPath ?: "fovus"
        this.pipelineName = config.pipelineName ?: "Nextflow Unnamed Pipeline"
    }

    FovusConfig() {
        this.cliPath = "fovus"
        this.pipelineName = "Nextflow Unnamed Pipeline"
    }

    String getCliPath() { cliPath }

    String getPipelineName() { pipelineName }
}
