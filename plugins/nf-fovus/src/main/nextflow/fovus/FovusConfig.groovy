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

    FovusConfig(Map config) {
        this.cliPath = config.cliPath ?: "fovus"
    }

    String getCliPath() {cliPath}
}
