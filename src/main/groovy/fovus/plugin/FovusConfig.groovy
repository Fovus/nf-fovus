package fovus.plugin

import fovus.plugin.util.FovusEnvironment
import groovy.transform.CompileStatic
import jdk.jfr.Description
import nextflow.Session
import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.ScopeName


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

    @ConfigOption
    @Description('(Optional) Non-interactive Fovus CLI authentication for automated runs. See `fovus.auth`.')
    final public FovusAuthConfig auth

    /** Required by extension point - DO NOT REMOVE */
    FovusConfig() {
        this.auth = new FovusAuthConfig()
    }

    FovusConfig(Map config) {
        this(config, new FovusAuthConfig())
    }

    FovusConfig(Map config, FovusAuthConfig auth) {
        this.cliPath = config.cliPath ?: "fovus"
        this.pipelineName = config.pipelineName

        if (pipelineName == null || pipelineName.isEmpty()) {
            throw new IllegalArgumentException("[FOVUS] Pipeline name is required.")
        }

        this.projectName = config.projectName ?: null
        this.auth = auth ?: new FovusAuthConfig()
    }

    String getCliPath() {cliPath}

    String getPipelineName() { pipelineName }

    FovusAuthConfig getAuth() { auth }

    /**
     * @return The environment every `fovus` CLI subprocess spawned with this config should carry,
     *  ie {@code FOVUS_EMAIL}/{@code FOVUS_PAT} when {@link #auth} is configured, unchanged otherwise.
     */
    Map<String, String> cliEnv() {
        return FovusAuthConfig.buildEnvironment(auth, [:])
    }

    /**
     * Scrub the configured personal access token out of {@code text}, eg before logging CLI output
     * or embedding it in an exception message. A no-op when {@link #auth} is not configured.
     */
    String redactSecret(String text) {
        return FovusUtil.redact(text, auth?.personalAccessToken)
    }

    /**
     * Build the `fovus` config for a run, resolving `fovus.auth` with the `LOCAL`-vs-`REMOTE`
     * workflow-host scoping applied (see {@link FovusAuthConfig#resolve}).
     */
    static FovusConfig fromSession(Session session) {
        final fovusConfigMap = session.config.navigate('fovus') as Map
        final authConfig = FovusAuthConfig.resolve(fovusConfigMap?.get('auth') as Map, session?.getConfigFiles(),
                                                    FovusEnvironment.isHostedMode())
        return new FovusConfig(fovusConfigMap, authConfig)
    }
}
