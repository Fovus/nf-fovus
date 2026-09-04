package fovus.plugin

import groovy.transform.CompileStatic
import jdk.jfr.Description
import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.ScopeName

import java.nio.file.Path

/**
 * Non-interactive Fovus CLI login for automated runs.
 *
 * User can specify their Fovus login email and personal access token in the nextflow.config file,
 * sourced only from Nextflow's secrets store, so the Fovus CLI can authenticate without a human ever
 * running `fovus auth login`:
 *
 * fovus {
 *     auth {
 *         email               = secrets.FOVUS_EMAIL
 *         personalAccessToken = secrets.FOVUS_PAT
 *     }
 * }
 */
@ScopeName('auth')
@Description('Non-interactive Fovus CLI authentication for automated runs.')
@CompileStatic
class FovusAuthConfig implements ConfigScope {

    static final String EMAIL_PATH = 'fovus.auth.email'
    static final String PERSONAL_ACCESS_TOKEN_PATH = 'fovus.auth.personalAccessToken'

    @ConfigOption
    @Description("""
        (Required together with `personalAccessToken`) The Fovus account email to authenticate as.

        Must be set via Nextflow's secrets store, eg `email = secrets.FOVUS_EMAIL`.
    """)
    final public String email

    @ConfigOption
    @Description("""
        (Required together with `email`) A Fovus personal access token used to authenticate the
        Fovus CLI for this run, without any interactive login step.

        Must be set via Nextflow's secrets store, eg `personalAccessToken = secrets.FOVUS_PAT`.
    """)
    final public String personalAccessToken

    /** Required by extension point - DO NOT REMOVE */
    FovusAuthConfig() {}

    /**
     * @param config The resolved `fovus.auth` config map, eg {@code session.config.navigate('fovus.auth')}
     * @param strippedEmail The value at {@code fovus.auth.email} in a config re-parsed with Nextflow's
     *  `stripSecrets` mode, ie {@link FovusUtil#stripSecretsRefs}
     * @param strippedPersonalAccessToken Same as {@code strippedEmail}, for {@code fovus.auth.personalAccessToken}
     */
    FovusAuthConfig(Map config, String strippedEmail, String strippedPersonalAccessToken) {
        this.email = config?.get('email') as String
        this.personalAccessToken = config?.get('personalAccessToken') as String

        final hasEmail = email != null && !email.isEmpty()
        final hasToken = personalAccessToken != null && !personalAccessToken.isEmpty()

        if (hasEmail != hasToken) {
            throw new IllegalArgumentException(
                    "[FOVUS] fovus.auth requires both 'email' and 'personalAccessToken' to be set, " +
                    "or neither.")
        }

        if (hasEmail) {
            requireSecretsRef('email', strippedEmail)
            requireSecretsRef('personalAccessToken', strippedPersonalAccessToken)
        }
    }

    private static void requireSecretsRef(String field, String strippedValue) {
        if (strippedValue == null || !(strippedValue ==~ /^secrets\.[A-Za-z_]\w*$/)) {
            throw new IllegalArgumentException(
                    "[FOVUS] fovus.auth.${field} must be set via secrets, eg " +
                    "${field} = secrets.FOVUS_${field == 'email' ? 'EMAIL' : 'PAT'}")
        }
    }

    String getEmail() { email }

    String getPersonalAccessToken() { personalAccessToken }

    /**
     * @return true when both `email` and `personalAccessToken` are set, ie the plugin should inject
     *  credentials into every `fovus` CLI subprocess it spawns
     */
    boolean isConfigured() {
        return email != null && !email.isEmpty() && personalAccessToken != null && !personalAccessToken.isEmpty()
    }

    /**
     * Compute the environment a `fovus` CLI subprocess should carry: {@code baseEnv} unchanged when
     * {@code authConfig} is absent or not fully configured, or {@code baseEnv} plus `FOVUS_EMAIL` /
     * `FOVUS_PAT` otherwise. Pure function, decoupled from process-spawning, so it is directly
     * testable without spawning a process or touching the real environment.
     */
    static Map<String, String> buildEnvironment(FovusAuthConfig authConfig, Map<String, String> baseEnv) {
        final env = new LinkedHashMap<String, String>()
        if (baseEnv) env.putAll(baseEnv)

        if (authConfig != null && authConfig.isConfigured()) {
            env.put('FOVUS_EMAIL', authConfig.email)
            env.put('FOVUS_PAT', authConfig.personalAccessToken)
        }

        return env
    }

    /**
     * Build the `fovus.auth` config for a run, applying the `LOCAL`-vs-`REMOTE` workflow-host scoping
     * decision explicitly rather than reaching into the environment internally: in hosted (`REMOTE`)
     * mode the feature is inactive regardless of what is configured, so the config files are not even
     * re-parsed.
     *
     * @param authConfigMap The resolved `fovus.auth` config map, eg {@code session.config.navigate('fovus.auth')}
     * @param configFiles The config files resolved by Nextflow, eg {@code session.configFiles}
     * @param isHostedMode Whether the pipeline is running in Fovus's own managed (`REMOTE`) execution context
     */
    static FovusAuthConfig resolve(Map authConfigMap, List<Path> configFiles, boolean isHostedMode) {
        if (isHostedMode) {
            return new FovusAuthConfig()
        }

        final stripped = FovusUtil.stripSecretsRefs(configFiles, [EMAIL_PATH, PERSONAL_ACCESS_TOKEN_PATH])
        return new FovusAuthConfig(authConfigMap, stripped[EMAIL_PATH], stripped[PERSONAL_ACCESS_TOKEN_PATH])
    }
}
