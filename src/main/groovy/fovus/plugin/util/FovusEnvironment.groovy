package fovus.plugin.util

import groovy.transform.CompileStatic

/**
 * Central accessor for Fovus-specific environment variables.
 * All env var reads in the plugin should go through this class
 * rather than calling {@code System.getenv} inline.
 */
@CompileStatic
class FovusEnvironment {

    /** Returns true when the pipeline is executing on a Fovus-hosted headnode. */
    static boolean isHostedMode() {
        return "REMOTE".equalsIgnoreCase(System.getenv("WORKFLOW_HOST"))
    }

    /** Pipeline ID assigned by the Fovus backend; set only in hosted mode. */
    static String getPipelineId() {
        return System.getenv("PIPELINE_ID")
    }

    /** S3 bucket provisioned for the Fovus user; set only in hosted mode. */
    static String getFovusUserBucket() {
        return System.getenv("FovusUserBucket")
    }
}
