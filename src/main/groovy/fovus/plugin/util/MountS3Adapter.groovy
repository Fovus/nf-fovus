package fovus.plugin.util

import fovus.plugin.FovusUtil
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Thin wrapper around the {@code mount-s3} (Mountpoint for Amazon S3) CLI.
 * All mount-s3 shell interactions are isolated here so the rest of the plugin
 * never constructs raw shell commands for S3 mounting.
 */
@Slf4j
@CompileStatic
class MountS3Adapter {

    /**
     * Mounts a prefix of an S3 bucket at a local filesystem path using mount-s3.
     *
     * <p>The mount makes {@code s3://<bucket>/<subpath>/} appear as a local directory
     * at {@code localPath}, giving effectively unlimited storage at that path.
     *
     * @param bucket    S3 bucket name (value of {@code FovusUserBucket} env var)
     * @param subpath   Key prefix inside the bucket, e.g. {@code pipelines/<id>/fovus-output/outputs}
     * @param localPath Absolute local path where the bucket prefix will be mounted
     * @return true if the mount succeeded, false if mount-s3 rejected the path
     */
    boolean mount(String bucket, String subpath, String localPath) {
        log.trace "[FOVUS] Mounting ${subpath} at ${localPath}"

        new File(localPath).mkdirs()

        // Trailing slash on the prefix is required by mount-s3 to scope the
        // mount to exactly that prefix and not to adjacent keys at the same level.
        final result = FovusUtil.executeCommand([
            'mount-s3', bucket, localPath, '--prefix', subpath + '/'
        ])

        if (result.exitCode == 0) {
            log.trace "[FOVUS] Successfully mounted ${subpath} at ${localPath}"
            return true
        }

        log.trace "[FOVUS] Could not mount at ${localPath} (exit ${result.exitCode}): ${result.error}"
        return false
    }
}
