package fovus.plugin.util

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.processor.PublishDir
import nextflow.processor.TaskConfig

import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

/**
 * Resolves each task's {@code publishDir} entries to local filesystem paths
 * and ensures those paths are backed by mount-s3 before Nextflow writes to them.
 *
 * <p><b>Path normalisation rules (applied in order):</b>
 * <ol>
 *   <li>Paths already under {@code /fovus-storage} are skipped — already S3-backed.</li>
 *   <li>Paths under {@code ~/&lt;pipelineId&gt;/} are truncated to their first segment
 *       below the pipeline dir (e.g. {@code ~/pid/process1/sub} → {@code ~/pid/process1}).
 *       Mounting the first segment covers all sub-paths from that process without creating
 *       multiple mounts for the same logical output directory.</li>
 *   <li>All other absolute paths are used as-is. A prefix dedup check prevents mounting
 *       a child path when a parent is already mounted.</li>
 * </ol>
 *
 * <p><b>Atomicity:</b> A {@link ConcurrentHashMap} keyed by local path holds
 * {@link CompletableFuture} sentinels so that if multiple threads race to mount
 * the same path, exactly one performs the mount and the rest wait on its result.
 */
@Slf4j
@CompileStatic
class PublishDirResolver {

    private final MountS3Adapter mountS3Adapter
    private final String bucket
    private final String pipelineId

    /**
     * Registry of paths that have been (or are being) mounted.
     * Key: absolute local path. Value: future that completes when the mount succeeds.
     */
    private final ConcurrentHashMap<String, CompletableFuture<Void>> mountRegistry = new ConcurrentHashMap<>()

    PublishDirResolver(MountS3Adapter mountS3Adapter, String bucket, String pipelineId) {
        this.mountS3Adapter = mountS3Adapter
        this.bucket = bucket
        this.pipelineId = pipelineId
    }

    /**
     * For each {@code publishDir} entry in the task config, computes the normalised
     * local path and ensures it is mounted. If a parent path is already being mounted
     * by another thread, waits for that mount to complete before returning.
     */
    void resolve(TaskConfig config) {
        if (!bucket || !pipelineId || !config) return

        final List<PublishDir> publishDirs = config.getPublishDir()
        if (!publishDirs) return

        for (PublishDir publishDir : publishDirs) {
            final String localPath = computeLocalPath(publishDir.path, this.pipelineId)
            if (!localPath) continue

            // If a parent path is already mounted (or being mounted), wait for it to
            // complete rather than skipping immediately — the mount may still be in
            // progress on another thread.
            final CompletableFuture<Void> coveringFuture = getCoveringMountFuture(localPath)
            if (coveringFuture != null) {
                log.trace "[FOVUS] publishDir ${publishDir.path} covered by an existing mount — waiting for completion"
                coveringFuture.get()
                continue
            }

            final String subpath = computeSubpath(localPath, this.pipelineId)
            ensureMounted(localPath, this.bucket, subpath)
        }
    }

    /**
     * Normalises a {@code publishDir} path to the local path that should be mounted.
     *
     * @return the absolute local path to mount, or {@code null} if no mount is needed
     */
    String computeLocalPath(Path publishDirPath, String pipelineId) {
        if (!publishDirPath) return null

        final String pathStr = publishDirPath.toString()

        if (pathStr.startsWith('/fovus-storage')) return null

        final String home = System.getProperty("user.home")
        final String pipelinePrefix = home + '/' + pipelineId + '/'

        if (pathStr.startsWith(pipelinePrefix)) {
            final String relative = pathStr.substring(pipelinePrefix.length())
            if (!relative) return null
            // Take only the first path segment so that all sub-paths under
            // the same process output dir share a single mount point.
            final String firstSegment = relative.contains('/') ? relative.split('/')[0] : relative
            if (!firstSegment) return null
            return pipelinePrefix + firstSegment
        }

        return pathStr
    }

    /**
     * Derives the S3 key prefix to use as the mount source for a given local path.
     * The convention is {@code pipelines/<pipelineId>/fovus-output/<suffix>} where
     * {@code suffix} is the portion of the local path below the pipeline home dir,
     * or the full absolute path with its leading slash stripped.
     */
    String computeSubpath(String localPath, String pipelineId) {
        final String home = System.getProperty("user.home")
        final String pipelinePrefix = home + '/' + pipelineId + '/'

        String suffix
        if (localPath.startsWith(pipelinePrefix)) {
            suffix = localPath.substring(pipelinePrefix.length())
        } else {
            suffix = localPath.startsWith('/') ? localPath.substring(1) : localPath
        }

        return "pipelines/${pipelineId}/fovus-output/${suffix}"
    }

    /**
     * Returns the {@link CompletableFuture} of an existing registry entry whose path
     * covers {@code target} (i.e. is a parent or exact match), or {@code null} if no
     * such entry exists.
     *
     * <p>Returning the future rather than a boolean lets the caller wait for an
     * in-progress parent mount to complete before proceeding, avoiding a race where
     * a task begins writing before the parent mount is ready.
     *
     * <p>The {@code + '/'} guard prevents a false match between sibling paths that share
     * a common string prefix (e.g. {@code /mnt/foo} must not match {@code /mnt/foobar}).
     */
    private CompletableFuture<Void> getCoveringMountFuture(String target) {
        for (Map.Entry<String, CompletableFuture<Void>> entry : mountRegistry.entrySet()) {
            final String key = entry.key
            if (target == key || target.startsWith(key + '/')) return entry.value
        }
        return null
    }

    /**
     * Ensures exactly one mount-s3 call is made for {@code localPath}, even when
     * called concurrently from multiple threads.
     *
     * <p>{@link ConcurrentHashMap#putIfAbsent} is the atomic gate: the thread that
     * inserts its future wins and performs the mount; all other threads block on
     * {@link CompletableFuture#get} until the winner's mount completes. If the
     * mount fails, the registry entry is removed so the next caller can retry.
     */
    private void ensureMounted(String localPath, String bucket, String subpath) {
        final CompletableFuture<Void> myFuture = new CompletableFuture<>()
        final CompletableFuture<Void> existing = mountRegistry.putIfAbsent(localPath, myFuture)

        if (existing == null) {
            try {
                mountS3Adapter.mount(bucket, subpath, localPath)
                myFuture.complete(null)
            } catch (Exception e) {
                mountRegistry.remove(localPath)
                myFuture.completeExceptionally(e)
                throw e
            }
        } else {
            existing.get()
        }
    }
}
