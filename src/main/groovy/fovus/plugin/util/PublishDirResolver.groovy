package fovus.plugin.util

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortRunException
import nextflow.processor.PublishDir
import nextflow.processor.TaskConfig

import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

/**
 * Resolves each task's {@code publishDir} entries to local filesystem paths
 * and ensures those paths are backed by mount-s3 before Nextflow writes to them.
 *
 * <p><b>Preconditions enforced (AbortRunException thrown if violated):</b>
 * <ul>
 *   <li>publishDir {@code mode} must be {@code copy}.</li>
 *   <li>publishDir path must not be the bare pipeline working directory
 *       ({@code ~/<pipelineId>}); a subdirectory is required.</li>
 * </ul>
 *
 * <p><b>Path resolution rules (applied in order):</b>
 * <ol>
 *   <li>Paths under {@code /fovus-storage} are skipped — already S3-backed.</li>
 *   <li>Paths under {@code ~/<pipelineId>/} are truncated to their first segment
 *       (e.g. {@code ~/pid/process1/sub} → mount at {@code ~/pid/process1}).
 *       Mounting the first segment covers all sub-paths from that process.</li>
 *   <li>All other absolute paths: each path segment is tried in order from the
 *       root down, skipping common Linux root directories. The first segment
 *       that mount-s3 accepts becomes the mount point. If none succeeds,
 *       AbortRunException is thrown.</li>
 * </ol>
 *
 * <p><b>Atomicity:</b> A {@link ConcurrentHashMap} keyed by candidate local path holds
 * {@link CompletableFuture}{@code <Boolean>} sentinels. The value is {@code true} if
 * the path is mounted and {@code false} if mount-s3 rejected it. This ensures
 * exactly one mount attempt per path across concurrent threads, and lets concurrent
 * threads wait for an in-progress attempt before proceeding.
 */
@Slf4j
@CompileStatic
class PublishDirResolver {

    /**
     * Single-segment absolute paths that are standard Linux root directories.
     * These are skipped during absolute-path segment walking because mounting
     * at a root directory would be too broad and is always rejected by mount-s3.
     */
    static final Set<String> COMMON_LINUX_DIRS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        '/tmp', '/mnt', '/home', '/var', '/usr', '/etc', '/opt',
        '/srv', '/proc', '/sys', '/dev', '/run', '/media', '/root'
    )))

    private final MountS3Adapter mountS3Adapter
    private final String bucket
    private final String pipelineId

    /**
     * Registry of mount attempts keyed by candidate local path.
     * {@code true} = mounted; {@code false} = mount-s3 rejected this path.
     * Entries are never removed so concurrent threads always converge on the same result.
     */
    private final ConcurrentHashMap<String, CompletableFuture<Boolean>> mountRegistry = new ConcurrentHashMap<>()

    PublishDirResolver(MountS3Adapter mountS3Adapter, String bucket, String pipelineId) {
        this.mountS3Adapter = mountS3Adapter
        this.bucket = bucket
        this.pipelineId = pipelineId
    }

    /**
     * For each {@code publishDir} entry in the task config, validates the entry
     * and ensures the target path is backed by mount-s3.
     *
     * @throws AbortRunException if mode is not {@code copy}, if the path is the bare
     *                           pipeline directory, or if no mountable path segment is found
     */
    void resolve(TaskConfig config) {
        if (!bucket || !pipelineId || !config) return

        final List<PublishDir> publishDirs = config.getPublishDir()
        if (!publishDirs) return

        final String home = System.getProperty("user.home")
        final String pipelineDir = home + '/' + pipelineId

        for (PublishDir publishDir : publishDirs) {
            if (!publishDir.path) continue

            final String pathStr = publishDir.path.toString()

            if (pathStr.startsWith('/fovus-storage')) continue

            if (publishDir.mode != PublishDir.Mode.COPY) {
                throw new AbortRunException(
                    "[FOVUS] publishDir '${pathStr}' uses mode '${publishDir.mode ?: 'symlink (default)'}'" +
                    " — only 'copy' mode is supported for S3-backed publishDir in Fovus hosted mode"
                )
            }

            if (pathStr == pipelineDir) {
                throw new AbortRunException(
                    "[FOVUS] publishDir '${pathStr}' targets the pipeline working directory directly," +
                    " which cannot be mounted. Use a subdirectory (e.g. '${pipelineDir}/results')."
                )
            }

            if (pathStr.startsWith(pipelineDir + '/')) {
                final String localPath = computeLocalPath(publishDir.path, pipelineId)
                if (!localPath) continue
                final String subpath = computeSubpath(localPath, pipelineId)
                if (!ensureSegmentMounted(localPath, bucket, subpath)) {
                    throw new AbortRunException(
                        "[FOVUS] Failed to mount publishDir at '${localPath}' — mount-s3 rejected the path"
                    )
                }
            } else {
                resolveAbsolutePath(pathStr)
            }
        }
    }

    /**
     * Walks the segments of an absolute path (skipping common Linux root directories)
     * and mounts at the first segment that mount-s3 accepts.
     *
     * @throws AbortRunException if no segment is mountable
     */
    private void resolveAbsolutePath(String pathStr) {
        for (String segment : buildPathSegments(pathStr)) {
            if (COMMON_LINUX_DIRS.contains(segment)) continue

            final String subpath = computeSubpath(segment, pipelineId)
            if (ensureSegmentMounted(segment, bucket, subpath)) return
        }

        throw new AbortRunException(
            "[FOVUS] Could not mount publishDir '${pathStr}' at any path segment" +
            " — mount-s3 rejected all candidate prefixes"
        )
    }

    /**
     * Returns the ordered list of absolute path prefixes for a given path.
     * e.g. {@code /tmp/my_results/sub} → {@code ["/tmp", "/tmp/my_results", "/tmp/my_results/sub"]}
     */
    static List<String> buildPathSegments(String absolutePath) {
        final List<String> segments = new ArrayList<>()
        String current = ''
        for (String part : absolutePath.split('/')) {
            if (part.isEmpty()) continue
            current = current + '/' + part
            segments.add(current)
        }
        return segments
    }

    /**
     * Normalises a path under the pipeline working directory to its first segment,
     * so that all sub-paths from the same process share a single mount point.
     *
     * @return the absolute local path to mount, or {@code null} if the segment is empty
     */
    String computeLocalPath(Path publishDirPath, String pipelineId) {
        if (!publishDirPath) return null

        final String home = System.getProperty("user.home")
        final String pipelinePrefix = home + '/' + pipelineId + '/'
        final String relative = publishDirPath.toString().substring(pipelinePrefix.length())
        if (!relative) return null

        // Take only the first path segment so that all sub-paths under
        // the same process output dir share a single mount point.
        final String firstSegment = relative.contains('/') ? relative.split('/')[0] : relative
        if (!firstSegment) return null

        return pipelinePrefix + firstSegment
    }

    /**
     * Derives the S3 key prefix for a given local path.
     * Convention: {@code pipelines/<pipelineId>/fovus-output/<suffix>}
     * where suffix is the path below the pipeline dir, or the full absolute path
     * with its leading slash stripped.
     */
    String computeSubpath(String localPath, String pipelineId) {
        final String home = System.getProperty("user.home")
        final String pipelinePrefix = home + '/' + pipelineId + '/'

        final String suffix = localPath.startsWith(pipelinePrefix)
            ? localPath.substring(pipelinePrefix.length())
            : (localPath.startsWith('/') ? localPath.substring(1) : localPath)

        return "pipelines/${pipelineId}/fovus-output/${suffix}"
    }

    /**
     * Atomically attempts to mount {@code localPath}, ensuring only one thread
     * performs the actual mount-s3 call even under concurrent access.
     *
     * <p>{@link ConcurrentHashMap#putIfAbsent} is the atomic gate. The winning thread
     * calls mount-s3 and completes the future with the result. All other threads block
     * on {@link CompletableFuture#get} and share the winner's result. Entries remain
     * in the registry on failure so subsequent callers immediately receive {@code false}
     * without retrying.
     *
     * @return true if the path is now mounted, false if mount-s3 rejected it
     */
    private boolean ensureSegmentMounted(String localPath, String bucket, String subpath) {
        final CompletableFuture<Boolean> myFuture = new CompletableFuture<>()
        final CompletableFuture<Boolean> existing = mountRegistry.putIfAbsent(localPath, myFuture)

        if (existing == null) {
            final boolean success = mountS3Adapter.mount(bucket, subpath, localPath)
            myFuture.complete(success)
            return success
        }

        return existing.get()
    }
}
