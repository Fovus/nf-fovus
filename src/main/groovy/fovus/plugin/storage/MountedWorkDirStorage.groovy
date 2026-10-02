package fovus.plugin.storage

import groovy.transform.CompileStatic

import java.nio.file.FileSystems
import java.nio.file.Path

/** Mount mode, unchanged: Fovus storage is FUSE-mounted at the parent of {@code workDir}. */
@CompileStatic
class MountedWorkDirStorage implements WorkDirStorage {

    private static final String REMOTE_MOUNT_POINT = '/fovus-storage'

    private final FovusStorageClient storageClient
    private final Path mountDir

    MountedWorkDirStorage(FovusStorageClient storageClient, Path sessionWorkDir) {
        this.storageClient = storageClient
        this.mountDir = sessionWorkDir.parent
    }

    @Override
    void prepare(String pipelineId) {
        storageClient.validateOrMountFovusStorage(mountDir)
    }

    @Override
    boolean isForeignFile(Path path) {
        if (path.fileSystem != FileSystems.default) return true
        return !path.toAbsolutePath().startsWith(mountDir.toAbsolutePath())
    }

    @Override
    Path remotePath(Path file) {
        // Replace the mount point part with the REMOTE_MOUNT_POINT
        return Path.of(REMOTE_MOUNT_POINT, file.toString().replace(mountDir.toString(), ''))
    }
}
