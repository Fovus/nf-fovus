package fovus.plugin.storage

import fovus.plugin.FovusConfig
import fovus.plugin.nio.FovusPath
import groovy.transform.CompileStatic
import nextflow.exception.AbortOperationException
import nextflow.extension.FilesEx

import java.nio.file.FileSystems
import java.nio.file.Path

/** Picks mount or direct mode from Nextflow's {@code workDir}, and rejects anything else. */
@CompileStatic
class WorkDirStorageFactory {

    static final String DIRECT_MODE_WORK_DIR = 'fovus:///fovus-storage/pipelines'

    static WorkDirStorage create(Path workDir, boolean isHostedMode, FovusConfig config) {
        if (workDir instanceof FovusPath) {
            return direct((FovusPath) workDir, isHostedMode, config)
        }
        if (workDir.fileSystem == FileSystems.default) {
            if (!workDir.endsWith('pipelines')) {
                throw new AbortOperationException(
                        "[FOVUS] Working directory must end with pipelines. Current work directory: ${workDir}".toString())
            }
            return new MountedWorkDirStorage(new FovusStorageClient(config), workDir)
        }
        throw new AbortOperationException(
                "[FOVUS] The Fovus executor needs workDir to be a Fovus storage mount (…/pipelines) or ${DIRECT_MODE_WORK_DIR}. Current work directory: ${FilesEx.toUriString(workDir)}".toString())
    }

    private static WorkDirStorage direct(FovusPath workDir, boolean isHostedMode, FovusConfig config) {
        if (isHostedMode) {
            throw new AbortOperationException(
                    '[FOVUS] Direct mode (a fovus:// workDir) is only for pipelines launched on your own machine; ' +
                    'Fovus-hosted runs use the Fovus storage mount')
        }
        if (workDir.fileType != FovusPath.PIPELINES || !workDir.isAreaRoot()) {
            throw new AbortOperationException(
                    "[FOVUS] In direct mode, workDir must be ${DIRECT_MODE_WORK_DIR}. Current work directory: ${workDir.toUri()}".toString())
        }
        return new DirectWorkDirStorage(workDir, new CliS3Connector(config))
    }
}
