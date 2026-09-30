package fovus.plugin.storage

import fovus.plugin.nio.FovusPath
import fovus.plugin.nio.PipelinesStorage
import fovus.plugin.s3.FovusS3Client
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException

import java.nio.file.Path

/**
 * Direct mode: the work directory is {@code fovus:///fovus-storage/pipelines}, read and written with the
 * AWS S3 SDK. A {@link FovusPath} prints as {@code /fovus-storage/...}, which is already the path the
 * compute node sees.
 */
@Slf4j
@CompileStatic
class DirectWorkDirStorage implements WorkDirStorage {

    private final FovusPath workDir
    private final S3Connector connector

    DirectWorkDirStorage(FovusPath workDir, S3Connector connector) {
        this.workDir = workDir
        this.connector = connector
    }

    /**
     * Fetch credentials and attach the S3 client. The trace observer does this when the flow is created, so
     * operators evaluated before the first process can use the work directory; the executor's own call when it
     * registers then finds the client attached and does nothing.
     */
    @Override
    void prepare(String pipelineId) {
        final fileSystem = workDir.getFileSystem()
        if (fileSystem.hasS3Client()) {
            log.debug "[FOVUS] Direct mode: Fovus storage is already connected for pipeline ${pipelineId}"
            return
        }
        fileSystem.attachS3Client(connect(pipelineId))
        log.debug "[FOVUS] Direct mode: using Fovus storage for pipeline ${pipelineId} without a mount"
    }

    @Override
    boolean isForeignFile(Path path) {
        return !(path instanceof FovusPath)
    }

    @Override
    Path remotePath(Path path) {
        return Path.of(path.toString())
    }

    /**
     * Fetch credentials, then list the pipeline folder with the read token and write its work folder marker with
     * the write token, so a wrong bucket, region or permission stops the run here rather than at the first task.
     */
    private FovusS3Client connect(String pipelineId) {
        try {
            final client = connector.connect(pipelineId)
            final pipelineKey = PipelinesStorage.keyOf((FovusPath) workDir.resolve(pipelineId))
            client.hasChildren(pipelineKey + '/')
            client.putDirectoryMarker(pipelineKey + '/fovus-work/')
            return client
        }
        catch (IOException e) {
            throw new AbortOperationException("[FOVUS] ${e.message}".toString(), e)
        }
    }
}
