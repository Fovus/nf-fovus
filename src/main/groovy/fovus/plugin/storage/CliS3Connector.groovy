package fovus.plugin.storage

import fovus.plugin.FovusConfig
import fovus.plugin.s3.FovusS3Client
import fovus.plugin.s3.FovusStorageCredentialsSource
import fovus.plugin.s3.RefreshingStorageCredentials
import groovy.transform.CompileStatic

/** Credentials from the Fovus CLI, fetched once now so a bad sign-in, CLI or pipeline fails at start-up. */
@CompileStatic
class CliS3Connector implements S3Connector {

    private final FovusConfig config

    CliS3Connector(FovusConfig config) {
        this.config = config
    }

    @Override
    FovusS3Client connect(String pipelineId) throws IOException {
        final credentials = new RefreshingStorageCredentials(new FovusStorageCredentialsSource(config, pipelineId))
        credentials.initialize()
        return FovusS3Client.create(credentials)
    }
}
