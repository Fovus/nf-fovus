package fovus.plugin.storage

import fovus.plugin.s3.FovusS3Client
import groovy.transform.CompileStatic

/** Builds the S3 client for one pipeline's work directory. */
@CompileStatic
interface S3Connector {
    FovusS3Client connect(String pipelineId) throws IOException
}
