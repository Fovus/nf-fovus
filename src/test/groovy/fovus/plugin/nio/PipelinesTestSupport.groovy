package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client

class PipelinesTestSupport {

    static final URI AREA = URI.create('fovus:///fovus-storage/pipelines')

    /** A fresh provider's pipelines/ file system, with an S3 client attached when one is given. */
    static FovusFileSystem fileSystem(FovusS3Client client = null) {
        final fs = (FovusFileSystem) new FovusFileSystemProvider().newFileSystem(AREA, [:])
        if (client != null) fs.attachS3Client(client)
        return fs
    }
}
