package fovus.plugin.nio

import fovus.plugin.s3.FovusS3Client

class StorageTestSupport {

    static final URI AREA = URI.create('fovus:///fovus-storage/pipelines')

    /** A fresh provider's pipelines/ file system, with an S3 client attached to the provider when one is given. */
    static FovusFileSystem fileSystem(FovusS3Client client = null) {
        final provider = new FovusFileSystemProvider()
        if (client != null) provider.attachS3Client(client)
        return (FovusFileSystem) provider.newFileSystem(AREA, [:])
    }
}
