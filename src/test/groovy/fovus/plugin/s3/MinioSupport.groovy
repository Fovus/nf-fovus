package fovus.plugin.s3

import org.testcontainers.containers.MinIOContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest

/** A MinIO container standing in for the user's Fovus bucket in the direct-mode S3 tests. */
class MinioSupport {

    // Override with FOVUS_MINIO_IMAGE; the default tag was removed from Docker Hub
    static final String IMAGE = 'minio/minio:RELEASE.2023-09-04T19-57-37Z'
    static final String BUCKET = 'fovus-test-bucket'
    static final String PREFIX = 'pipelines/p-1-user/'

    static MinIOContainer start() {
        final override = System.getenv('FOVUS_MINIO_IMAGE')
        final image = override?.trim() ? override.trim() : IMAGE
        final minio = new MinIOContainer(DockerImageName.parse(image).asCompatibleSubstituteFor('minio/minio'))
        minio.start()
        s3Client(minio).withCloseable { it.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build()) }
        return minio
    }

    static S3Client s3Client(MinIOContainer minio, ExecutionInterceptor... interceptors) {
        return S3Client.builder()
                .endpointOverride(URI.create(minio.getS3URL()))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(minio.getUserName(), minio.getPassword())))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .executionInterceptors(interceptors as List<ExecutionInterceptor>)
                        .build())
                .build()
    }

    static FovusS3Client fovusClient(S3Client s3, int partSize = FovusS3Client.MIN_PART_SIZE,
                                     int listPageSize = FovusS3Client.DEFAULT_LIST_PAGE_SIZE) {
        return new FovusS3Client(s3, s3, BUCKET, PREFIX, null, partSize, listPageSize)
    }
}
