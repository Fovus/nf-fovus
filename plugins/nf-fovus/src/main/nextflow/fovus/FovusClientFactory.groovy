package nextflow.fovus

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.AnonymousAWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.auth.SystemPropertiesCredentialsProvider
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import nextflow.SysEnv
import nextflow.exception.AbortOperationException
import nextflow.fovus.util.S3CredentialsProvider

class FovusClientFactory {

    /**
     * The AWS access key credentials (optional)
     */
    private String accessKey

    /**
     * The AWS secret key credentials (optional)
     */
    private String secretKey

    /**
     * The AWS region eg. {@code eu-west-1}. If it's not specified the current region is retrieved from
     * the EC2 instance metadata
     */
    private String region

    private String profile

    FovusClientFactory() {

    }

    AmazonS3 getS3Client(ClientConfiguration clientConfig=null, boolean global=false) {
        final builder = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withForceGlobalBucketAccessEnabled(global)

//        final endpoint = config.s3Config.endpoint
//        if( endpoint )
//            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
//        else
        builder.withRegion(region)

        final creds = new BasicAWSCredentials(accessKey, secretKey)
        final credentials = new S3CredentialsProvider(new AWSStaticCredentialsProvider(creds));


        builder.withCredentials(credentials)

        if( clientConfig )
            builder.withClientConfiguration(clientConfig)

        return builder.build()
    }


}