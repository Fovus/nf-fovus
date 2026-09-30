package fovus.plugin.s3

import software.amazon.awssdk.core.interceptor.Context
import software.amazon.awssdk.core.interceptor.ExecutionAttributes
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/** The user's own AWS configuration must not send Fovus-signed requests anywhere but Fovus storage. */
class FovusS3ClientEndpointTest extends Specification {

    static final String PREFIX = 'pipelines/p-1-user/'
    static final String FOVUS_HOST = 'bucket.s3.us-east-2.amazonaws.com'

    private final Map<String, String> savedProperties = [:]

    private void setSystemProperty(String name, String value) {
        if (!savedProperties.containsKey(name)) savedProperties[name] = System.getProperty(name)
        System.setProperty(name, value)
    }

    def cleanup() {
        savedProperties.each { String name, String value -> value == null ? System.clearProperty(name) : System.setProperty(name, value) }
    }

    /** Records the host of every request just before it would be sent, and stops it there. */
    static class HostRecorder implements ExecutionInterceptor {
        final List<String> hosts = new CopyOnWriteArrayList<>()

        @Override
        void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            hosts.add(context.httpRequest().host())
            throw new UnsupportedOperationException('stopped before any network I/O')
        }
    }

    private static FovusS3Client realClient(HostRecorder recorder) {
        final keys = new SessionKeys('AKIA-TEST', 'secret-test', 'token-test', Instant.now().plusSeconds(3600))
        final fetcher = { -> new StorageCredentials('bucket', 'us-east-2', PREFIX, keys, keys) } as CredentialsFetcher
        return FovusS3Client.createWithInterceptors(new RefreshingStorageCredentials(fetcher), [recorder as ExecutionInterceptor])
    }

    @Unroll
    def 'a real client should reach Fovus storage in its region whatever #setting says'() {
        given:
        settings.each { String name, String value -> setSystemProperty(name, value) }
        def recorder = new HostRecorder()
        def client = realClient(recorder)

        when: 'a read, with the download token'
        client.head(PREFIX + 'x')

        then:
        thrown(IOException)
        !recorder.hosts.isEmpty()
        recorder.hosts.every { it == FOVUS_HOST }

        when: 'a write, with the upload token'
        recorder.hosts.clear()
        client.putObject(PREFIX + 'x', new byte[0])

        then:
        thrown(IOException)
        !recorder.hosts.isEmpty()
        recorder.hosts.every { it == FOVUS_HOST }

        where:
        setting                    | settings
        'aws.endpointUrl(S3)'      | ['aws.endpointUrl': 'http://user-endpoint.example:9000', 'aws.endpointUrlS3': 'http://user-s3.example:9000']
        'aws.endpointUrl'          | ['aws.endpointUrl': 'http://user-endpoint.example:9000']
        'aws.endpointUrlS3'        | ['aws.endpointUrlS3': 'http://user-s3.example:9000']
        // the SDK refuses FIPS or dual-stack next to an explicit endpoint, so these must not reach the client either
        'aws.useFipsEndpoint'      | ['aws.useFipsEndpoint': 'true']
        'aws.useDualstackEndpoint' | ['aws.useDualstackEndpoint': 'true']
    }

    def 'a real client should read no AWS profile or config file'() {
        given:
        def client = realClient(new HostRecorder())

        expect:
        [client.@reader, client.@writer].every { sdk ->
            sdk.serviceClientConfiguration().overrideConfiguration().defaultProfileFile().map { it.profiles().isEmpty() }.orElse(false)
        }
    }
}
