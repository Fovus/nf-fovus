package fovus.plugin.s3

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import spock.lang.Specification

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class RefreshingStorageCredentialsTest extends Specification {

    static final Instant NOW = Instant.parse('2026-09-30T12:00:00Z')

    static class MutableClock extends Clock {
        volatile Instant now

        MutableClock(Instant now) { this.now = now }

        @Override ZoneId getZone() { ZoneOffset.UTC }

        @Override Clock withZone(ZoneId zone) { this }

        @Override Instant instant() { now }
    }

    /** Hands out the queued responses in order, repeating the last; a queued exception is thrown. */
    static class QueueFetcher implements CredentialsFetcher {
        final List<Object> responses
        final long delayMillis
        final AtomicInteger calls = new AtomicInteger()

        QueueFetcher(List<Object> responses, long delayMillis = 0) {
            this.responses = responses
            this.delayMillis = delayMillis
        }

        @Override
        StorageCredentials fetch() throws StorageCredentialsException {
            final index = Math.min(calls.getAndIncrement(), responses.size() - 1)
            if (delayMillis) Thread.sleep(delayMillis)
            final response = responses[index]
            if (response instanceof StorageCredentialsException) throw (StorageCredentialsException) response
            return (StorageCredentials) response
        }
    }

    static StorageCredentials credentials(Instant expiration, String bucket = 'bucket') {
        final keys = new SessionKeys('key-id', 'secret', 'token', expiration)
        return new StorageCredentials(bucket, 'us-east-2', 'pipelines/p-1-user/', keys, keys)
    }

    MutableClock clock = new MutableClock(NOW)

    def 'get should reuse credentials until the prefetch window'() {
        given:
        def first = credentials(NOW.plus(Duration.ofHours(1)))
        def fetcher = new QueueFetcher([first])
        def cache = new RefreshingStorageCredentials(fetcher, clock)

        when:
        cache.initialize()
        clock.now = NOW.plus(Duration.ofMinutes(49))
        def result = cache.get()

        then:
        result.is(first)
        fetcher.calls.get() == 1
    }

    def 'get should refresh inside the ten-minute prefetch window'() {
        given:
        def second = credentials(NOW.plus(Duration.ofHours(2)))
        def fetcher = new QueueFetcher([credentials(NOW.plus(Duration.ofHours(1))), second])
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        cache.initialize()

        when:
        clock.now = NOW.plus(Duration.ofMinutes(51))

        then:
        cache.get().is(second)
        fetcher.calls.get() == 2
    }

    def 'a failed prefetch should keep the current credentials'() {
        given:
        def first = credentials(NOW.plus(Duration.ofHours(1)))
        def fetcher = new QueueFetcher([first, new StorageCredentialsException('CLI down', true)])
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        cache.initialize()

        when:
        clock.now = NOW.plus(Duration.ofMinutes(55))

        then:
        cache.get().is(first)
    }

    def 'a failed refresh in the last two minutes should fail the caller'() {
        given:
        def fetcher = new QueueFetcher([credentials(NOW.plus(Duration.ofHours(1))),
                                        new StorageCredentialsException('CLI down', true)])
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        cache.initialize()

        when:
        clock.now = NOW.plus(Duration.ofMinutes(59))
        cache.get()

        then:
        def e = thrown(StorageCredentialsException)
        e.message == 'Unable to refresh Fovus storage credentials: CLI down'
        e.retryable
    }

    def 'concurrent callers should share one fetch'() {
        given:
        def fetcher = new QueueFetcher([credentials(NOW.plus(Duration.ofHours(1)))], 100)
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        def pool = Executors.newFixedThreadPool(50)

        when:
        def futures = (1..50).collect { pool.submit({ cache.get() } as Callable<StorageCredentials>) }
        futures*.get()

        then:
        fetcher.calls.get() == 1

        cleanup:
        pool.shutdownNow()
    }

    def 'a refresh that moves the storage location should be rejected'() {
        given:
        def fetcher = new QueueFetcher([credentials(NOW.plus(Duration.ofHours(1))),
                                        credentials(NOW.plus(Duration.ofHours(2)), 'other-bucket')])
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        cache.initialize()

        when:
        clock.now = NOW.plus(Duration.ofMinutes(59))
        cache.get()

        then:
        def e = thrown(StorageCredentialsException)
        e.message.contains('storage location changed')
        !e.retryable
    }

    def 'forceRefresh should fetch again unless a fetch just happened'() {
        given:
        def fetcher = new QueueFetcher([credentials(NOW.plus(Duration.ofHours(1))),
                                        credentials(NOW.plus(Duration.ofHours(2)))])
        def cache = new RefreshingStorageCredentials(fetcher, clock)
        cache.initialize()

        when: 'S3 rejects a token right after a fetch'
        cache.forceRefresh()

        then: 'the fetch that just happened is reused'
        fetcher.calls.get() == 1

        when: 'S3 rejects a token later'
        clock.now = NOW.plus(Duration.ofMinutes(5))
        cache.forceRefresh()

        then:
        fetcher.calls.get() == 2
    }

    def 'the providers should hand the SDK the read and write keys'() {
        given:
        def expiry = NOW.plus(Duration.ofHours(1))
        def stored = new StorageCredentials('b', 'us-east-2', 'pipelines/p-1-user/',
                                            new SessionKeys('read-id', 'read-secret', 'read-token', expiry),
                                            new SessionKeys('write-id', 'write-secret', 'write-token', expiry))
        def cache = new RefreshingStorageCredentials(new QueueFetcher([stored]), clock)

        expect:
        (cache.readProvider().resolveCredentials() as AwsSessionCredentials).accessKeyId() == 'read-id'
        (cache.writeProvider().resolveCredentials() as AwsSessionCredentials).sessionToken() == 'write-token'
    }

    def 'a provider should surface a credentials failure as an unchecked exception'() {
        given:
        def failure = new StorageCredentialsException('not signed in', false)
        def cache = new RefreshingStorageCredentials(new QueueFetcher([failure]), clock)

        when:
        cache.readProvider().resolveCredentials()

        then:
        def e = thrown(UncheckedStorageCredentialsException)
        e.cause.is(failure)
    }
}
