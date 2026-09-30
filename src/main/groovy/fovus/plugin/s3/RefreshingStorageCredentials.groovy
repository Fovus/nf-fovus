package fovus.plugin.s3

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock

/**
 * Keeps the current direct-mode storage credentials in memory and fetches new ones before they expire.
 *
 * Fresh credentials are returned as they are. Within {@link #PREFETCH_BEFORE_EXPIRY} of expiry a caller
 * refreshes them, falling back to the current ones if that fails; within {@link #STALE_BEFORE_EXPIRY}
 * a caller must get fresh ones or fail. Only one thread fetches at a time; the others wait and reuse its
 * result.
 */
@Slf4j
@CompileStatic
class RefreshingStorageCredentials {

    static final Duration PREFETCH_BEFORE_EXPIRY = Duration.ofMinutes(10)
    static final Duration STALE_BEFORE_EXPIRY = Duration.ofMinutes(2)
    static final Duration FORCED_REFRESH_DEBOUNCE = Duration.ofSeconds(30)

    private final CredentialsFetcher fetcher
    private final Clock clock
    private final ReentrantLock refreshLock = new ReentrantLock()
    private volatile StorageCredentials current
    private volatile Instant lastFetchedAt

    RefreshingStorageCredentials(CredentialsFetcher fetcher) {
        this(fetcher, Clock.systemUTC())
    }

    RefreshingStorageCredentials(CredentialsFetcher fetcher, Clock clock) {
        this.fetcher = fetcher
        this.clock = clock
    }

    /** Fetch the first credentials now, so a bad sign-in, CLI or pipeline stops the run at start-up. */
    StorageCredentials initialize() throws StorageCredentialsException {
        return refresh(null)
    }

    StorageCredentials get() throws StorageCredentialsException {
        final snapshot = current
        if (snapshot == null) return refresh(null)

        final now = clock.instant()
        final expiration = snapshot.expiration
        if (now.isBefore(expiration.minus(PREFETCH_BEFORE_EXPIRY))) return snapshot

        if (now.isBefore(expiration.minus(STALE_BEFORE_EXPIRY))) {
            try {
                return refresh(snapshot)
            }
            catch (StorageCredentialsException e) {
                log.warn "[FOVUS] Could not refresh Fovus storage credentials; the current ones stay in use until they are close to expiry: ${e.message}"
                return snapshot
            }
        }

        try {
            return refresh(snapshot)
        }
        catch (StorageCredentialsException e) {
            throw new StorageCredentialsException("Unable to refresh Fovus storage credentials: ${e.message}".toString(), e.retryable)
        }
    }

    /** S3 rejected a token as expired: fetch new keys, unless a fetch finished moments ago. */
    StorageCredentials forceRefresh() throws StorageCredentialsException {
        final snapshot = current
        final fetchedAt = lastFetchedAt
        if (snapshot != null && fetchedAt != null && clock.instant().isBefore(fetchedAt.plus(FORCED_REFRESH_DEBOUNCE))) {
            return snapshot
        }
        return refresh(snapshot)
    }

    AwsCredentialsProvider readProvider() {
        return { -> resolve().read.toAwsCredentials() } as AwsCredentialsProvider
    }

    AwsCredentialsProvider writeProvider() {
        return { -> resolve().write.toAwsCredentials() } as AwsCredentialsProvider
    }

    private StorageCredentials resolve() {
        try {
            return get()
        }
        catch (StorageCredentialsException e) {
            throw new UncheckedStorageCredentialsException(e)
        }
    }

    private StorageCredentials refresh(StorageCredentials seen) throws StorageCredentialsException {
        refreshLock.lock()
        try {
            // Another thread refreshed while this one waited for the lock
            if (!current.is(seen)) return current

            final fresh = fetcher.fetch()
            final previous = current
            if (previous != null && (fresh.bucket != previous.bucket || fresh.region != previous.region
                    || fresh.prefix != previous.prefix)) {
                throw new StorageCredentialsException(
                        'Unexpected response from Fovus CLI (storage location changed during the run)', false)
            }
            current = fresh
            lastFetchedAt = clock.instant()
            log.debug "[FOVUS] Fetched Fovus storage credentials, expires at ${fresh.expiration}"
            return fresh
        }
        finally {
            refreshLock.unlock()
        }
    }
}
