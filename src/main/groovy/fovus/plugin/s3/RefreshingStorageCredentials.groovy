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
 * Fresh credentials are returned as they are. Within {@link #PREFETCH_BEFORE_EXPIRY} of expiry one caller
 * refreshes them while every other caller carries on with the current ones, and a failed attempt is not
 * repeated for {@link #FAILED_PREFETCH_COOLDOWN}. Within {@link #STALE_BEFORE_EXPIRY} (or with no credentials
 * yet) callers must get fresh ones or fail: only one thread fetches at a time, and the callers that were
 * waiting for it share its result or its failure instead of running the CLI again.
 */
@Slf4j
@CompileStatic
class RefreshingStorageCredentials {

    static final Duration PREFETCH_BEFORE_EXPIRY = Duration.ofMinutes(10)
    static final Duration STALE_BEFORE_EXPIRY = Duration.ofMinutes(2)
    static final Duration FORCED_REFRESH_DEBOUNCE = Duration.ofSeconds(30)
    static final Duration FAILED_PREFETCH_COOLDOWN = Duration.ofSeconds(30)

    /** A failed fetch, kept so that callers that waited for it do not each run the CLI again. */
    private static final class FetchFailure {
        final StorageCredentialsException exception
        final Instant at

        FetchFailure(StorageCredentialsException exception, Instant at) {
            this.exception = exception
            this.at = at
        }
    }

    private final CredentialsFetcher fetcher
    private final Clock clock
    private final ReentrantLock refreshLock = new ReentrantLock()
    private volatile StorageCredentials current
    private volatile Instant lastFetchedAt
    private volatile FetchFailure lastFailure

    RefreshingStorageCredentials(CredentialsFetcher fetcher) {
        this(fetcher, Clock.systemUTC())
    }

    RefreshingStorageCredentials(CredentialsFetcher fetcher, Clock clock) {
        this.fetcher = fetcher
        this.clock = clock
    }

    /** Fetch the first credentials now, so a bad sign-in, CLI or pipeline stops the run at start-up. */
    StorageCredentials initialize() throws StorageCredentialsException {
        return refreshBlocking(null)
    }

    StorageCredentials get() throws StorageCredentialsException {
        final snapshot = current
        if (snapshot == null) return refreshBlocking(null)

        final now = clock.instant()
        final expiration = snapshot.expiration
        if (now.isBefore(expiration.minus(PREFETCH_BEFORE_EXPIRY))) return snapshot

        if (now.isBefore(expiration.minus(STALE_BEFORE_EXPIRY))) return prefetch(snapshot)

        try {
            return refreshBlocking(snapshot)
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
        return refreshBlocking(snapshot)
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

    /**
     * Refresh early without holding anyone up: the current credentials are still valid, so a caller that finds
     * a refresh in progress, or a failed one from the last {@link #FAILED_PREFETCH_COOLDOWN}, just uses them.
     */
    private StorageCredentials prefetch(StorageCredentials snapshot) {
        if (recentlyFailed()) return snapshot
        if (!refreshLock.tryLock()) return snapshot
        try {
            // Another thread refreshed or failed between the checks above and taking the lock
            if (!current.is(snapshot)) return current
            if (recentlyFailed()) return snapshot

            try {
                return fetchAndStore()
            }
            catch (StorageCredentialsException e) {
                log.warn "[FOVUS] Could not refresh Fovus storage credentials; the current ones stay in use until they are close to expiry: ${e.message}"
                return snapshot
            }
        }
        finally {
            refreshLock.unlock()
        }
    }

    /**
     * Refresh when the caller cannot proceed without new credentials. One thread fetches; a thread that waited
     * for it reuses its credentials, or rethrows its failure if the fetch failed while this one was waiting.
     */
    private StorageCredentials refreshBlocking(StorageCredentials seen) throws StorageCredentialsException {
        final waitStarted = clock.instant()
        try {
            refreshLock.lockInterruptibly()
        }
        catch (InterruptedException ignored) {
            Thread.currentThread().interrupt()
            throw new StorageCredentialsException('Interrupted while waiting for Fovus storage credentials', false)
        }
        try {
            // Another thread refreshed while this one waited for the lock
            if (!current.is(seen)) return current

            final failure = lastFailure
            if (failure != null && !failure.at.isBefore(waitStarted)) throw failure.exception

            return fetchAndStore()
        }
        finally {
            refreshLock.unlock()
        }
    }

    private boolean recentlyFailed() {
        final failure = lastFailure
        return failure != null && clock.instant().isBefore(failure.at.plus(FAILED_PREFETCH_COOLDOWN))
    }

    /** Fetch and store new credentials. The caller must hold {@code refreshLock}. */
    private StorageCredentials fetchAndStore() throws StorageCredentialsException {
        try {
            final fresh = fetcher.fetch()
            final previous = current
            if (previous != null && (fresh.bucket != previous.bucket || fresh.region != previous.region
                    || fresh.prefix != previous.prefix)) {
                throw new StorageCredentialsException(
                        'Unexpected response from Fovus CLI (storage location changed during the run)', false)
            }
            current = fresh
            lastFetchedAt = clock.instant()
            lastFailure = null
            log.debug "[FOVUS] Fetched Fovus storage credentials, expires at ${fresh.expiration}"
            return fresh
        }
        catch (StorageCredentialsException e) {
            lastFailure = new FetchFailure(e, clock.instant())
            throw e
        }
    }
}
