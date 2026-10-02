package fovus.plugin.s3

import software.amazon.awssdk.core.interceptor.Context
import software.amazon.awssdk.core.interceptor.ExecutionAttributes
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** Counts S3 requests by type, e.g. {@code count('HeadObjectRequest')}. */
class CountingInterceptor implements ExecutionInterceptor {

    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>()

    @Override
    void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
        counts.computeIfAbsent(context.request().getClass().simpleName) { new AtomicInteger() }.incrementAndGet()
    }

    int count(String requestType) {
        return counts.get(requestType)?.get() ?: 0
    }

    void reset() {
        counts.clear()
    }
}
