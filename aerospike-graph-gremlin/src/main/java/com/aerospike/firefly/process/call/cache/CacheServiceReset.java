package com.aerospike.firefly.process.call.cache;

import com.aerospike.firefly.io.aerospike.CacheManager;
import com.aerospike.firefly.io.aerospike.CacheManager.CacheMode;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

/**
 * Service to reset/clear current caches.
 * <p>
 * In TRANSACTIONAL mode, this clears thread-local caches.
 * In GLOBAL mode, this clears and reinitializes the global caches.
 * <p>
 * Usage:
 * <pre>
 * g.call("aerospike.graph.admin.cache.reset").next();
 * </pre>
 */
public class CacheServiceReset<I, R> extends CacheServiceBase<I, R> {

    public CacheServiceReset(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "reset";
    }

    @Override
    public Map<String, String> describeParams() {
        return new HashMap<>();
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tNo parameters are required.\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExample of correct usage:\n" +
                        "\t\tg.call(\"%s\").next();",
                getName(), params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        return params.isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        final CacheManager cacheManager = graph.getBaseGraph().cacheManager;
        final CacheMode currentMode = cacheManager.getCacheMode();
        final long cacheWeight = cacheManager.getCacheWeight();

        // Get stats before reset
        final long previousEntryCount = cacheManager.getTotalEstimatedEntryCount();
        final long previousWeightedSize = cacheManager.getTotalWeightedSize();

        // Reset by switching to same mode (this clears and reinitializes caches)
        if (currentMode == CacheMode.GLOBAL) {
            // For GLOBAL mode, we need to clear global caches and reinit
            cacheManager.setCacheMode(graph.getBaseGraph(), CacheMode.TRANSACTIONAL);
            cacheManager.setCacheMode(graph.getBaseGraph(), CacheMode.GLOBAL, cacheWeight);
        } else {
            // For TRANSACTIONAL mode, reset works as usual
            cacheManager.resetCache(graph.getBaseGraph());
        }

        final Map<String, Object> result = new HashMap<>();
        result.put("status", "success");
        result.put("mode", currentMode.name());
        result.put("previous_entry_count", previousEntryCount);
        result.put("previous_weighted_size", previousWeightedSize);
        result.put("current_entry_count", cacheManager.getTotalEstimatedEntryCount());
        result.put("current_weighted_size", cacheManager.getTotalWeightedSize());

        return (R) result;
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Reset cache.", getUser(), getName());
    }
}
