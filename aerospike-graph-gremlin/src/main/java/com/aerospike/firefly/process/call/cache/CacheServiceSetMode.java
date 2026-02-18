package com.aerospike.firefly.process.call.cache;

import com.aerospike.firefly.io.aerospike.CacheManager;
import com.aerospike.firefly.io.aerospike.CacheManager.CacheMode;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;

import java.util.HashMap;
import java.util.Map;

/**
 * Service to set the cache mode.
 * <p>
 * Usage:
 * <pre>
 * g.call("aerospike.graph.admin.cache.set-mode")
 *   .with("mode", "GLOBAL")
 *   .with("cache_weight", "20000000")  // optional cache weight units; default 1M for TRANSACTIONAL, 20M for GLOBAL
 *   .next();
 * </pre>
 */
public class CacheServiceSetMode<I, R> extends CacheServiceBase<I, R> {

    private static final Map<String, String> PARAMS = new HashMap<>();
    static {
        PARAMS.put(MODE, "The cache mode to set. Valid values: 'TRANSACTIONAL' or 'GLOBAL'.");
        PARAMS.put(CACHE_WEIGHT, "Optional. Cache weight units (not raw bytes). Default: 1000000 for TRANSACTIONAL, 20000000 for GLOBAL.");
    }

    public CacheServiceSetMode(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "set-mode";
    }

    @Override
    public Map<String, String> describeParams() {
        return PARAMS;
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tRequired parameter: '" + MODE + "' (TRANSACTIONAL or GLOBAL).\n" +
                        "\tOptional parameter: '" + CACHE_WEIGHT + "' (long weight units, default 1000000 for TRANSACTIONAL, 20000000 for GLOBAL).\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExamples of correct usage:\n" +
                        "\t\tg.call(\"%s\").with(\"" + MODE + "\", \"GLOBAL\").next();\n" +
                        "\t\tg.call(\"%s\").with(\"" + MODE + "\", \"GLOBAL\").with(\"" + CACHE_WEIGHT + "\", \"50000000\").next();",
                getName(), params, getName(), getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        if (params.isEmpty() || params.size() > 2) {
            return false;
        }
        if (!params.containsKey(MODE)) {
            return false;
        }

        final Object modeValue = params.get(MODE);
        if (!(modeValue instanceof String)) {
            return false;
        }

        final String modeStr = ((String) modeValue).toUpperCase();
        if (!modeStr.equals("TRANSACTIONAL") && !modeStr.equals("GLOBAL")) {
            return false;
        }

        if (params.containsKey(CACHE_WEIGHT)) {
            final Object weightValue = params.get(CACHE_WEIGHT);
            if (!(weightValue instanceof String)) {
                return false;
            }
            try {
                final long weight = Long.parseLong((String) weightValue);
                if (weight < 1) {
                    return false;
                }
            } catch (final NumberFormatException e) {
                return false;
            }
        }

        return true;
    }

    @Override
    protected R execute(final Map params) {
        final String modeStr = ((String) params.get(MODE)).toUpperCase();
        final CacheMode newMode = CacheMode.valueOf(modeStr);
        final CacheManager cacheManager = graph.getBaseGraph().cacheManager;

        final CacheMode previousMode = cacheManager.getCacheMode();

        // Determine cache weight to use
        final long weight;
        if (params.containsKey(CACHE_WEIGHT)) {
            weight = Long.parseLong((String) params.get(CACHE_WEIGHT));
        } else {
            weight = CacheManager.getDefaultWeightForMode(newMode);
        }

        // 1. Apply cache mode change via CacheManager
        cacheManager.setCacheMode(graph.getBaseGraph(), newMode, weight);

        // 2. Persist configuration
        final Map<String, Object> configUpdate = new HashMap<>();
        configUpdate.put(ConfigurationHelper.Keys.FIREFLY_READ_THROUGH_CACHE_MODE, cacheManager.getCacheMode().name());
        configUpdate.put(ConfigurationHelper.Keys.FIREFLY_READ_THROUGH_CACHE_WEIGHT, String.valueOf(cacheManager.getCacheWeight()));
        graph.getBaseGraph().updateConfiguration(configUpdate);

        final Map<String, Object> result = new HashMap<>();
        result.put("status", "success");
        result.put("previous_mode", previousMode.name());
        result.put("current_mode", cacheManager.getCacheMode().name());
        result.put("cache_weight", cacheManager.getCacheWeight());

        return (R) result;
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Set cache mode to {}.", getUser(), getName(), params.get(MODE));
    }
}
