package com.aerospike.firefly.process.call.cache;

import com.aerospike.firefly.io.aerospike.admin.ServiceRegistryBase;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Set;

/**
 * Registry for cache management services.
 * <p>
 * Registers the following services:
 * <ul>
 *   <li>{@link CacheServiceSetMode} - Set cache mode (TRANSACTIONAL or GLOBAL)</li>
 *   <li>{@link CacheServiceStatus} - Get current cache mode and usage statistics</li>
 *   <li>{@link CacheServiceReset} - Reset/clear current caches</li>
 * </ul>
 */
public class CacheServiceRegistry extends ServiceRegistryBase {

    public CacheServiceRegistry(final FireflyGraph graph) {
        services = Set.of(
                new CacheServiceSetMode<>(graph),
                new CacheServiceStatus<>(graph),
                new CacheServiceReset<>(graph));

        services.forEach(graph.getServiceRegistry()::registerService);
    }
}
