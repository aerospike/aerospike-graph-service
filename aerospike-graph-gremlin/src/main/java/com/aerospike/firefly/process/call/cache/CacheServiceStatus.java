/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.process.call.cache;

import com.aerospike.firefly.io.aerospike.CacheManager;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

/**
 * Service to get the current cache mode and usage statistics.
 * <p>
 * Usage:
 * <pre>
 * g.call("aerospike.graph.admin.cache.status").next();
 * </pre>
 */
public class CacheServiceStatus<I, R> extends CacheServiceBase<I, R> {

    public CacheServiceStatus(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "status";
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

        final Map<String, Object> result = new HashMap<>();
        result.put("mode", cacheManager.getCacheMode().name());
        result.put("cache_weight", cacheManager.getCacheWeight());
        result.put("estimated_entry_count", cacheManager.getTotalEstimatedEntryCount());
        result.put("weighted_size", cacheManager.getTotalWeightedSize());
        result.put("estimated_memory_bytes", cacheManager.getTotalEstimatedMemoryUsageBytes());
        result.put("estimated_memory_formatted", cacheManager.getTotalEstimatedMemoryUsageFormatted());
        result.put("hit_count", cacheManager.getTotalHitCount());
        result.put("miss_count", cacheManager.getTotalMissCount());

        // Supernode edge ID cache stats (only available in GLOBAL mode)
        result.put("supernode_edge_cache_entries", cacheManager.getSupernodeEdgeCacheEntryCount());
        result.put("supernode_edge_cache_edge_ids", cacheManager.getSupernodeEdgeCacheTotalEdgeIds());
        result.put("supernode_edge_cache_memory_bytes", cacheManager.getSupernodeEdgeCacheEstimatedMemoryBytes());
        result.put("supernode_edge_cache_hits", cacheManager.getSupernodeEdgeCacheHitCount());
        result.put("supernode_edge_cache_misses", cacheManager.getSupernodeEdgeCacheMissCount());

        return (R) result;
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get cache status.", getUser(), getName());
    }

    @Override
    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.READ;
    }
}
