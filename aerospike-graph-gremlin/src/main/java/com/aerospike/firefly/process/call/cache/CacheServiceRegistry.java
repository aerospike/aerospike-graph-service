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
