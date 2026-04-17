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

package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.io.aerospike.admin.ServiceRegistryBase;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Set;

public class BulkLoaderServiceRegistry extends ServiceRegistryBase {

    public BulkLoaderServiceRegistry(final FireflyGraph graph) {
        // Check if com.aerospike.firefly.bulkloader.SparkBulkLoaderMain exists. Only load if it does.
        if (!bulkLoaderExists()) {
            services = Set.of();
            return;
        }
        services = Set.of(
                new BulkLoaderServiceLoad<>(graph),
                new BulkLoaderServiceErrors<>(graph),
                new BulkLoaderServiceCountErrors<>(graph),
                new BulkLoaderServiceLoadDeprecated<>(graph),
                new BulkLoaderServiceErrorsDeprecated<>(graph),
                new BulkLoaderServiceCountErrorsDeprecated<>(graph),
                new BulkLoaderServiceStatus<>(graph));

        services.forEach(graph.getServiceRegistry()::registerService);
    }

    private boolean bulkLoaderExists() {
        try {
            Class.forName("com.aerospike.firefly.bulkloader.SparkBulkLoaderMain");
            return true;
        } catch (final ClassNotFoundException e) {
            return false;
        }
    }
}
