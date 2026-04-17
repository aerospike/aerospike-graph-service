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

package com.aerospike.firefly.io.aerospike.admin;

import com.aerospike.firefly.process.call.AdministrativeInfoService;
import com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceRegistry;
import com.aerospike.firefly.process.call.cache.CacheServiceRegistry;
import com.aerospike.firefly.process.call.expressionindex.ExpressionIndexServiceRegistry;
import com.aerospike.firefly.process.call.metadata.MetadataServiceRegistry;
import com.aerospike.firefly.process.call.query.QueryServiceRegistry;
import com.aerospike.firefly.process.call.rbac.JwtServiceRegistry;
import com.aerospike.firefly.process.call.sindex.SindexServiceRegistry;
import com.aerospike.firefly.structure.FireflyGraph;
import io.vertx.ext.web.Router;

public class AdminServiceRegistry {

    private final SindexServiceRegistry sindexServiceRegistry;
    private final ExpressionIndexServiceRegistry expressionIndexServiceRegistry;
    private final MetadataServiceRegistry metadataServiceRegistry;
    private final BulkLoaderServiceRegistry bulkLoaderServiceRegistry;
    private final AdministrativeInfoService administrativeInfoService;
    private final JwtServiceRegistry jwtServiceRegistry;
    private final QueryServiceRegistry queryServiceRegistry;
    private final CacheServiceRegistry cacheServiceRegistry;

    public AdminServiceRegistry(final FireflyGraph firefly) {
        sindexServiceRegistry = new SindexServiceRegistry(firefly);
        expressionIndexServiceRegistry = new ExpressionIndexServiceRegistry(firefly);
        metadataServiceRegistry = new MetadataServiceRegistry(firefly);
        bulkLoaderServiceRegistry = new BulkLoaderServiceRegistry(firefly);
        administrativeInfoService = new AdministrativeInfoService(firefly);
        jwtServiceRegistry = new JwtServiceRegistry(firefly);
        queryServiceRegistry = new QueryServiceRegistry(firefly);
        cacheServiceRegistry = new CacheServiceRegistry(firefly);
    }

    public void appendHandlers(final Router router) {
        sindexServiceRegistry.routeServices(router);
        expressionIndexServiceRegistry.routeServices(router);
        metadataServiceRegistry.routeServices(router);
        bulkLoaderServiceRegistry.routeServices(router);
        jwtServiceRegistry.routeServices(router);
        queryServiceRegistry.routeServices(router);
        cacheServiceRegistry.routeServices(router);
    }
}
