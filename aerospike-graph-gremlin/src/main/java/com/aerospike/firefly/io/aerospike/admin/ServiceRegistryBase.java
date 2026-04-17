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

import io.vertx.ext.web.Router;

import java.util.Set;

public abstract class ServiceRegistryBase {
    protected Set<AdminService> services;

    public void routeServices(final Router router) {
        for (final AdminService service : services) {
            if (service.needRouting())
                router.route(service.getPath()).handler(service.getHandler());
        }
    }
}
