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

package com.aerospike.firefly.process.call;

import com.aerospike.firefly.io.aerospike.admin.AdminService;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.service.Service;

import java.util.Map;

public class AdministrativeInfoService extends AdminService {
    public AdministrativeInfoService(final FireflyGraph graph) {
        super(graph);
        graph.getServiceRegistry().registerService(this);
    }

    @Override
    public Service createService(final boolean isStart, final Map params) {
        return this;
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }

    @Override
    protected String getAdminNamespace() {
        return "reserved";
    }

    @Override
    protected String getAdminServiceName() {
        return "info";
    }

    @Override
    protected String usage(final Map params) {
        // This should never happen.
        throw new UnsupportedOperationException("This service should never be executed.");
    }

    @Override
    protected boolean sanitize(final Map params) {
        // This should never happen.
        throw new UnsupportedOperationException("This service should never be executed.");
    }

    @Override
    protected Object execute(final Map params) {
        // This should never happen.
        throw new UnsupportedOperationException("This service should never be executed.");
    }

    @Override
    protected void auditLog(final Map params) {
        // This should never happen.
        throw new UnsupportedOperationException("This service should never be executed.");
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }
}
