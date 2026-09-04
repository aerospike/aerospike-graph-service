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

package com.aerospike.firefly.process.call.sindex;

import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.aerospike.admin.AdminService;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

public abstract class SindexServiceBase<I, R> extends AdminService<I, R> {
    protected static final String ELEMENT_TYPE = "element_type";
    protected static final String PROPERTY_KEY = "property_key";
    protected static final String INDEX_TYPE = "index_type";
    protected static final Map<String, IndexType> INDEX_TYPE_LOOKUP = new HashMap<>();
    static {
        INDEX_TYPE_LOOKUP.put("string", IndexType.STRING);
        INDEX_TYPE_LOOKUP.put("numeric", IndexType.NUMERIC);
        INDEX_TYPE_LOOKUP.put("geo", IndexType.GEO2DSPHERE);
    }

    public SindexServiceBase(final FireflyGraph graph) {
        super(graph);
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }

    @Override
    protected String getAdminNamespace() {
        return "index";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }
}
