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

import com.aerospike.firefly.io.aerospike.admin.Admin;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Collections;
import java.util.Map;

// Used as g.call("aerospike.graph.admin.index.list").next();
public class SindexServiceList<I, R> extends SindexServiceBase<I, R> {

    public SindexServiceList(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminServiceName() {
        return "list";
    }

    @Override
    public Map<String, String> describeParams() {
        // No parameters.
        return Collections.emptyMap();
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tExpected no arguments provided.\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExample of correct usage: g.call(\"%s\").next();",
                getName(), params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        // Should be no parameters.
        return params.isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        return (R) Admin.INDEX.getIndexList(graph);
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - List indexes.", getUser(), getName());
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.READ;
    }
}
