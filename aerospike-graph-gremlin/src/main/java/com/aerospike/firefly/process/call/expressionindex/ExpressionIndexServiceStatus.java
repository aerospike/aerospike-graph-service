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

package com.aerospike.firefly.process.call.expressionindex;

import com.aerospike.firefly.io.aerospike.admin.Admin;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

/*
    g.call("aerospike.graph.admin.compound-index.status").
      with("index_name", "<index_name>").next();
 */
public class ExpressionIndexServiceStatus<I, R> extends ExpressionIndexServiceBase<I, R> {
    protected static final String INDEX_NAME = "index_name";
    private static final Map<String, String> PARAMS = new HashMap<>();
    static {
        PARAMS.put(INDEX_NAME, "The name of the compound index to get the status of.");
    }

    public ExpressionIndexServiceStatus(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminServiceName() {
        return "status";
    }

    @Override
    public Map<String, String> describeParams() {
        return PARAMS;
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tRequired parameters: '" + INDEX_NAME + "'.\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExample of correct usage:\n" +
                        "\t\tg.call(\"%s\").with(\"" + INDEX_NAME + "\", \"compound_index_name\").next();",
                getName(), params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        if (params.size() != 1) {
            return false;
        }
        if (!params.containsKey(INDEX_NAME)) {
            return false;
        }
        return params.get(INDEX_NAME) instanceof String;
    }

    @Override
    protected R execute(final Map params) {
        final String indexName = (String) params.get(INDEX_NAME);
        return (R) Admin.INDEX.getExpressionIndexStatus(graph, indexName);
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get compound index status: {}.", getUser(), getName(), params.get(INDEX_NAME));
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.READ;
    }
}
