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

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

public class BulkLoaderServiceCountErrors<I, R> extends BulkLoaderServiceBase<I, R> {

    public BulkLoaderServiceCountErrors(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "error-count";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                "\tExpected no arguments.\n" +
                "\tProvided arguments: '%s'.\n" +
                "\tExample of correct usage:\n" +
                "\t\tg.call(\"%s\").next();\n",
                getName(), params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        return params.isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        final Map<String, Long> errorCounts = new HashMap<>();
        final AerospikeConnection db = graph.getBaseGraph();
        final long badEntryCount = db.incrementAndGetBadEntryCount(0);
        final long duplicateVertexIdCount = db.incrementAndGetDuplicateVertexIdCount(0);
        final long badEdgeCount = db.incrementAndGetBadEdgeCount(0);
        errorCounts.put("duplicate-vertex-id-count", duplicateVertexIdCount);
        errorCounts.put("bad-edge-count", badEdgeCount);
        errorCounts.put("bad-entry-count", badEntryCount);
        return (R) errorCounts;
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get bulk load error count.", getUser(), getName());
    }
}
