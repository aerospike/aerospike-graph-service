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

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;
import java.util.Set;

public class BulkLoaderServiceErrors<I, R> extends BulkLoaderServiceBase<I, R> {
    private static final String KEY = "type";
    public static final String DUPLICATE_VID = "duplicate-vertex-ids";
    public static final String BAD_ENTRY = "bad-entries";
    public static final String BAD_EDGE = "bad-edges";
    private static final Set<String> VALUES = Set.of(DUPLICATE_VID, BAD_ENTRY, BAD_EDGE);

    public BulkLoaderServiceErrors(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "errors";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                "\tExpected argument key '%s' with value of any of '%s'.\n" +
                "\tProvided argument: '%s'.\n" +
                "\tExamples of correct usage:\n" +
                "\t\tg.call(\"%s\").with(\"%s\", \"%s\").next();\n" +
                "\t\tg.call(\"%s\").with(\"%s\", \"%s\").next();\n" +
                "\t\tg.call(\"%s\").with(\"%s\", \"%s\").next();\n",
                getName(), KEY, VALUES, params,
                getName(), KEY, BAD_ENTRY,
                getName(), KEY, DUPLICATE_VID,
                getName(), KEY, BAD_EDGE);
    }

    @Override
    protected boolean sanitize(final Map params) {
        return (params.size() == 1 && params.containsKey(KEY) && VALUES.contains(params.get(KEY)));
    }

    @Override
    protected R execute(final Map params) {
        if (params.get(KEY).equals(DUPLICATE_VID)) {
            return (R) graph.readDuplicateVertexIdErrors();
        } else if (params.get(KEY).equals(BAD_ENTRY)) {
            return (R) graph.readBadEntryErrors();
        } else if (params.get(KEY).equals(BAD_EDGE)) {
            return (R) graph.readBadEdgeErrors();
        } else {
            // This should never happen since it's already safety checked via sanitization.
            throw new IllegalStateException(
                    "Invalid \"" + KEY + "\" detected for \"" + getName() + "\": '" + params.get(KEY) + "'.");
        }
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get bulk load errors.", getUser(), getName());
    }
}
