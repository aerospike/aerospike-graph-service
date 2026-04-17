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

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.FireflyExpressionIndex;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
    g.call("aerospike.graph.admin.compound-index.create").
      with("predicates", ["~label:person", "name:John", "age:~gte(30)"]).next();
 */
public class ExpressionIndexServiceCreate<I, R> extends ExpressionIndexServiceBase<I, R> {
    protected static final String PREDICATES = "predicates";
    private static final Map<String, String> PARAMS = new HashMap<>();
    static {
        PARAMS.put(PREDICATES, "A list of predicate strings. Each string should be in the format 'key:value' or 'key:~operator(value)'. " +
                "Example: ['~label:person', 'age:~gte(30)', 'status:active']");
    }

    public ExpressionIndexServiceCreate(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminServiceName() {
        return "create";
    }

    @Override
    public Map<String, String> describeParams() {
        return PARAMS;
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tRequired parameters: '" + PREDICATES + "' (a list of at least 2 predicate strings).\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExamples of correct usage:\n" +
                        "\t\tg.call(\"%s\").with(\"" + PREDICATES + "\", [\"~label:person\", \"status:active\"]).next();\n" +
                        "\t\tg.call(\"%s\").with(\"" + PREDICATES + "\", [\"~label:product\", \"category:electronics\", \"price:~n*\"]).next();",
                getName(), params, getName(), getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        if (params.size() != 1) {
            return false;
        }
        if (!params.containsKey(PREDICATES)) {
            return false;
        }
        final Object predicates = params.get(PREDICATES);
        if (!(predicates instanceof List)) {
            return false;
        }
        final List<?> predicatesList = (List<?>) predicates;
        if (predicatesList.size() < 2) {
            return false;
        }
        for (final Object item : predicatesList) {
            if (!(item instanceof String)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected R execute(final Map params) {
        final List<String> predicatesList = (List<String>) params.get(PREDICATES);
        final String configString = String.join(",", predicatesList);

        final AerospikeConnection db = graph.getBaseGraph();
        db.validateExpressionIndexSupport();

        final List<String> existingIndexes = AerospikeConnection.InfoOps.listExistingIndexes(db).stream()
                .map(Map.Entry::getKey).collect(Collectors.toList());

        final FireflyExpressionIndex index = FireflyExpressionIndex.fromConfigString(db, configString);
        db.createExpIndex(existingIndexes, index);

        graph.fireflyIndexMetadata.updateMetadata();

        final String indexName = index.getName();
        return (R) ("Compound index '" + indexName + "' creation in progress.");
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Create compound index with predicates: {}.", getUser(), getName(), params.get(PREDICATES));
    }
}
