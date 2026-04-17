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

package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.runtime.tasks.FireflyGraphSummaryUpdater;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;

public class MetadataServiceSummary<I, R> extends MetadataServiceBase<I, R> {
    public static final String PRETTY_PRINT_FORMAT_LOG = "\tTotal vertex count: {}.\n" +
            "\tVertex count by label: {}.\n" +
            "\tVertex properties by label: {}.\n" +
            "\tTotal edge count: {}.\n" +
            "\tEdge count by label: {}.\n" +
            "\tEdge properties by label: {}.\n" +
            "\tTotal supernode count: {}.\n" +
            "\tSupernode count by label: {}.";
    public static final String PRETTY_PRINT_FORMAT_SYSTEM = "\tTotal vertex count: %d.\n" +
            "\tVertex count by label: %s.\n" +
            "\tVertex properties by label: %s.\n" +
            "\tTotal edge count: %d.\n" +
            "\tEdge count by label: %s.\n" +
            "\tEdge properties by label: %s.\n" +
            "\tTotal supernode count: %d.\n" +
            "\tSupernode count by label: %s.";

    public MetadataServiceSummary(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "summary";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                        "\tExpected either no arguments provided or just 'pretty' with a value of true or false.\n" +
                        "\tProvided arguments: '%s'.\n" +
                        "\tExample of correct usage:\n" +
                        "\t\tg.call(\"%s\").next();\n" +
                        "\t\t\tor\n" +
                        "\t\tg.call(\"%s\").with(\"pretty\", true).next();\n",
                getName(), params, getName(), getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        return params.isEmpty() || (params.containsKey("pretty") && params.get("pretty") instanceof Boolean);
    }

    @Override
    protected R execute(final Map params) {
        final FireflyGraphSummaryUpdater.FireflyElementMetadata fireflyElementMetadata =
                graph.fireflySummaryUpdater.getFireflyStatistics();
        if (params.containsKey("pretty") && params.get("pretty").equals(true)) {
            return (R)
                    String.format(PRETTY_PRINT_FORMAT_SYSTEM,
                            fireflyElementMetadata.totalVertexCount(),
                            fireflyElementMetadata.vertexCountByLabel().toString(),
                            fireflyElementMetadata.vertexPropertiesByLabel().toString(),
                            fireflyElementMetadata.totalEdgeCount(),
                            fireflyElementMetadata.edgeCountByLabel().toString(),
                            fireflyElementMetadata.edgePropertiesByLabel().toString(),
                            fireflyElementMetadata.totalSupernodeCount(),
                            fireflyElementMetadata.supernodesCountByLabel().toString());
        }
        return (R) Map.of(
                "Total vertex count", fireflyElementMetadata.totalVertexCount(),
                "Vertex count by label", fireflyElementMetadata.vertexCountByLabel(),
                "Vertex properties by label", fireflyElementMetadata.vertexPropertiesByLabel(),
                "Total edge count", fireflyElementMetadata.totalEdgeCount(),
                "Edge count by label", fireflyElementMetadata.edgeCountByLabel(),
                "Edge properties by label", fireflyElementMetadata.edgePropertiesByLabel(),
                "Total supernode count", fireflyElementMetadata.totalSupernodeCount(),
                "Supernode count by label", fireflyElementMetadata.supernodesCountByLabel());
    }

    @Override
    public Map<String, String> describeParams() {
        return Map.of("pretty", "Pretty print the output.");
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get graph summary.", getUser(), getName());
    }
}
