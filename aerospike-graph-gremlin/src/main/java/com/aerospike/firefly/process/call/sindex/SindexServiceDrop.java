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
import com.aerospike.firefly.io.aerospike.admin.Admin;
import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/*
    g.call("aerospike.graph.admin.index.drop").
      with("element_type", "<element_type>").
      with("property_key", "<property_key>"); -> drop
 */
public class SindexServiceDrop<I, R> extends SindexServiceBase<I, R> {
    private static final Logger LOG = LoggerFactory.getLogger(SindexServiceDrop.class);
    private static final Map<String, String> PARAMS = new HashMap<>();
    static {
        PARAMS.put(ELEMENT_TYPE, "The type of element to drop the index on. Only 'vertex' is currently supported.");
        PARAMS.put(PROPERTY_KEY, "The property key to drop the index on. '~label' can be used to drop an index on labels.");
        PARAMS.put(INDEX_TYPE, "Optional parameter and not supported for labels. The type of index to drop on the property key. Value must be 'string' or 'numeric'. If not specified both types are dropped.");
    }

    public SindexServiceDrop(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminServiceName() {
        return "drop";
    }

    @Override
    public Map<String, String> describeParams() {
        return PARAMS;
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to %s.\n" +
                        "\tRequired parameters: '" + ELEMENT_TYPE + "', '" + PROPERTY_KEY + "'.\n" +
                        "\tOptional parameters: '" + INDEX_TYPE + "'.\n" +
                        "\tNote: Only 'vertex' is currently supported for '" + ELEMENT_TYPE + "'.\n" +
                        "\tProvided arguments: %s.\n" +
                        "\tExamples of correct usage:\n" +
                        "\t\tg.call(\"%s\").with(\"" + ELEMENT_TYPE + "\", \"vertex\").with(\"" + PROPERTY_KEY + "\", \"~label\").next();\n" +
                        "\t\tg.call(\"%s\").with(\"" + ELEMENT_TYPE + "\", \"vertex\").with(\"" + PROPERTY_KEY + "\", \"name\").next();\n" +
                        "\t\tg.call(\"%s\").with(\"" + ELEMENT_TYPE + "\", \"vertex\").with(\"" + PROPERTY_KEY + "\", \"age\").with(\"" + INDEX_TYPE + "\", \"numeric\").next();",
                getName(), params,getName(), getName(), getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        if (params.size() < 2 || params.size() > 3) {
            return false;
        }
        if (!params.containsKey(ELEMENT_TYPE) || !params.containsKey(PROPERTY_KEY)) {
            return false;
        }
        if (params.size() == 3 && !params.containsKey(INDEX_TYPE)) {
            return false;
        }
        if (!params.get(ELEMENT_TYPE).equals("vertex") || !(params.get(PROPERTY_KEY) instanceof String)) {
            return false;
        }
        if (params.containsKey(INDEX_TYPE)) {
            final Object indexType = params.get(INDEX_TYPE);
            if (!(indexType instanceof String)) {
                return false;
            }
            final String loweredIndexType = ((String) indexType).toLowerCase();
            if (!INDEX_TYPE_LOOKUP.containsKey(loweredIndexType)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected R execute(final Map params) {
        if (params.get(ELEMENT_TYPE).equals("vertex")) {
            try {
                if (params.get(PROPERTY_KEY).equals("~label")) {
                    return (R) Admin.INDEX.dropVertexLabelIndex(graph);
                } else if (params.containsKey(INDEX_TYPE)) {
                    final IndexType indexType = INDEX_TYPE_LOOKUP.get(((String) params.get(INDEX_TYPE)).toLowerCase());
                    return (R) Admin.INDEX.dropVertexPropertyIndex(graph, (String) params.get(PROPERTY_KEY), indexType);
                } else {
                    for (final IndexType indexType : INDEX_TYPE_LOOKUP.values()) {
                        Admin.INDEX.dropVertexPropertyIndex(graph, (String) params.get(PROPERTY_KEY), indexType);
                    }
                    return (R) ("Vertex index of property key '" + params.get(PROPERTY_KEY) + "' dropped.");
                }
            } finally {
                try {
                    graph.fireflyIndexMetadata.updateMetadata();
                } catch (final Exception e) {
                    LOG.warn("Updating Index metadata forcibly due to dropping an index failed.", e);
                }
            }
        } else {
            // Should be caught by sanitize().
            throw new IllegalArgumentException("Only 'vertex' is currently supported for '" + ELEMENT_TYPE + "'.");
        }
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Drop index.", getUser(), getName());
    }
}
