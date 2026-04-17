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

package com.aerospike.firefly.structure;

public class FireflyEdgeProperty<V> extends FireflyProperty<V> {
    private final FireflyGraph graph;
    private final FireflyEdge edge;

    /**
     * Constructor for RelationalProperty.
     *
     * @param graph   Graph that property exists in.
     * @param edge    Edge that property exists on.
     * @param key     Key of property.
     * @param value   Value of property.
     */
    public FireflyEdgeProperty(final FireflyGraph graph, final FireflyEdge edge, final String key, final V value) {
        super(edge, key, value);
        this.graph = graph;
        this.edge = edge;
    }

    /**
     * Remove this property from the graph.
     */
    @Override
    public void remove() {
        graph.getAerospikeOperations().removeEdgeProperty(this);
    }

    public FireflyEdge edge() {
        return edge;
    }
}
