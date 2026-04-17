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

package com.aerospike.firefly.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class FireflyFeatures implements Graph.Features {
    private final VertexProperty.Cardinality defaultCardinality;
    private final boolean transactionsEnabled;

    public FireflyFeatures(final VertexProperty.Cardinality defaultCardinality,
                           final boolean transactionsEnabled) {
        this.defaultCardinality = defaultCardinality;
        this.transactionsEnabled = transactionsEnabled;
    }

    @Override
    public GraphFeatures graph() {
        return new FireflyGraphFeatures(this.transactionsEnabled);
    }

    /**
     * Gets the features related to "vertex" operation.
     */
    @Override
    public VertexFeatures vertex() {
        return new FireflyVertexFeatures(defaultCardinality);
    }

    /**
     * Gets the features related to "edge" operation.
     */
    @Override
    public EdgeFeatures edge() {
        return new FireflyEdgeFeatures();
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }
}
