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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

class FireflyEdgeFeatures extends FireflyElementFeatures implements Graph.Features.EdgeFeatures {
    @Override
    public boolean supportsUserSuppliedIds() {
        return false;
    }

    @Override
    public boolean supportsNumericIds() {
        return false;
    }

    /**
     * Determines if an {@link Edge} can be added to a {@code Vertex}.
     */
    @Override
    public boolean supportsAddEdges() {
        return true;
    }

    /**
     * Determines if an {@link Edge} can be removed from a {@code Vertex}.
     */
    @Override
    public boolean supportsRemoveEdges() {
        return true;
    }

    /**
     * Determines if the {@code Graph} implementation uses upsert functionality as opposed to insert
     * functionality for {@link Vertex#addEdge(String, Vertex, Object...)}. This feature gives graph providers
     * some flexibility as to how graph mutations are treated. For graph providers, testing of this feature
     * (as far as TinkerPop is concerned) only covers graphs that can support user supplied identifiers as
     * there is no other way for TinkerPop to know what aspect of a edge is unique to appropriately apply
     * assertions. Graph providers, especially those who support schema features, may have other methods for
     * uniquely identifying a edge and should therefore resort to their own body of tests to validate this
     * feature.
     */
    @Override
    public boolean supportsUpsert() {
        return false;
    }

    /**
     * Gets features related to "properties" on an {@link Edge}.
     */
    @Override
    public Graph.Features.EdgePropertyFeatures properties() {
        return new FireflyEdgePropertyFeatures();
    }
}
