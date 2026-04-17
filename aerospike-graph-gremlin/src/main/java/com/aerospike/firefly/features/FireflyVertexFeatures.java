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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

class FireflyVertexFeatures extends FireflyElementFeatures implements Graph.Features.VertexFeatures {

    private final VertexProperty.Cardinality defaultCardinality;

    public FireflyVertexFeatures(final VertexProperty.Cardinality defaultCardinality) {
        this.defaultCardinality = defaultCardinality;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
        return true;
    }

    @Override
    public boolean supportsNumericIds() {
        return true;
    }

    /**
     * Gets the {@link VertexProperty.Cardinality} for a key.  By default, this method will return
     * {@link VertexProperty.Cardinality#list}.  Implementations that employ a schema can consult it to
     * determine the {@link VertexProperty.Cardinality}.  Those that do no have a schema can return their
     * default {@link VertexProperty.Cardinality} for every key.
     * <p/>
     * Note that this method is primarily used by TinkerPop for internal usage and may not be suitable to
     * reliably determine the cardinality of a key. For some implementation it may offer little more than a
     * hint on the actual cardinality. Generally speaking it is likely best to drop down to the API of the
     * {@link Graph} implementation for any schema related queries.
     */
    @Override
    public VertexProperty.Cardinality getCardinality(final String key) {
        return this.defaultCardinality;
    }

    /**
     * Determines if a {@link Vertex} can be added to the {@code Graph}.
     */
    @Override
    public boolean supportsAddVertices() {
        return true;
    }

    /**
     * Determines if a {@link Vertex} can be removed from the {@code Graph}.
     */
    @Override
    public boolean supportsRemoveVertices() {
        return true;
    }

    /**
     * Determines if a {@link Vertex} can support multiple properties with the same key.
     */
    @Override
    public boolean supportsMultiProperties() {
        return true;
    }

    /**
     * Determines if a {@link Vertex} can support non-unique values on the same key. For this value to be
     * {@code true}, then {@link #supportsMetaProperties()} must also return true. By default this method,
     * just returns what {@link #supportsMultiProperties()} returns.
     */
    @Override
    public boolean supportsDuplicateMultiProperties() {
        return supportsMultiProperties();
    }

    /**
     * Determines if a {@link Vertex} can support properties on vertex properties.  It is assumed that a
     * graph will support all the same data types for meta-properties that are supported for regular
     * properties.
     */
    @Override
    public boolean supportsMetaProperties() {
        return true;
    }

    /**
     * Determines if the {@code Graph} implementation uses upsert functionality as opposed to insert
     * functionality for {@link Graph#addVertex(String)}. This feature gives graph providers some flexibility as
     * to how graph mutations are treated. For graph providers, testing of this feature (as far as TinkerPop
     * is concerned) only covers graphs that can support user supplied identifiers as there is no other way
     * for TinkerPop to know what aspect of a vertex is unique to appropriately apply assertions. Graph
     * providers, especially those who support schema features, may have other methods for uniquely identifying
     * a vertex and should therefore resort to their own body of tests to validate this feature.
     */
    @Override
    public boolean supportsUpsert() {
        return false;
    }

    /**
     * Gets features related to "properties" on a {@link Vertex}.
     */
    @Override
    public Graph.Features.VertexPropertyFeatures properties() {
        return new FireflyVertexPropertyFeatures();
    }
}
