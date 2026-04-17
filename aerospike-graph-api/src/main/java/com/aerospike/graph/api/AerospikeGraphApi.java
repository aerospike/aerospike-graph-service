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

package com.aerospike.graph.api;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph.Features;

/**
 * Embedded entry point for applications that want to run Aerospike Graph
 * Service in-process against an Aerospike cluster.
 *
 * <p>Obtain an instance via {@link #builder()}, then use {@link #traversal()}
 * to get a Gremlin {@link GraphTraversalSource}. Instances hold cluster
 * connections and <strong>must</strong> be closed; they implement
 * {@link AutoCloseable} so they can be used in a try-with-resources block:
 *
 * <pre>{@code
 * try (AerospikeGraphApi graph = AerospikeGraphApi.builder()
 *         .withHost("localhost")
 *         .withPort(3000)
 *         .withNamespace("test")
 *         .build()) {
 *     long vertexCount = graph.traversal().V().count().next();
 * }
 * }</pre>
 *
 * <p>For a remote / Gremlin-Server deployment, use the standard
 * {@code gremlin-driver} {@code DriverRemoteConnection} instead of this API.
 *
 * <p>Implementations are expected to be thread-safe once constructed; the
 * returned {@link GraphTraversalSource} may be shared across threads.
 */
public interface AerospikeGraphApi extends AutoCloseable {
    /**
     * Returns a new {@link AerospikeGraphApiBuilder} for configuring and
     * constructing an {@code AerospikeGraphApi} instance.
     *
     * @return a fresh builder; never {@code null}
     */
    static AerospikeGraphApiBuilder builder() {
        return new AerospikeGraphApiBuilder();
    }

    /**
     * Returns a Gremlin {@link GraphTraversalSource} bound to this graph.
     * The returned source is the primary entry point for read and write
     * traversals; callers may cache it for the lifetime of this
     * {@code AerospikeGraphApi}.
     *
     * @return a traversal source rooted at this graph; never {@code null}
     */
    GraphTraversalSource traversal();

    /**
     * Returns the effective, fully-resolved Apache Commons
     * {@link Configuration} used to construct this graph. The returned
     * configuration reflects any properties set via the builder together
     * with any values loaded from a configuration file.
     *
     * @return the resolved configuration; never {@code null}
     */
    Configuration configuration();

    /**
     * Returns the TinkerPop {@link Features} descriptor for this graph,
     * describing which optional graph features (transactions, user-supplied
     * IDs, graph computer, etc.) this Aerospike-backed graph supports.
     *
     * @return the feature descriptor; never {@code null}
     */
    Features features();
}
