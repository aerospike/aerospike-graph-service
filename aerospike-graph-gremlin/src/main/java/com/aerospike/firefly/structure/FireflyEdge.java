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

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.util.exceptions.GraphError;
import com.aerospike.firefly.util.exceptions.TtlArgumentException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

public class FireflyEdge extends FireflyElement implements Edge {
    // Individual edges' data are stored in a List within the phat edge.
    // These are the indexes in the List for where each value is stored.
    public static final int LABEL_POSITION = 0;
    public static final int IN_V_POSITION = 1;
    public static final int OUT_V_POSITION = 2;
    public static final int PROPERTIES_POSITION = 3;
    public static final int TYPE_HINTS_POSITION = 4;
    public static final int EDGE_DATA_SIZE = 5;
    public static final String EDGE_SUPERNODE_LABEL_KEY = T.label.getAccessor();
    public static final String EDGE_SUPERNODE_ADJACENT_ID_KEY = "~ADJACENT_ID";

    // These are used for FireflyEdgeRecord when unpacking a phat Edge records' data.
    public static final int IS_IN_SUPERNODE_POSITION = 5;
    public static final int IS_OUT_SUPERNODE_POSITION = 6;

    protected final AerospikeConnection db;
    public boolean removed;
    protected final FireflyGraph graph;
    protected final FireflyId inVid;
    protected final FireflyId outVid;
    protected final Map<String, Object> properties;
    protected final Map<String, Object> typeHints;
    private final boolean isInSupernode;
    private final boolean isOutSupernode;
    private final int generation;

    public FireflyEdge(final FireflyPhatEdgeId id,
                       final String label,
                       final FireflyGraph graph,
                       final FireflyId outVid,
                       final FireflyId inVid,
                       final Map<String, Object> properties,
                       final Map<String, Object> typeHints,
                       final boolean isOutSupernode,
                       final boolean isInSupernode,
                       final int generation) {
        super(id, label);
        this.graph = graph;
        this.inVid = inVid;
        this.outVid = outVid;
        this.properties = properties;
        this.typeHints = typeHints;
        this.db = graph.getBaseGraph();
        this.isOutSupernode = isOutSupernode;
        this.isInSupernode = isInSupernode;
        this.generation = generation;
    }

    /**
     * Remove property from edge property cache.
     *
     * @param key Key to remove.
     */
    public void removePropertyFromCache(final String key) {
        properties.remove(key);
        typeHints.remove(key);
    }

    public FireflyId outVertexId() {
        return outVid;
    }

    public FireflyId inVertexId() {
        return inVid;
    }

    public boolean isOutSupernode() {
        return isOutSupernode;
    }

    public boolean isInSupernode() {
        return isInSupernode;
    }

    public void addTypeHint(final String propertyKey, final Object typeHint) {
        typeHints.put(propertyKey, typeHint);
    }

    public <V> void addPropertyToCache(final String propertyKey, final V value) {
        properties.put(propertyKey, value);
    }

    @Override
    public Vertex outVertex() {
        // TODO GRAPH-1145: Restore the following and handle skipping null returns.
        //return graph.readVertex(this.outVid);
        return graph.aerospikeOperations.getSingleVertex(this.outVid);
    }

    @Override
    public Vertex inVertex() {
        // TODO GRAPH-1145: Restore the following and handle skipping null returns.
        //return graph.readVertex(this.inVid);
        return graph.aerospikeOperations.getSingleVertex(this.inVid);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (removed) return Collections.emptyIterator();
        switch (direction) {
            case OUT:
                return FireflyCloseableIteratorUtils.of(this.outVertex());
            case IN:
                return FireflyCloseableIteratorUtils.of(this.inVertex());
            default:
                return FireflyCloseableIteratorUtils.of(this.outVertex(), this.inVertex());
        }
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public <V> Property<V> property(final String key) {
        final Object value = properties.get(key);
        if (value != null) {
            final V casted = (V) this.db.convertValueToTypeUsingHint(value, typeHints.get(key));
            return new FireflyEdgeProperty<>(graph, this, key, casted);
        } else {
            return Property.empty();
        }
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        FireflyHelper.legalPropertyKeyValueArray(key, value);

        // Edge is already removed.
        if (this.removed) {
            throw elementAlreadyRemoved(Edge.class, id);
        }

        // Handle TTL.
        if (TTL_PROPERTY_KEY.equals(key)) {
            if (!db.getConfig().ttlEnabledFlag) {
                throw new AerospikeGraphException(GraphError.TTL_NOT_ENABLED);
            }
            if (value == null) {
                return Property.empty();
            }
            if (Number.class.isAssignableFrom(value.getClass())) {
                graph.aerospikeOperations.setEdgeTTL(this, ((Number) value).longValue());
                return Property.empty();
            } else {
                throw new TtlArgumentException(value);
            }
        }

        // Cannot be hidden key.
        if (isHidden(key))
            throw Property.Exceptions.propertyKeyCanNotBeAHiddenKey(key);

        // Remove the property.
        if (null == value) {
            properties(key).forEachRemaining(Property::remove);
            properties.remove(key);
            typeHints.remove(key);
            return Property.empty();
        }

        // Write the property and add to edge.
        final V validatedValue = (V) FireflyHelper.validatePropertyValue(value);
        return graph.getAerospikeOperations().writeProperty(this, key, validatedValue);
    }

    @Override
    public void remove() {
        graph.aerospikeOperations.removeEdge(this, true, true, null);
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        // If there is only 1 key.
        if (propertyKeys.length == 1) {
            // And that key is null, return empty iterator.
            if (propertyKeys[0] == null) {
                return Collections.emptyIterator();
            }

            // Otherwise if there is only 1 key and it is not null, return the property if we have it, otherwise empty iterator.
            final Object singleValue = properties.get(propertyKeys[0]);
            if (singleValue != null) {
                return FireflyCloseableIteratorUtils.of(new FireflyEdgeProperty<>(graph, this, propertyKeys[0],
                        (V) this.db.convertValueToTypeUsingHint(
                                singleValue, typeHints.get(propertyKeys[0]))));
            } else {
                return Collections.emptyIterator();
            }
        } else {
            // There are multiple keys.
            final List<Property<V>> propertyList = new ArrayList<>();
            for (final String key : properties.keySet()) {
                if (ElementHelper.keyExists(key, propertyKeys)) {
                    propertyList.add(new FireflyEdgeProperty<>(graph, this, key,
                            (V) this.graph.getBaseGraph().convertValueToTypeUsingHint(
                                    properties.get(key), typeHints.get(key))));
                }
            }
            return propertyList.iterator();
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    public int getGeneration() {
        return generation;
    }
}
