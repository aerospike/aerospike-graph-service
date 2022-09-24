package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.aerospike.firefly.util.Tokens.UNIMPLEMENTED;
import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyVertex extends FireflyElement implements Vertex {

    protected FireflyGraph graph;

    public FireflyVertex(final FireflyId fid, final String label, final FireflyGraph graph) {
        super(fid, label);
        this.graph = graph;
    }

    // Abstract functions to be implemented by concrete implementation
    protected abstract <V> Iterator<Map.Entry<String, VertexProperty<V>>> readVertexProperties();

    protected abstract <V> Iterator<VertexProperty<V>> readVertexProperty(final String key);

    public abstract void writeVertexProperty(final FireflyVertexProperty vertexProperties);

    public abstract void removeVertexPropertyForModel(final String key, final FireflyId vertexPropertyId);

    public abstract long getVertexPropertyCount();

    protected abstract void removeEdge(final Direction direction, final FireflyId edgeId, final String edgeLabel);

    public abstract void writeEdge(final Direction direction, final FireflyId edgeId, final String edgeLabel);

    public abstract Iterator<Long> getEdgeIdsFromVertex(final Direction direction);

    protected abstract Set<String> readVertexPropertyKeys();

    /**
     * Function to remove vertex properties. Goes here so it can ensure it hits the star model as well.
     *
     * @param key              Vertex property key.
     * @param vertexPropertyId Vertex property id.
     */
    public void removeVertexProperty(final String key, final FireflyId vertexPropertyId) {
        if (StarPackedGraph.isStarPackedGraph(graph)) {
            StarPackedGraph.removeVertexProperty(graph.getBaseGraph(), this, key);
        }
        removeVertexPropertyForModel(key, vertexPropertyId);

    }

    /**
     * Create a new vertex property. If the cardinality is {@link VertexProperty.Cardinality#single}, then set the key
     * to the value. If the cardinality is {@link VertexProperty.Cardinality#list}, then add a new value to the key.
     * If the cardinality is {@link VertexProperty.Cardinality#set}, then only add a new value if that value doesn't
     * already exist for the key. If the value already exists for the key, add the provided key value vertex property
     * properties to it.
     *
     * @param cardinality the desired cardinality of the property key
     * @param key         the key of the vertex property
     * @param value       The value of the vertex property
     * @param keyValues   the key/value pairs to turn into vertex property properties
     * @param <V>         the type of the value of the vertex property
     * @return the newly created vertex property
     */
    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (this.removed)
            throw elementAlreadyRemoved(Vertex.class, this.id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        if (ElementHelper.getIdValue(keyValues).isPresent() &&
                !graph.features().vertex().properties().supportsUserSuppliedIds())
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();

        // If single cardinality, we are setting key to value.
        if (graph.features().vertex().getCardinality(key).equals(VertexProperty.Cardinality.single) ||
                VertexProperty.Cardinality.single.equals(cardinality)) {
            // If single cardinality we should remove existing properties with the same key.
            properties(key).forEachRemaining(Property::remove);

            // If we do not support null and the value is null, we should return empty.
            if (!allowNullPropertyValues && null == value) {
                return VertexProperty.empty();
            }
        }

        // If we don't allow null property values and the value is null then the key can be removed but only if the
        // cardinality is single.
        // If it is list/set then we can just ignore the null.
        final VertexProperty.Cardinality card = null == cardinality ? graph.features().vertex().getCardinality(key) : cardinality;
        // If we do not support null and the value is null, we should return empty.
        if (!allowNullPropertyValues && null == value) {
            return VertexProperty.empty();
        }

        if (VertexProperty.Cardinality.single == card ||
                graph.features().vertex().getCardinality(key).equals(VertexProperty.Cardinality.single)) {
            // If single cardinality we should remove existing properties with the same key.
            properties(key).forEachRemaining(Property::remove);
        }


        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) {
            return optionalVertexProperty.get();
        }
        if (FireflyHelper.inComputerMode(this.graph)) {
            throw new RuntimeException(UNIMPLEMENTED);
        }

        // Create Firefly id for vertex property.
        final FireflyId vertexPropertyId = ElementHelper.getIdValue(keyValues).isPresent() ?
                FireflyId.of(FireflyVertexProperty.class, ElementHelper.getIdValue(keyValues).get()) :
                FireflyId.createFromManager(graph, FireflyVertexProperty.class);

        // Write vertex property to graph.
        final VertexProperty<V> vertexProperty = graph.writeVertexProperty(vertexPropertyId, this, key, value);
        ElementHelper.attachProperties(vertexProperty, keyValues);

        // Return vertex property.
        return vertexProperty;
    }

    @Override
    public Set<String> keys() {
        return FireflyHelper.inComputerMode((FireflyGraph) graph()) ?
                Vertex.super.keys() : readVertexPropertyKeys();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        FireflyHelper.legalPropertyKeyValueArray(keyValues);

        // Validate edge and vertex.
        if (ElementHelper.getIdValue(keyValues).isPresent() && !graph.features().edge().supportsUserSuppliedIds())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();
        if (null == vertex)
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (null == label || label.isEmpty())
            throw Graph.Exceptions.argumentCanNotBeNull("label");
        if (isHidden(label))
            throw Edge.Exceptions.labelCanNotBeAHiddenKey(label);
        if (this.removed)
            throw elementAlreadyRemoved(Vertex.class, this.id);

        // Get id for edge.
        FireflyId edgeId = FireflyId.createFromKeyValuesOrManager(graph, FireflyEdge.class, keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
            try {
                NumericIdManager.convert(edgeId.value());
            } catch (IllegalArgumentException ignored) {
                // Invalid type for id.
                throw Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            }
            if (graph.edgeExists(edgeId)) {
                throw Graph.Exceptions.edgeWithIdAlreadyExists(edgeId.value());
            }
        } else {
            while (graph.edgeExists(edgeId)) {
                edgeId = FireflyId.createFromManager(graph, FireflyEdge.class);
            }
        }

        // Write fully qualified edge.
        final List<Map.Entry<String, Object>> properties =
                graph.convertFullyQualified(graph.features().edge().supportsNullPropertyValues(), keyValues);
        return graph.writeEdge(edgeId, label, properties, (FireflyVertex) vertex, this);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        final Iterator<Edge> edgeIterator = FireflyHelper.getEdges(graph, this, direction, edgeLabels);
        return FireflyHelper.inComputerMode(this.graph) ?
                IteratorUtils.filter(edgeIterator,
                        edge -> this.graph.graphComputerView.legalEdge(this, edge)) :
                edgeIterator;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return FireflyHelper.inComputerMode(this.graph) ? direction.equals(Direction.BOTH) ?
                IteratorUtils.concat(
                        IteratorUtils.map(this.edges(Direction.OUT, edgeLabels), Edge::inVertex),
                        IteratorUtils.map(this.edges(Direction.IN, edgeLabels), Edge::outVertex)) :
                IteratorUtils.map(this.edges(direction, edgeLabels), edge -> edge.vertices(direction.opposite()).next()) :
                FireflyHelper.getVertices(graph, this, direction, edgeLabels);
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if (propertyKeys.length == 1) {
            return (propertyKeys[0] != null) ?
                    // Read single vertex property.
                    readVertexProperty(propertyKeys[0]) :
                    // Null property key is not valid and also can cause null key exception in the map.
                    Collections.emptyIterator();
        }


        // Read multiple vertex properties.
        final Iterator<Map.Entry<String, VertexProperty<V>>> vertexProperties = readVertexProperties();
        List<Map.Entry<String, VertexProperty<Object>>> ct = IteratorUtils.list(readVertexProperties());
//        List<Map.Entry<String, VertexProperty<V>>> l = IteratorUtils.list(vertexProperties);
        // Return an iterator over the map.
        return (!vertexProperties.hasNext()) ? Collections.emptyIterator() :

                IteratorUtils.map(IteratorUtils.filter(vertexProperties,
                                e -> ElementHelper.keyExists(e.getKey(), propertyKeys)),
                        Map.Entry::getValue);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Record getBaseElement() {
        return FireflyRecord.read(graph.getBaseGraph(), graph.getBaseGraph().VERTEX_AERO_SET, FireflyId.fromElement(this)).record();
    }

}
