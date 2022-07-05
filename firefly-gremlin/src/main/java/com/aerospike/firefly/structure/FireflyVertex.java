package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.aerospike.firefly.structure.util.FireflyHelper.removeVertex;
import static com.aerospike.firefly.util.Tokens.UNIMPLEMENTED;
import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertex extends FireflyElement implements Vertex {

    private final FireflyGraph graph;

    private Map<String, List<VertexProperty>> readVertexProperties() {
        return this.graph.getBaseGraph().readVertexProperties(this);
    }

    private Map<String, List<VertexProperty>> readVertexProperty(String key) {
        return this.graph.getBaseGraph().readVertexProperty(this, key);
    }


    protected Iterator<Object> getInEdgeIds() {
        return this.graph.getBaseGraph().getInEdgeIdsFromVertex(this);
    }

    protected Iterator<Object> getOutEdgeIds() {
        return this.graph.getBaseGraph().getOutEdgeIdsFromVertex(this);
    }

    public FireflyVertex(final FireflyId fid, final String label, final FireflyGraph graph) {
        super(fid, label);
        this.graph = graph;
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
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        if(ElementHelper.getIdValue(keyValues).isPresent())
            if (!graph.features().vertex().properties().supportsUserSuppliedIds())
                throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();

        // If single cardinality, we are setting key to value.
        if (graph.features().vertex().getCardinality(key).equals(VertexProperty.Cardinality.single)
                || VertexProperty.Cardinality.single.equals(cardinality)) {
            // If we do not support null and the value is null, we should simply remove the value.
            if (!allowNullPropertyValues && null == value) {
                properties(key).forEachRemaining(Property::remove);
            }
        }
        // if we don't allow null property values and the value is null then the key can be removed but only if the
        // cardinality is single. if it is list/set then we can just ignore the null.
        final VertexProperty.Cardinality card = null == cardinality ? graph.features().vertex().getCardinality(key) : cardinality;
        if (!allowNullPropertyValues && null == value) {
            if (VertexProperty.Cardinality.single == card || graph.features().vertex().getCardinality(key) == VertexProperty.Cardinality.single) {
                properties(key).forEachRemaining(Property::remove);
            }
            return VertexProperty.empty();
        }


        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

        if (FireflyHelper.inComputerMode(this.graph)) {
            throw new RuntimeException(UNIMPLEMENTED);
        } else {
            FireflyId fid = FireflyId.createFromKeyValuesOrManager(graph,FireflyVertexProperty.class,keyValues);

            this.graph.getBaseGraph().writeVertexProperty(this, fid, key, key, value);
            VertexProperty<Object> vp = this.graph.getBaseGraph().readVertexProperty(this, fid);
            ElementHelper.attachProperties(vp, keyValues);
            return (VertexProperty<V>) vp;
        }
    }

    @Override
    public Set<String> keys() {
        if (null == this.properties()) return Collections.emptySet();
        return FireflyHelper.inComputerMode((FireflyGraph) graph()) ?
                Vertex.super.keys() :
                this.readVertexProperties().keySet();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        FireflyHelper.legalPropertyKeyValueArray(keyValues);
        if(ElementHelper.getIdValue(keyValues).isPresent())
            if (!graph.features().edge().supportsUserSuppliedIds())
                throw Edge.Exceptions.userSuppliedIdsNotSupported();
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (null == label || label.isEmpty()) throw Graph.Exceptions.argumentCanNotBeNull("label");
        if (isHidden(label)) throw Edge.Exceptions.labelCanNotBeAHiddenKey(label);
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);

        return FireflyHelper.addEdge(this.graph, this, (FireflyVertex) vertex, label, keyValues);
    }

    @Override
    public void remove() {
        final List<Edge> edges = new ArrayList<>();
        this.edges(Direction.BOTH).forEachRemaining(edges::add);
        IteratorUtils.filter(IteratorUtils.asIterator(edges), edge -> !((FireflyEdge) edge).removed).forEachRemaining(edge -> ((Edge) edge).remove());
        removeVertex(graph, this.id);
        this.removed = true;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        final Iterator<Edge> edgeIterator = FireflyHelper.getEdges(this, direction, edgeLabels);
        return FireflyHelper.inComputerMode(this.graph) ?
                IteratorUtils.filter(edgeIterator, edge -> this.graph.graphComputerView.legalEdge(this, edge)) :
                edgeIterator;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return FireflyHelper.inComputerMode(this.graph) ?
                direction.equals(Direction.BOTH) ?
                        IteratorUtils.concat(
                                IteratorUtils.map(this.edges(Direction.OUT, edgeLabels), Edge::inVertex),
                                IteratorUtils.map(this.edges(Direction.IN, edgeLabels), Edge::outVertex)) :
                        IteratorUtils.map(this.edges(direction, edgeLabels), edge -> edge.vertices(direction.opposite()).next()) :
                (Iterator) FireflyHelper.getVertices(this, direction, edgeLabels);
    }

    @Override
    public Graph graph() {
        return this.graph;
    }


    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        Map<String, List<VertexProperty>> propertiesMap = (propertyKeys.length == 1) ?
                readVertexProperty(propertyKeys[0]) : readVertexProperties();
        if (propertiesMap.isEmpty()) {
            return Collections.emptyIterator();
        } else if (propertyKeys.length == 1) {
            final List<VertexProperty> properties = propertiesMap.getOrDefault(propertyKeys[0], Collections.emptyList());
            if (properties.size() == 1) {
                return IteratorUtils.of(properties.get(0));
            } else {
                return (Iterator) new ArrayList<>(properties).iterator();
            }
        } else {
            return (Iterator) propertiesMap.entrySet().stream().
                    // Filter for keys that exist.
                    filter(e -> ElementHelper.keyExists(e.getKey(), propertyKeys)).
                    // Map from {String:List<List>} to List<List>.
                    map((Map.Entry::getValue)).
                    // Flatten List<List> to List.
                    flatMap(List::stream).
                    // Convert to iterator.
                    collect(Collectors.toList()).iterator();
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Record getBaseElement() {
        return FireflyRecord.read(graph.getBaseGraph(),graph.getBaseGraph().VERTEX_AERO_SET,FireflyId.fromElement(this)).record();
    }

}
