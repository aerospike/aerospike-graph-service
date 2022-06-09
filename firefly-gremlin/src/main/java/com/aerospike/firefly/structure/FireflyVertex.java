package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyRecord;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

import static com.aerospike.firefly.structure.FireflyHelper.removeVertex;
import static com.aerospike.firefly.util.Tokens.UNIMPLEMENTED;
import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyVertex extends FireflyElement implements WrappedVertex<Record>, Vertex {

    private final FireflyGraph graph;

    private Map<String, List<VertexProperty>> readVertexProperties() {
        return this.graph.db.readVertexProperties(this);
    }


    protected Iterator<Object> getInEdgeIds() {
        return this.graph.db.getInEdgeIdsFromVertexByScan(this);
    }

    protected Iterator<Object> getOutEdgeIds() {
        return this.graph.db.getOutEdgeIdsFromVertexByScan(this);
    }

    public FireflyVertex(final FireflyRecord record, final FireflyId fid, final String label, final FireflyGraph graph) {
        super(fid, label, record);
        this.graph = graph;
    }


    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        if(ElementHelper.getIdValue(keyValues).isPresent())
            if (!graph.features().vertex().properties().supportsUserSuppliedIds())
                throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();
        // if we don't allow null property values and the value is null then the key can be removed but only if the
        // cardinality is single. if it is list/set then we can just ignore the null.
        final VertexProperty.Cardinality card = null == cardinality ? graph.features().vertex().getCardinality(key) : cardinality;
        if (VertexProperty.Cardinality.single == card || graph.features().vertex().getCardinality(key) == VertexProperty.Cardinality.single)
            properties(key).forEachRemaining(VertexProperty::remove);
        if (!allowNullPropertyValues && null == value) {
            return VertexProperty.empty();
        }


        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

        if (FireflyHelper.inComputerMode(this.graph)) {
            throw new RuntimeException(UNIMPLEMENTED);
        } else {
            FireflyId fid = FireflyId.createFromKeyValuesOrManager(graph,FireflyVertexProperty.class,keyValues);

            this.graph.db.writeVertexProperty(this, fid, key, key, value);
            VertexProperty<Object> vp = this.graph.db.readVertexProperty(this, fid);
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
        final Iterator<Edge> edgeIterator = (Iterator) FireflyHelper.getEdges(this, direction, edgeLabels);
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
        //@todo performance
        Map<String, List<VertexProperty>> allProperties = this.readVertexProperties();
        if (propertyKeys.length == 1) {
            final List<VertexProperty> properties = allProperties.getOrDefault(propertyKeys[0], Collections.emptyList());
            if (properties.size() == 1) {
                return IteratorUtils.of(properties.get(0));
            } else if (properties.isEmpty()) {
                return Collections.emptyIterator();
            } else {
                return (Iterator) new ArrayList<>(properties).iterator();
            }
        } else {
            return IteratorUtils.flatMap(IteratorUtils.filter(IteratorUtils.asIterator(allProperties.entrySet()),
                            entry -> ElementHelper.keyExists((String) ((AbstractMap.Entry) entry).getKey(), propertyKeys)),
                    entry -> IteratorUtils.asIterator(((AbstractMap.Entry) entry).getValue()));
        }
    }

    @Override
    public Record getBaseVertex() {
        return record.record;
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

}
