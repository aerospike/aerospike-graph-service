package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyGraphSummaryVertex implements Vertex {
    private final FireflyGraph graph;

    private static final String VERTEX_COUNT = "vertex_count";
    private static final String VERTEX_COUNT_PER_LABEL = "vertex_count_per_label";
    private static final String VERTEX_PROPERTIES_PER_LABEL = "vertex_properties_per_label";
    private static final String EDGE_COUNT = "edge_count";
    private static final String EDGE_COUNT_PER_LABEL = "edge_count_per_label";
    private static final String EDGE_PROPERTIES_PER_LABEL = "edge_properties_per_label";
    private static final Set<String> PROPERTY_KEYS = Set.of(
            VERTEX_COUNT,
            VERTEX_COUNT_PER_LABEL,
            VERTEX_PROPERTIES_PER_LABEL,
            EDGE_COUNT,
            EDGE_COUNT_PER_LABEL,
            EDGE_PROPERTIES_PER_LABEL
    );
    public static final String GRAPH_SUMMARY_VERTEX = "~graph_summary";

    public FireflyGraphSummaryVertex(final FireflyGraph graph) {
        this.graph = graph;
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new UnsupportedOperationException("Graph summary vertex is read-only.");
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        throw new UnsupportedOperationException("Graph summary vertex properties are read-only.");
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        throw new UnsupportedOperationException("Graph summary vertex does not have edges.");
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        throw new UnsupportedOperationException("Graph summary vertex does not have adjacent vertices.");
    }

    private Map<String, Object> readData() {
        final FireflyGraphSummaryUpdater.FireflyElementMetadata elementMetadata =
                graph.fireflySummaryUpdater.getFireflyStatistics();
        final Map<String, Object> data = new HashMap<>();
        if (elementMetadata.edgeInfo != null) {
            data.put(EDGE_COUNT, elementMetadata.totalEdgeCount());
            data.put(EDGE_COUNT_PER_LABEL, elementMetadata.edgeCountByLabel());
            data.put(EDGE_PROPERTIES_PER_LABEL, elementMetadata.edgePropertiesByLabel());
        }
        if (elementMetadata.vertexInfo != null) {
            data.put(VERTEX_COUNT, elementMetadata.totalVertexCount());
            data.put(VERTEX_COUNT_PER_LABEL, elementMetadata.vertexCountByLabel());
            data.put(VERTEX_PROPERTIES_PER_LABEL, elementMetadata.vertexPropertiesByLabel());
        }
        return data;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<String> propertyKeyList = new ArrayList<>(List.of(propertyKeys));
        if (propertyKeyList.isEmpty()) {
            propertyKeyList.addAll(PROPERTY_KEYS);
        }
        if (propertyKeyList.stream().anyMatch(key -> !PROPERTY_KEYS.contains(key))) {
            throw new IllegalArgumentException("Invalid property key, acceptable keys are none or any of " + PROPERTY_KEYS + ".");
        }
        final Iterator<String> propertyKeyIterator = propertyKeyList.iterator();
        final Map<String, Object> data = readData();
        final Vertex v = this;

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return propertyKeyIterator.hasNext();
            }

            @Override
            public VertexProperty<V> next() {
                final String propertyKey = propertyKeyIterator.next();
                return new VertexProperty<>() {

                    @Override
                    public String key() {
                        return propertyKey;
                    }

                    @Override
                    public V value() throws NoSuchElementException {
                        return (V) data.get(propertyKey);
                    }

                    @Override
                    public boolean isPresent() {
                        return true;
                    }

                    @Override
                    public Vertex element() {
                        return v;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Approximate statistics are read-only");
                    }

                    @Override
                    public Object id() {
                        return GRAPH_SUMMARY_VERTEX + ":" + propertyKey;
                    }

                    @Override
                    public <V> Property<V> property(final String key, final V value) {
                        throw new UnsupportedOperationException("Approximate statistics are read-only.");
                    }

                    @Override
                    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
                        throw new UnsupportedOperationException("Approximate statistics vertex properties do not contain properties.");
                    }
                };
            }
        };
    }

    @Override
    public Object id() {
        return GRAPH_SUMMARY_VERTEX;
    }

    @Override
    public String label() {
        return GRAPH_SUMMARY_VERTEX;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Approximate statistics vertices are read-only");
    }
}
