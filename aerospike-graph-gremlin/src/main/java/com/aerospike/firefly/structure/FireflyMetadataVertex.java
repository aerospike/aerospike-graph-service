package com.aerospike.firefly.structure;

import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.aerospike.firefly.structure.FireflyGraph.FIREFLY_CONFIGURATION_VARIABLE_NAME;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyMetadataVertex implements Vertex {
    private final FireflyGraph graph;
    private final Configuration config;

    public FireflyMetadataVertex(FireflyGraph graph) {
        this.graph = graph;
        this.config = graph.configuration();
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        throw new UnsupportedOperationException("Metadata vertices are read-only");
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        throw new UnsupportedOperationException("Metadata vertices are read-only");
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        throw new UnsupportedOperationException("Metadata vertices do not have edges");
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        throw new UnsupportedOperationException("Metadata vertices do not have adjacent vertices");
    }

    @Override
    public Object id() {
        return FIREFLY_CONFIGURATION_VARIABLE_NAME;
    }

    @Override
    public String label() {
        return FIREFLY_CONFIGURATION_VARIABLE_NAME;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Metadata vertices are read-only");
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        List<String> keyList = FireflyCloseableIteratorUtils.list(config.getKeys());
        keyList.add(graph.getBaseGraph().DATA_MODEL_NAME);
        keyList.add(graph.getBaseGraph().DATA_MODEL_VER);
        for (String key : propertyKeys) {
            if (!keyList.contains(key)) {
                throw Property.Exceptions.propertyDoesNotExist(this, key);
            }
        }

        final Iterator<String> requestedKeys = propertyKeys.length > 0 ? List.of(propertyKeys).iterator() : keyList.iterator();


        return new Iterator<VertexProperty<V>>() {
            @Override
            public boolean hasNext() {
                return requestedKeys.hasNext();
            }

            @Override
            public VertexProperty<V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                String nextKey = requestedKeys.next();
                String nextValue;
                if (nextKey.equals(graph.getBaseGraph().DATA_MODEL_NAME))
                    nextValue = graph.getBaseGraph().getDataModelMetadata().getDataModelName();
                else if (nextKey.equals(graph.getBaseGraph().DATA_MODEL_VER))
                    nextValue = String.valueOf(graph.getBaseGraph().getDataModelMetadata().getDataModelVersion());
                else
                    nextValue = config.getString(nextKey);
                return new VertexProperty<V>() {
                    @Override
                    public Vertex element() {
                        return new FireflyMetadataVertex(graph);
                    }

                    @Override
                    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
                        return Collections.emptyIterator();
                    }

                    @Override
                    public Object id() {
                        return nextKey;
                    }

                    @Override
                    public <V> Property<V> property(String key, V value) {
                        return null;
                    }

                    @Override
                    public String key() {
                        return nextKey;
                    }

                    @Override
                    public V value() throws NoSuchElementException {
                        return (V) nextValue;
                    }

                    @Override
                    public boolean isPresent() {
                        return true;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Metadata properties are read-only");
                    }
                };
            }
        };
    }
}
