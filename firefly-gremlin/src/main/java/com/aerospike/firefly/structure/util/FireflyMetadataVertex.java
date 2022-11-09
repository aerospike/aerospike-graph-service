package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.aerospike.firefly.io.impl.relational.RelationalGraph.FIREFLY_CONFIGURATION_VARIABLE_NAME;

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
        throw new UnsupportedOperationException("Metadata vertices do not have edges");
    }

    @Override
    public Object id() {
        return FIREFLY_CONFIGURATION_VARIABLE_NAME;
    }

    @Override
    public String label() {
        return null;
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

        for (String key : propertyKeys) {
            if (!config.containsKey(key)) {
                throw Property.Exceptions.propertyDoesNotExist(this, key);
            }
        }

        final Iterator<String> keys = propertyKeys.length > 0 ? List.of(propertyKeys).iterator() : config.getKeys();

        return new Iterator<VertexProperty<V>>() {
            @Override
            public boolean hasNext() {
                return keys.hasNext();
            }

            @Override
            public VertexProperty<V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                String nextKey = keys.next();
                String nextValue = config.getString(nextKey);
                return new VertexProperty<V>() {
                    @Override
                    public Vertex element() {
                        return new FireflyMetadataVertex(graph);
                    }

                    @Override
                    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
                        return null;
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
