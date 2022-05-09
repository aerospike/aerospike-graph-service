package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.process.computer.FireflyGraphComputerView;
import com.aerospike.firefly.util.Exceptions.Unimplemented;
import com.aerospike.firefly.util.FireflyConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.aerospike.firefly.util.Tokens.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyGraph implements Graph, WrappedGraph<AerospikeConnection> {
    protected final AerospikeConnection db;
    private final FireflyConfiguration fireflyConfiguration;
    private AtomicBoolean closed = new AtomicBoolean(false);

    private final FireflyGraphFeatures features = new FireflyGraphFeatures();


    protected final FireflyGraph.IdManager<?> vertexIdManager;
    protected final FireflyGraph.IdManager<?> edgeIdManager;
    protected final FireflyGraph.IdManager<?> vertexPropertyIdManager;


    protected FireflyGraphComputerView graphComputerView = null;

    private FireflyVertex readVertex(Object id) {
        return this.db.readVertex(this, id);
    }

    private void writeVertex(Object id, String label) {
        this.db.writeVertex(this, id, label);
    }

    public void removeVertex(Object id) {
        this.db.removeVertex(this, id);
    }

    public boolean hasVertex(Object id) {
        return !(readVertex(id) == null);
    }

    private FireflyEdge readEdge(Object id) {
        throw new Unimplemented();
    }

    private void writeEdge(Object id, String label) {
        throw new Unimplemented();
    }

    private void removeEdge(Object id) {
        throw new Unimplemented();
    }

    private boolean hasEdge(Object id) {
        throw new Unimplemented();
    }

    private long getLastId(String type) {
        return this.db.getIdCounter(type);
    }

    private long getNextId(String type) {
        return this.db.incrementIdCounter(type);
    }

    protected FireflyGraph(final AerospikeConnection db, final FireflyConfiguration configuration) {
        this.fireflyConfiguration = configuration;
        this.db = db;
        vertexPropertyIdManager = new DefaultIdManager(VERTEX_PROPERTY_ID_COUNTER_SET);
        vertexIdManager = new DefaultIdManager(VERTEX_ID_COUNTER_SET);
        edgeIdManager = new DefaultIdManager(EDGE_ID_COUNTER_SET);
    }

    public static FireflyGraph open(AerospikeConnection db, FireflyConfiguration conf) {
        return new FireflyGraph(db, conf);
    }

    @Override
    public AerospikeConnection getBaseGraph() {
        return db;
    }


    public interface IdManager<T> {
        /**
         * Generate an identifier which should be unique to the {@link FireflyGraph} instance.
         */
        T getNextId(final FireflyGraph graph);

        /**
         * Convert an identifier to the type required by the manager.
         */
        T convert(final Object id);

        /**
         * Determine if an identifier is allowed by this manager given its type.
         */
        boolean allow(final Object id);
    }

    @Override
    public Features features() {
        return features;
    }

    public class FireflyGraphFeatures implements Features {

    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        if (null != idValue) {
            if (this.readVertex(idValue) != null)
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = vertexIdManager.getNextId(this);
        }
        //@todo performance: dont reread
        this.writeVertex(idValue, label);
        Vertex vertex = this.readVertex(idValue);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        return vertex;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new Unimplemented();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new Unimplemented();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        //@todo correctness
        // should be a scan query, find vertices that have not been deleted
        Iterator<Long> itr;
        List<Long> longs = new ArrayList<>();
        Arrays.stream(vertexIds).forEach(o -> {
            longs.add((long)o);
        });
        if (vertexIds.length != 0)
            itr = longs.iterator();
        else
            itr = (Iterator<Long>)LongStream.range(1L, (long) db.getIdCounter(GLOBAL)).iterator();

        return new FireflyVertexIterator(this, itr);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return new FireflyEdgeIterator(this, Arrays.stream(edgeIds).iterator());
    }

    @Override
    public Transaction tx() {
        return null;
    }

    @Override
    public void close() throws Exception {
        this.closed.set(true);
        this.db.close();

    }

    @Override
    public Variables variables() {
        return null;
    }

    @Override
    public Configuration configuration() {
        return fireflyConfiguration.toApacheConfiguration();
    }

    public static class DefaultIdManager implements FireflyGraph.IdManager<Long> {

        /**
         * Manages identifiers of type {@code Long}. Will convert any class that extends from {@link Number} to a
         * {@link Long} and will also attempt to convert {@code String} values
         */
        private final String counterNamespace;

        public DefaultIdManager(String counterNamespace) {
            this.counterNamespace = counterNamespace;
        }

        private static String createErrorMessage(final Class<?> expectedType, final Object id) {
            return String.format("Expected an id that is convertible to %s but received %s - [%s]", expectedType, id.getClass(), id);
        }

        @Override
        public Long getNextId(FireflyGraph graph) {
            return graph.db.incrementIdCounter(GLOBAL);
        }

        @Override
        public Long convert(Object id) {
            if (null == id)
                return null;
            else if (id instanceof Long)
                return (Long) id;
            else if (id instanceof Number)
                return ((Number) id).longValue();
            else if (id instanceof String) {
                try {
                    return Long.parseLong((String) id);
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException(createErrorMessage(Long.class, id));
                }
            } else
                throw new IllegalArgumentException(createErrorMessage(Long.class, id));
        }

        @Override
        public boolean allow(Object id) {
            return id instanceof Number || id instanceof String;
        }
    }


}
