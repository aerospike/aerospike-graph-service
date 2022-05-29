package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.process.computer.FireflyGraphComputerView;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.firefly.structure.FireflyHelper.readVertex;
import static com.aerospike.firefly.structure.FireflyHelper.writeVertex;
import static com.aerospike.firefly.util.Tokens.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)

// THESE TESTS ARE SLOW SO DURING DEVELOPMENT UNCOMMENT THE OPT_OUTS
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest",
        method = "*",
        reason = "MAKE ACTIVE LATER",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest",
        method = "*",
        reason = "MAKE ACTIVE LATER",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.TransactionTest",
        method = "*",
        reason = "MAKE ACTIVE LATER",
        computers = {"ALL"})

public class FireflyGraph implements Graph, WrappedGraph<AerospikeConnection> {
    protected final AerospikeConnection db;
    private AtomicBoolean closed = new AtomicBoolean(false);

    private final FireflyGraphFeatures features;

    private final Configuration configuration;

    protected final FireflyGraph.IdManager<Long> vertexIdManager;
    protected final FireflyGraph.IdManager<Long> edgeIdManager;

    protected final FireflyGraph.IdManager<Long> vertexPropertyIdManager;
    private final FireflyGraphVariables variables;


    protected FireflyGraphComputerView graphComputerView = null;

    protected FireflyGraph(final Configuration conf) {
        this.configuration = conf;
        final String aerospikeHost = conf.get(String.class, ConfigurationHelper.Keys.AEROSPIKE_HOST);
        final Integer aerospikePort = conf.get(Integer.class, ConfigurationHelper.Keys.AEROSPIKE_PORT);
        final String aerospikeNamespace = conf.get(String.class, ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE);
        this.db = AerospikeConnection.connect(aerospikeHost, aerospikePort, aerospikeNamespace);
        vertexPropertyIdManager = new LongIdManager<>(FireflyVertexProperty.class, VERTEX_PROPERTY_ID_COUNTER);
        vertexIdManager = new LongIdManager<>(FireflyVertex.class, VERTEX_ID_COUNTER);
        edgeIdManager = new LongIdManager<>(FireflyEdge.class, EDGE_ID_COUNTER);
        variables = new FireflyGraphVariables(this);
        this.features = new FireflyGraphFeatures(this);
    }

    public static FireflyGraph open(Configuration conf) {
        return new FireflyGraph(conf);
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

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Iterator i = IteratorUtils.asIterator(keyValues);
        while (i.hasNext()) {
            i.next();
            FireflyHelper.validatePropertyValue(i.next());
        }
        Object idValue = vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        if (null != idValue) {
            if(db.vertexExists(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = vertexIdManager.getNextId(this);
        }
        //@todo performance: dont reread
        writeVertex(this, idValue, label);
        Vertex vertex = readVertex(this, idValue);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        return vertex;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new UnsupportedOperationException(UNIMPLEMENTED);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException(UNIMPLEMENTED);
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIdsOrVerticies) {
        Iterator<Long> itr;
        List<Long> longs = new ArrayList<>();
        IteratorUtils.asIterator(vertexIdsOrVerticies).forEachRemaining(o -> {
            longs.add(vertexIdManager.convert(o));
        });
        if (vertexIdsOrVerticies.length != 0)
            if(!IteratorUtils.allMatch(longs.iterator(), db::vertexExists))
                throw new NoSuchElementException("vertex not found");
        if (vertexIdsOrVerticies.length != 0)
            itr = longs.iterator();
        else
            itr = (Iterator<Long>) db.readElementIds(FireflyVertex.class);

        return new FireflyVertexIterator(this, itr);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        Iterator<Long> itr;
        List<Long> longs = new ArrayList<>();
        IteratorUtils.asIterator(edgeIds).forEachRemaining(o -> {
            longs.add(edgeIdManager.convert(o));
        });
        if (edgeIds.length != 0)
            itr = longs.iterator();
        else
            itr = (Iterator<Long>) db.readElementIds(FireflyEdge.class);

        return new FireflyEdgeIterator(this, itr);
    }

    @Override
    public Transaction tx() {
        throw new UnsupportedOperationException(UNIMPLEMENTED);
    }

    @Override
    public void close() {
        this.closed.set(true);
        this.db.close();
    }

    @Override
    public Variables variables() {
        return variables;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }


    public static class LongIdManager<T extends FireflyElement> implements FireflyGraph.IdManager<Long> {

        /**
         * Manages identifiers of type {@code Long}. Will convert any class that extends from {@link Number} to a
         * {@link Long} and will also attempt to convert {@code String} values
         */
        private final String counterName;
        private final Class<? extends FireflyElement> type;

        public LongIdManager(Class<? extends FireflyElement> type, String counterName) {
            this.counterName = counterName;
            this.type = type;
        }

        private static String createErrorMessage(final Class<?> expectedType, final Object id) {
            return String.format("Expected an id that is convertible to %s but received %s - [%s]", expectedType, id.getClass(), id);
        }

        @Override
        public Long getNextId(FireflyGraph graph) {
            return graph.db.incrementAndGetIdCounter(this.counterName);
        }

        @Override
        public Long convert(Object id) {
            if (null == id)
                return null;
            else if (Element.class.isAssignableFrom(id.getClass()))
                return (Long) ((Element) id).id();
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
            return id instanceof Long;
        }
    }


    @Override
    public String toString() {
        return StringFactory.graphString(this, db.toString());
    }

}
