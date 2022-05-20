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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.firefly.io.AerospikeConnection.GLOBAL;
import static com.aerospike.firefly.structure.FireflyHelper.readVertex;
import static com.aerospike.firefly.structure.FireflyHelper.writeVertex;
import static com.aerospike.firefly.util.Tokens.UNIMPLEMENTED;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)

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
// THESE TESTS ARE SLOW SO DURING DEVELOPMENT UNCOMMENT THE OPT_OUTS
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest",
        method = "*",
        reason = "Creating another graph on the same cluster with an open transaction causes a locking issue",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphSONTest",
        method = "shouldWriteNormalizedGraphSON",
        reason = "Test assumes integer when IgniteGraph uses longs",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphSONV3D0Test",
        method = "shouldWriteNormalizedGraphSON",
        reason = "Test assumes integer when IgniteGraph uses longs",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphSONV2D0Test",
        method = "shouldWriteNormalizedGraphSON",
        reason = "Test assumes integer when IgniteGrapht uses longs",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphSONV2D0Test",
        method = "shouldWriteNormalizedGraphSON",
        reason = "Test assumes integer when IgniteGraph uses longs",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.util.star.StarGraphTest",
        method = "shouldCopyFromGraphAToGraphB",
        reason = "Creating another graph on the same cluster with an open transaction causes a locking issue",
        computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.TransactionTest",
        method = "*",
        reason = "MAKE ACTIVE LATER",
        computers = {"ALL"})

public class FireflyGraph implements Graph, WrappedGraph<AerospikeConnection> {
    protected final AerospikeConnection db;
    private AtomicBoolean closed = new AtomicBoolean(false);

    private final FireflyGraphFeatures features = new FireflyGraphFeatures();

    private final Configuration configuration;
    protected final FireflyGraph.IdManager<?> vertexIdManager;
    protected final FireflyGraph.IdManager<?> edgeIdManager;
    protected final FireflyGraph.IdManager<?> vertexPropertyIdManager;
    private final FireflyGraphVariables variables;


    protected FireflyGraphComputerView graphComputerView = null;

    protected FireflyGraph(final Configuration conf) {
        this.configuration = conf;
        final String aerospikeHost = conf.get(String.class, ConfigurationHelper.Keys.AEROSPIKE_HOST);
        final Integer aerospikePort = conf.get(Integer.class, ConfigurationHelper.Keys.AEROSPIKE_PORT);
        final String aerospikeNamespace = conf.get(String.class, ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE);
        this.db = AerospikeConnection.connect(aerospikeHost, aerospikePort, aerospikeNamespace);
        vertexPropertyIdManager = new LongIdManager<>(FireflyVertexProperty.class, GLOBAL);
        vertexIdManager = new LongIdManager<>(FireflyVertex.class, GLOBAL);
        edgeIdManager = new LongIdManager<>(FireflyEdge.class, GLOBAL);
        variables = new FireflyGraphVariables(this);
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
        Object idValue = vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        if (null != idValue) {
            if (readVertex(this, idValue) != null)
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
    public Iterator<Vertex> vertices(Object... vertexIds) {
        Iterator<Long> itr;
        List<Long> longs = new ArrayList<>();
        Arrays.stream(vertexIds).forEach(o -> {
            longs.add(((Number) o).longValue());
        });
        if (vertexIds.length != 0)
            itr = longs.iterator();
        else
            itr = (Iterator<Long>) db.readElementIds(FireflyVertex.class);

        return new FireflyVertexIterator(this, itr);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        Iterator<Long> itr;
        List<Long> longs = new ArrayList<>();
        Arrays.stream(edgeIds).forEach(o -> {
            longs.add(((Number) o).longValue());
        });
        if (edgeIds.length != 0)
            itr = longs.iterator();
        else
            itr = (Iterator<Long>) db.readElementIds(FireflyEdge.class);

        return new FireflyEdgeIterator(this, itr);
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
        private final String counterNamespace;
        private final Class<? extends FireflyElement> type;

        public LongIdManager(Class<? extends FireflyElement> type, String counterNamespace) {
            this.counterNamespace = counterNamespace;
            this.type = type;
        }

        private static String createErrorMessage(final Class<?> expectedType, final Object id) {
            return String.format("Expected an id that is convertible to %s but received %s - [%s]", expectedType, id.getClass(), id);
        }

        @Override
        public Long getNextId(FireflyGraph graph) {
            long val = graph.db.incrementIdCounter(GLOBAL);
            graph.db.writeElementId(type, val);
            return val;
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
            return id instanceof Long || id instanceof String;
        }
    }


    public class FireflyGraphFeatures implements Features {

        private final FireflyGraph.FireflyGraphGraphFeatures graphFeatures = new FireflyGraph.FireflyGraphGraphFeatures();
        private final FireflyGraph.FireflyGraphEdgeFeatures edgeFeatures = new FireflyGraph.FireflyGraphEdgeFeatures();
        private final FireflyGraph.FireflyGraphVertexFeatures vertexFeatures = new FireflyGraph.FireflyGraphVertexFeatures();

        private FireflyGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

    }

    public class FireflyGraphVertexFeatures implements Features.VertexFeatures {

        private final FireflyGraph.FireflyGraphVertexPropertyFeatures vertexPropertyFeatures = new FireflyGraph.FireflyGraphVertexPropertyFeatures();

        private FireflyGraphVertexFeatures() {
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return true;
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }
    }

    public class FireflyGraphEdgeFeatures implements Features.EdgeFeatures {

        private FireflyGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return true;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return edgeIdManager.allow(id);
        }
    }

    public class FireflyGraphGraphFeatures implements Features.GraphFeatures {

        private FireflyGraphGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public boolean supportsServiceCall() {
            return true;
        }

    }

    public class FireflyGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

        private FireflyGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsNullPropertyValues() {
            return true;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }
    }










    @Override
    public String toString() {
        return StringFactory.graphString(this, db.toString());
    }


}
