package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.process.computer.FireflyGraphComputerView;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphCountStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphStepStrategy;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.IdManager;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.structure.iterator.FireflyEdgeIterator;
import com.aerospike.firefly.structure.iterator.FireflyVertexIterator;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.firefly.structure.util.FireflyHelper.writeFullyQualifiedVertex;
import static com.aerospike.firefly.structure.util.FireflyHelper.writeVertex;
import static com.aerospike.firefly.util.Tokens.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)


@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.TransactionTest",
        method = "*",
        reason = "MAKE ACTIVE WHEN TRANSACTIONS IMPLEMENTED",
        computers = {"ALL"})

@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest",
        method = "*",
        reason = "MAKE ACTIVE WHEN PARALLEL SCAN RESULT ITERATOR IMPLEMENTED",
        computers = {"ALL"})

@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest",
        method = "*",
        reason = "THROW PROPER EXCEPTIONS WHEN DESIRED FINAL FEATURE SET IS DETERMINED",
        computers = {"ALL"})


@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest",
        method = "*",
        reason = "THESE TESTS READ AND WRITE FROM 2 GRAPHS, BUT WHEN BACKED BY THE SAME AEROSPIKE INSTANCE, PRODUCE INVALID RESULTS",
        computers = {"ALL"})

@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest",
        method = "*",
        reason = "CURRENTLY DO NOT WORK, NEED TO FIX AND ENABLE",
        computers = {"ALL"})


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


public class FireflyGraph implements Graph, WrappedGraph<AerospikeConnection> {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraph.class);
    private final AerospikeConnection db;
    private AtomicBoolean closed = new AtomicBoolean(false);

    private final FireflyGraphFeatures features;

    private final Configuration configuration;

    public final IdManager<Long> vertexIdManager;
    public final IdManager<Long> edgeIdManager;

    public final IdManager<Long> vertexPropertyIdManager;
    private final FireflyGraphVariables variables;


    protected FireflyGraphComputerView graphComputerView = null;


    static {
        TraversalStrategies.GlobalCache.registerStrategies(
                FireflyGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                        .addStrategies(FireflyGraphStepStrategy.instance())
                        .addStrategies(FireflyGraphCountStrategy.instance()));
    }


    protected FireflyGraph(final Configuration conf) {
        this(AerospikeConnection.connect(conf), conf);
    }

    protected FireflyGraph(AerospikeConnection db, final Configuration conf) {
        db.createGraphIndexes();
        this.configuration = conf;
        this.db = db;
        this.vertexPropertyIdManager = new NumericIdManager<>(FireflyVertexProperty.class, VERTEX_PROPERTY_ID_COUNTER);
        this.vertexIdManager = new NumericIdManager<>(FireflyVertex.class, VERTEX_ID_COUNTER);
        this.edgeIdManager = new NumericIdManager<>(FireflyEdge.class, EDGE_ID_COUNTER);
        this.variables = new FireflyGraphVariables(this);
        this.features = new FireflyGraphFeatures(this);
    }

    public static FireflyGraph open(Configuration conf) {
        return new FireflyGraph(conf);
    }


    @Override
    public AerospikeConnection getBaseGraph() {
        return db;
    }


    @Override
    public Features features() {
        return features;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        // Validate key value pairs are valid for TinkerPop.
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        // Validate key value pairs are valid for Firefly.
        final Iterator i = IteratorUtils.asIterator(keyValues);
        while (i.hasNext()) {
            i.next();
            FireflyHelper.validatePropertyValue(i.next());
        }

        // If a user-supplied id is provided and it is not supported, throw exception.
        if (ElementHelper.getIdValue(keyValues).isPresent() && !features.vertex().supportsUserSuppliedIds())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        // Create a new id or use the provided user-supplied id (if present and supported).
        FireflyId idValue = FireflyId.createFromKeyValuesOrManager(this, FireflyVertex.class, keyValues);

        if (ElementHelper.getIdValue(keyValues).isPresent() && db.vertexExists(idValue)) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(idValue.value());
        } else {
            while (db.vertexExists(idValue)) {
                idValue = FireflyId.createFromManager(this, FireflyVertex.class);
            }
        }

        // Get label from key value pairs.
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        // Write fully qualified Vertex.
        final List<Map.Entry<String, Object>> properties =
                convertFullyQualified(this.features().vertex().supportsNullPropertyValues(), keyValues);
        writeFullyQualifiedVertex(this, idValue, label, properties);

        // Return FireflyVertex.
        return new FireflyVertex(idValue, label, this);
    }

    public List<Map.Entry<String, Object>> convertFullyQualified(final boolean supportNullProperties, final Object... propertyKeyValues) {
        final List<Map.Entry<String, Object>> properties = new ArrayList<>();
        for (int i = 0; i < propertyKeyValues.length; i += 2) {
            if (propertyKeyValues[i].equals(T.id) || propertyKeyValues[i].equals(T.label) || (!supportNullProperties && propertyKeyValues[i + 1] == null)) {
                continue;
            }
            properties.add(new AbstractMap.SimpleEntry<>((String) propertyKeyValues[i], propertyKeyValues[i + 1]));
        }
        return properties;
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
    public Iterator<Vertex> vertices(Object... vertexIdsOrVertices) {
        // Convert vertexIds to longs
        final List<Long> longs = Arrays.stream(vertexIdsOrVertices).
                map(NumericIdManager::convert).collect(Collectors.toList());

        // If vertex id count is > 0 && not all vertices exist, then we have a no such element exception.
        if (!longs.isEmpty() && !longs.stream().map(
                id -> FireflyId.of(FireflyVertex.class, id)).allMatch(db::vertexExists)) {
            throw new NoSuchElementException("vertex could not be found and edge could not be created");
        }

        // Create vertex iterator with graph and vertex id iterator.
        // If there are vertexIds present use them, otherwise read from database.
        return new FireflyVertexIterator(this, longs.isEmpty() ?
                db.readElementIds(FireflyVertex.class) :
                longs.iterator());
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        // Create edge iterator with graph and edge id iterator.
        // If there are edgeIds present, convert them to an iterator of Longs, otherwise read edges from database.
        return new FireflyEdgeIterator(this,
                (edgeIds.length == 0) ?
                        db.readElementIds(FireflyEdge.class) :
                        Arrays.stream(edgeIds).map(NumericIdManager::convert).collect(Collectors.toList()).iterator());
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


    @Override
    public String toString() {
        return StringFactory.graphString(this, db.toString());
    }

}
