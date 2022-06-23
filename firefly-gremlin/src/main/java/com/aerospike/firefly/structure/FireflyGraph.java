package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.process.computer.FireflyGraphComputerView;
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
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphStepStrategy;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

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
                        .addStrategies(FireflyGraphStepStrategy.instance()));
    }


    protected FireflyGraph(final Configuration conf) {
        this.configuration = conf;
        this.db = AerospikeConnection.connect(conf);
        this.vertexPropertyIdManager = new NumericIdManager<>(FireflyVertexProperty.class, VERTEX_PROPERTY_ID_COUNTER);
        this.vertexIdManager = new NumericIdManager<>(FireflyVertex.class, VERTEX_ID_COUNTER);
        this.edgeIdManager = new NumericIdManager<>(FireflyEdge.class, EDGE_ID_COUNTER);
        this.variables = new FireflyGraphVariables(this);
        this.features = new FireflyGraphFeatures(this);
    }

    public static FireflyGraph open(Configuration conf) {
        return new FireflyGraph(conf);
    }



    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return this.db.getIndexedKeys((Class<? extends FireflyElement>) elementClass);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return null == null ? Collections.emptySet() : this.db.getIndexedKeys((Class<? extends FireflyElement>) elementClass);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
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
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Iterator i = IteratorUtils.asIterator(keyValues);
        while (i.hasNext()) {
            i.next();
            FireflyHelper.validatePropertyValue(i.next());
        }
        if (ElementHelper.getIdValue(keyValues).isPresent())
            if (!features.vertex().supportsUserSuppliedIds())
                throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        FireflyId idValue = FireflyId.createFromKeyValuesOrManager(this, FireflyVertex.class, keyValues);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        writeVertex(this, idValue, label);
        Vertex vertex = new FireflyVertex(idValue,label,this);
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
    public Iterator<Vertex> vertices(Object... vertexIdsOrVertices) {
        Iterator<Long> itr;
        List<Long> longs = new ArrayList<>();
        IteratorUtils.asIterator(vertexIdsOrVertices).forEachRemaining(o -> {
            longs.add(vertexIdManager.convert(o));
        });
        if (vertexIdsOrVertices.length != 0)
            if (!IteratorUtils.allMatch(IteratorUtils.map(longs.iterator(), longId -> FireflyId.of(db,FireflyVertex.class, longId)), db::vertexExists))
                throw new NoSuchElementException("vertex could not be found and edge could not be created");
        if (vertexIdsOrVertices.length != 0)
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


    @Override
    public String toString() {
        return StringFactory.graphString(this, db.toString());
    }

}
