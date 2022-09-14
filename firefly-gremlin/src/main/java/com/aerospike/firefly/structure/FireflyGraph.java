package com.aerospike.firefly.structure;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.GraphFactory;
import com.aerospike.firefly.process.computer.FireflyGraphComputerView;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphCountStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphStepStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyTraversalCacheStrategy;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.IdManager;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.structure.iterator.FireflyEdgeIterator;
import com.aerospike.firefly.structure.iterator.FireflyVertexIterator;
import com.aerospike.firefly.structure.util.FireflyHelper;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.firefly.util.Tokens.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn("com.aerospike.firefly.structure.process.CustomGraphProcessStandardTest")

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.TransactionTest", method = "*", reason = "MAKE ACTIVE WHEN TRANSACTIONS IMPLEMENTED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest", method = "*", reason = "MAKE ACTIVE WHEN PARALLEL SCAN RESULT ITERATOR IMPLEMENTED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest", method = "*", reason = "THROW PROPER EXCEPTIONS WHEN DESIRED FINAL FEATURE SET IS DETERMINED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "*", reason = "THESE TESTS READ AND WRITE FROM 2 GRAPHS, BUT WHEN BACKED BY THE SAME AEROSPIKE INSTANCE, PRODUCE INVALID RESULTS", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest", method = "*", reason = "CURRENTLY DO NOT WORK, NEED TO FIX AND ENABLE", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldEvaluateConnectivityPatterns", reason = "This test fails due to caching.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest", method = "shouldNotGetConcurrentModificationException", reason = "Concurrent writes are not supported in star packed data model.", computers = {"ALL"})

// Opt out of grateful since they are by far the slowest tests.
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_both_both_count", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX3X_count", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "g_V_repeatXoutX_timesX8X_count", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest", method = "g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest", method = "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest", method = "g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest", method = "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest", method = "g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest", method = "g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__b_followedBy_c__c_writtenBy_d__whereXd_neqXaXXX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest", method = "g_V_hasLabelXsongsX_matchXa_name_b__a_performances_cX_selectXb_cX_count", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest", method = "g_V_matchXa_followedBy_count_isXgtX10XX_b__a_0followedBy_count_isXgtX10XX_bX_count", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest", method = "g_V_matchXa_hasXsong_name_sunshineX__a_mapX0followedBy_weight_meanX_b__a_0followedBy_c__c_filterXweight_whereXgteXbXXX_outV_dX_selectXdX_byXnameX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategyProcessTest", method = "shouldHandleRangeSteps", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategyProcessTest", method = "shouldSeedLocalSample", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategyProcessTest", method = "shouldSeedGlobalSample", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest", method = "playlistPaths", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest", method = "classicRecommendation", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest", method = "g_V_hasXsong_name_OHBOYX_outXfollowedByX_outXfollowedByX_order_byXperformancesX_byXsongType_descX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest", method = "g_V_hasLabelXsongX_order_byXperformances_descX_byXnameX_rangeX110_120X_name", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest", method = "grateful_V_out_out_profile", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest", method = "grateful_V_out_out_profileXmetricsX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest", method = "g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest", method = "g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest", method = "g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest", method = "g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX", reason = "Grateful graph takes long to load.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest", method = "g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX", reason = "Grateful graph takes long to load.", computers = {"ALL"})

// THESE TESTS ARE SLOW SO DURING DEVELOPMENT UNCOMMENT THE OPT_OUTS
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest", method = "*", reason = "MAKE ACTIVE LATER", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest", method = "*", reason = "MAKE ACTIVE LATER", computers = {"ALL"})

public abstract class FireflyGraph implements Graph, WrappedGraph<AerospikeConnection> {
    public static String FIREFLY_VERSION = "0.3.0-SNAPSHOT";

    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraph.class);

    public final IdManager<Long> vertexIdManager;
    public final IdManager<Long> edgeIdManager;
    public final IdManager<Long> vertexPropertyIdManager;
    protected final AerospikeConnection db;
    private final FireflyGraphFeatures features;
    private final Configuration configuration;
    private final FireflyGraphVariables variables;

    protected FireflyGraphComputerView graphComputerView = null;
    private AtomicBoolean closed = new AtomicBoolean(false);

    static {
        TraversalStrategies.GlobalCache.registerStrategies(
                FireflyGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                        .addStrategies(FireflyGraphStepStrategy.instance())
                        .addStrategies(OptionsStrategy.build().create()));
    }

    protected FireflyGraph(final Configuration conf) {
        this(AerospikeConnection.connect(conf), conf);
    }


    protected FireflyGraph(final AerospikeConnection db, final Configuration conf) {
        this.configuration = conf;
        db.createGraphIndexes();
        this.db = db;
        this.vertexPropertyIdManager = new NumericIdManager<>(FireflyVertexProperty.class, VERTEX_PROPERTY_ID_COUNTER);
        this.vertexIdManager = new NumericIdManager<>(FireflyVertex.class, VERTEX_ID_COUNTER);
        this.edgeIdManager = new NumericIdManager<>(FireflyEdge.class, EDGE_ID_COUNTER);
        this.variables = new FireflyGraphVariables(this);
        this.features = new FireflyGraphFeatures(this);

        if (Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ENABLE_FAST_COUNT_STRATEGY, configuration))) {
            //@todo
            // this can be supported by querying all nodes and dividing by replication factor,
            // but since there is another known issue with Info lagging, and querying all nodes would produce results
            // at different moments in time, perhaps we should wait for another official global countRecords(set_name) api
            if (db.getClient().getNodes().length > 1)
                throw new RuntimeException("fast count not supported for multi node");
            TraversalStrategies.GlobalCache.registerStrategies(
                    FireflyGraph.class,
                    TraversalStrategies.GlobalCache.getStrategies(FireflyGraph.class).clone()
                            .addStrategies(FireflyGraphCountStrategy.instance()));
        }

        if (Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ENABLE_SUBGRAPH_CACHE_STRATEGY, configuration))) {
            TraversalStrategies.GlobalCache.registerStrategies(
                    FireflyGraph.class,
                    TraversalStrategies.GlobalCache.getStrategies(FireflyGraph.class).clone()
                            .addStrategies(FireflyTraversalCacheStrategy.instance()));
        }
    }

    public static FireflyGraph open(final Configuration conf) {
        return GraphFactory.createGraph(AerospikeConnection.connect(conf), conf);
    }
    public static final String GETDATAMODELNAME = "getDataModelName";
    public static final String DATAMODELVERSION = "dataModelVersion";
    public static ComparableVersion dataModelVersion(){
        return new ComparableVersion(FIREFLY_VERSION);
    };

    public abstract String getDataModel();

    // Vertex functions.
    protected abstract Iterator<Long> scanAllVertices();

    public abstract FireflyVertex writeVertex(final FireflyId idValue, final String label, final List<Map.Entry<String, Object>> properties);

    public abstract FireflyVertex readVertex(final FireflyId idValue);

    public abstract FireflyVertex vertexFromRecord(final KeyRecord record);

    public abstract boolean vertexExists(final FireflyId idValue);

    // Edge functions.
    public abstract FireflyEdge writeEdge(final FireflyId edgeId, final String label, final List<Map.Entry<String, Object>> properties, final FireflyVertex inVertex, final FireflyVertex outVertex);

    public abstract FireflyEdge readEdge(final FireflyId edgeId);

    public abstract FireflyEdge edgeFromRecord(final KeyRecord record);

    public abstract boolean edgeExists(final FireflyId idValue);

    // Graph variable functions.
    public abstract Set<String> readGraphVariableKeys();

    public abstract <V> void writeGraphVariable(final String key, final V value);

    public abstract <V> V readGraphVariable(final String key);

    public abstract void removeGraphVariable(final String key);

    // Vertex property and property functions.
    public abstract void removeProperty(final FireflyElement element, final String key);

    public abstract <V> Property<V> writeProperty(final FireflyElement element, final String key, final V value);

    public abstract <V> Map<String, Property<V>> readProperties(final FireflyElement element);

    public abstract <V> Property<V> readProperty(final FireflyElement element, final String key);

    public abstract <V> FireflyVertexProperty<V> writeVertexProperty(final FireflyId vertexPropertyId, final FireflyVertex vertex, final String key, final V value);

    // Counting functions.
    public abstract long getVertexCount();

    public abstract long getEdgeCount();

    // Index functions.
    public abstract Iterator<FireflyEdge> queryEdgePropertyStringMatchIndex(final String key, final Object value);

    public abstract Iterator<FireflyEdge> queryEdgePropertyNumericMatchIndex(final String key, final P<?> predicate);

    public abstract Iterator<FireflyEdge> queryEdgePropertyNumericRangeIndex(final String key, final P<?> predicate);

    public abstract Iterator<FireflyVertex> queryVertexLabelStringIndex(final Object value);

    public abstract Iterator<FireflyEdge> queryEdgeLabelStringIndex(final Object value);

    public abstract Iterator<FireflyVertexProperty> queryVertexPropertyStringIndex(final String key, final Object value);

    public abstract Iterator<FireflyVertexProperty> queryVertexPropertyNumberMatchIndex(final String key, final P<?> predicate);

    public abstract Iterator<FireflyVertexProperty> queryVertexPropertyNumberRangeIndex(final String key, final P<?> predicate);

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

        if (ElementHelper.getIdValue(keyValues).isPresent()) {
            try {
                NumericIdManager.convert(idValue.value());
            } catch (IllegalArgumentException ignored) {
                // Invalid type for id.
                throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            }
            if (vertexExists(idValue)) {

                throw Graph.Exceptions.vertexWithIdAlreadyExists(idValue.value());
            }
        } else {
            while (vertexExists(idValue)) {
                idValue = FireflyId.createFromManager(this, FireflyVertex.class);
            }
        }

        // Get label from key value pairs.
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        // Write fully qualified Vertex.

        final List<Map.Entry<String, Object>> properties = convertFullyQualified(this.features().vertex().supportsNullPropertyValues(), keyValues);
        return writeVertex(idValue, label, properties);
    }

    public List<Map.Entry<String, Object>> convertFullyQualified(final boolean supportNullProperties, final Object... propertyKeyValues) {
        List<Map.Entry<String, Object>> properties = new ArrayList<>();
        for (int i = 0; i < propertyKeyValues.length; i += 2) {
            // Skip label and user supplied id key/value pairs.
            // Also, if we do not support null and it is null, skip as well. We don't need to explicitly remove
            // null properties here since we are doing a fully qualified write and will overwrite regardless.
            if (propertyKeyValues[i].equals(T.id) || propertyKeyValues[i].equals(T.label) || (!supportNullProperties && propertyKeyValues[i + 1] == null)) {
                continue;
            }
            final String key = (String) propertyKeyValues[i];
            final Object value = propertyKeyValues[i + 1];

            // Key cannot be empty, must be non-empty String.
            if (key.isEmpty()) {
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
            }

            // If cardinality is single we must only retain the final item.
            if (this.features().vertex().getCardinality(key).equals(VertexProperty.Cardinality.single)) {
                properties = properties.stream().filter(p -> !key.equals(p.getKey())).collect(Collectors.toList());
            }
            properties.add(new AbstractMap.SimpleEntry<>(key, value));
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
        final List<Long> longs = Arrays.stream(vertexIdsOrVertices).map(NumericIdManager::convert).collect(Collectors.toList());

        // If vertex id count is > 0 && not all vertices exist, then we have a no such element exception.
        if (!longs.isEmpty() && !longs.stream().map(id -> FireflyId.of(FireflyVertex.class, id)).allMatch(this::vertexExists)) {
            throw new NoSuchElementException("vertex could not be found and edge could not be created");
        }

        // Create vertex iterator with graph and vertex id iterator.
        // If there are vertexIds present use them, otherwise read from database.
        return new FireflyVertexIterator(this, longs.isEmpty() ? scanAllVertices() : longs.iterator());
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
        LOG.info("Closing FireflyGraph.");
        this.closed.set(true);
        TraversalStrategies.GlobalCache
                .getStrategies(FireflyGraph.class)
                .removeStrategies(FireflyTraversalCacheStrategy.class);
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
