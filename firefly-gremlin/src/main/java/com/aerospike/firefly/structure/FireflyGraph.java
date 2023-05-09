package com.aerospike.firefly.structure;

import ch.qos.logback.classic.Level;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.ReadContext;
import com.aerospike.firefly.io.impl.GraphFactory;
import com.aerospike.firefly.io.impl.relational.RelationalEdge;
import com.aerospike.firefly.process.call.FireflyBulkLoaderServiceFactory;
import com.aerospike.firefly.process.call.FireflyMetadataServiceFactory;
import com.aerospike.firefly.process.computer.FireflyGraphComputerView;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyContentionHandlingStrategy;
import com.aerospike.firefly.structure.id.BufferedNumericIdManager;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.IdManager;
import com.aerospike.firefly.structure.id.RecyclingBufferedNumericIdManager;
import com.aerospike.firefly.structure.iterator.FireflyBatchElementIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.util.FireflyGraphSummaryVertex;
import com.aerospike.firefly.structure.util.FireflyHelper;
import com.aerospike.firefly.structure.util.FireflyMetadataTask;
import com.aerospike.firefly.structure.util.FireflyMetadataVertex;
import com.aerospike.firefly.structure.util.FireflyGraphSummaryUpdater;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.LoggerUtil;
import com.aerospike.firefly.util.WarmupUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static com.aerospike.firefly.util.Tokens.EDGE_RECYCLED_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.EDGE_UNIQUE_ID_COUNTER;
import static com.aerospike.firefly.structure.util.FireflyGraphSummaryVertex.GRAPH_SUMMARY_VERTEX;
import static com.aerospike.firefly.util.Tokens.UNIMPLEMENTED;
import static com.aerospike.firefly.util.Tokens.VERTEX_ID_COUNTER;
import static com.aerospike.firefly.util.Tokens.VERTEX_PROPERTY_ID_COUNTER;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn("com.aerospike.firefly.structure.process.CustomGraphProcessStandardTest")

// Tests that require lambda support.
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV1d0Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV2d0Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV3d0Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoV1d0Test", method = "shouldSerializePathAsDetached", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoV3d0Test", method = "shouldSerializePathAsDetached", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.TransactionTest", method = "*", reason = "MAKE ACTIVE WHEN TRANSACTIONS IMPLEMENTED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest", method = "*", reason = "MAKE ACTIVE WHEN PARALLEL SCAN RESULT ITERATOR IMPLEMENTED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest", method = "*", reason = "THROW PROPER EXCEPTIONS WHEN DESIRED FINAL FEATURE SET IS DETERMINED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "*", reason = "THESE TESTS READ AND WRITE FROM 2 GRAPHS, BUT WHEN BACKED BY THE SAME AEROSPIKE INSTANCE, PRODUCE INVALID RESULTS", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest", method = "*", reason = "CURRENTLY DO NOT WORK, NEED TO FIX AND ENABLE", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldEvaluateConnectivityPatterns", reason = "This test fails due to caching.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.VertexTest$BasicVertexTest", method = "shouldNotGetConcurrentModificationException", reason = "Concurrent writes are not supported in star packed data model.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.VertexPropertyTest$VertexPropertyRemoval", method = "shouldRemoveMultiPropertiesWhenVerticesAreRemoved", reason = "Replaced in TestAerospikeGraphIntegration with cache-friendly implementation.", computers = {"ALL"})

// Firefly does not support Float ids
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentation", reason = "Firefly does not support Float ids", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentations", reason = "Firefly does not support Float ids", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentations", reason = "Firefly does not support Float ids", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentation", reason = "Firefly does not support Float ids", computers = {"ALL"})

public abstract class FireflyGraph implements Graph, WrappedGraph<AerospikeConnection> {
    public static final String FIREFLY_CONFIGURATION_VARIABLE_NAME = "FIREFLY_CONFIGURATION";
    public static final String FIREFLY_WARMUP_VARIABLE_NAME = "FIREFLY_WARMUP";
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraph.class);
    public static String FIREFLY_VERSION = "0.7.0-SNAPSHOT";
    public final AtomicBoolean closed = new AtomicBoolean(false);
    private final Timer fireflyCardinalityMetadataTask = new Timer(true);
    private final Timer fireflyIndexMetadataTask = new Timer(true);
    private final FireflyGraphFeatures features;
    private final Configuration configuration;
    public static String VP_INDEX_PREFIX = "VP";
    public static String EP_INDEX_PREFIX = "EP";
    private final FireflyGraphVariables variables;
    protected final AerospikeConnection db;
    private final FireflyIdFactory idFactory;
    protected FireflyGraphComputerView graphComputerView = null;
    public final IdManager<Long> vertexIdManager;
    public final IdManager<byte[]> edgeIdManager;
    public final IdManager<Long> vertexPropertyIdManager;
    public FireflyCardinalityMetadata fireflyCardinalityMetadata = null;
    public FireflyIndexMetadata fireflyIndexMetadata = null;
    public FireflyGraphSummaryUpdater fireflySummaryUpdater = null;
    private final ServiceRegistry serviceRegistry = new ServiceRegistry();

    static {
        synchronized (TraversalStrategies.GlobalCache.class) {
            TraversalStrategies.GlobalCache.registerStrategies(
                    FireflyGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                            .addStrategies(new FireflyContentionHandlingStrategy())
                            .addStrategies(OptionsStrategy.build().create()));
        }
    }

    protected FireflyGraph(final Configuration conf) {
        this(AerospikeConnection.connect(conf), conf);
    }


    protected FireflyGraph(final AerospikeConnection db, final Configuration conf) {
        this.configuration = conf;
        db.createGraphIndexes();
        this.db = db;
        this.idFactory = db.getIdFactory();

        this.vertexPropertyIdManager = new BufferedNumericIdManager(VERTEX_PROPERTY_ID_COUNTER,
                Long.parseLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.PROPERTY_ID_BUFFER_SIZE, configuration)), false);
        this.vertexIdManager = new BufferedNumericIdManager(VERTEX_ID_COUNTER,
                Long.parseLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.VERTEX_ID_BUFFER_SIZE, configuration)), true);
        this.edgeIdManager = new RecyclingBufferedNumericIdManager(EDGE_RECYCLED_ID_COUNTER, EDGE_UNIQUE_ID_COUNTER,
                Long.parseLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE, configuration)), false);
        this.variables = new FireflyGraphVariables(this);
        this.features = new FireflyGraphFeatures(this);

        // Create index metadata background task that will populate indexes for the named graph on the fly.
        fireflyIndexMetadata = new FireflyIndexMetadata(db);
        final TimerTask indexMetadataTimerTask = new FireflyMetadataTask(fireflyIndexMetadata);
        fireflyIndexMetadataTask.schedule(indexMetadataTimerTask, 0, db.INDEX_METADATA_UPDATE_FREQUENCY);

        // Grab user defined vertex property indexes from the configuration and create them.
        final List<String> vertexPropertyIndexes = ConfigurationHelper.getOrDefaultList(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES, configuration);
        createIndexes(FireflyVertex.class, db.VERTEX_PROPERTY_NAME_TO_VALUE, db.getVpIndexPrefix(), vertexPropertyIndexes);

        // Grab user defined edge property indexes from the configuration and create them.
        final List<String> edgePropertyIndexes = ConfigurationHelper.getOrDefaultList(ConfigurationHelper.Keys.EDGE_PROPERTY_INDEXES, configuration);
        if (edgePropertyIndexes != null && !edgePropertyIndexes.isEmpty()) {
            // TODO: Edge indexes.
            throw new RuntimeException("Edge property indexes are not currently supported.");
        }
        createIndexes(FireflyEdge.class, db.PROPERTIES, db.getEpIndexPrefix(), edgePropertyIndexes);

        // Create cardinality metadata background task that will populate cardinality for the named graph on the fly.
        fireflyCardinalityMetadata = new FireflyCardinalityMetadata(db, db.V_LABEL_INDEX, db.E_LABEL_INDEX, fireflyIndexMetadata);
        final TimerTask cardinalityMetadataTimerTask = new FireflyMetadataTask(fireflyCardinalityMetadata);
        fireflyCardinalityMetadataTask.schedule(cardinalityMetadataTimerTask, 0, db.CARDINALITY_METADATA_UPDATE_FREQUENCY);
        fireflySummaryUpdater = new FireflyGraphSummaryUpdater(db);
        serviceRegistry.registerService(new FireflyMetadataServiceFactory(this));
        serviceRegistry.registerService(new FireflyBulkLoaderServiceFactory());
    }

    public static FireflyGraph open(final Configuration conf) {
        try {
            final Level logLevel = Level.toLevel(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.LOG_LEVEL, conf));
            LoggerUtil.setLogLevel(logLevel);
        } catch (Exception e) {
            LOG.warn("Failed to set log level {}", e.getMessage());
        }
        try {
            final Runtime javaRuntime = Runtime.getRuntime();
            LOG.info("Java Runtime: {} available processors.", javaRuntime.availableProcessors());
            LOG.info("Java Runtime: {} MB max memory.", javaRuntime.maxMemory() / (1024 * 1024));
            LOG.info("Java Runtime: {} MB total memory.", javaRuntime.totalMemory() / (1024 * 1024));
            LOG.info("Java Runtime: {} MB free memory.", javaRuntime.freeMemory() / (1024 * 1024));
            LOG.info("JVM Vendor: {}.", System.getProperty("java.vm.vendor"));
            LOG.info("JVM Specification Vendor: {}.", System.getProperty("java.vm.specification.vendor"));
            LOG.info("Java Specification Version: {}.", System.getProperty("java.specification.version"));
            LOG.info("JVM Runtime: {}.", System.getProperty("java.runtime.name"));
            LOG.info("JVM Runtime Version: {}.", System.getProperty("java.runtime.version"));
            LOG.info("Firefly configuration: {}.", conf);
            LOG.info("Starting Aerospike Firefly v{}.", FIREFLY_VERSION.replace("-SNAPSHOT", ""));
            if (Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AUTO_PRE_HEAT, conf)))
                WarmupUtil.create(conf).preheat(WarmupUtil.passes);
            return GraphFactory.createGraph(AerospikeConnection.connect(conf), conf);
        } catch (Exception e) {
            LOG.error("=================== FAILED TO START FIREFLY GRAPH ===================");
            LOG.error("========== Firefly failing to start is usually a result of an incorrect configuration.");
            LOG.error("========== Verify that the Aerospike IP and port are correct.");
            LOG.error("========== See Error message for more details:", e);

            // Signal to gremlin-server to shut down.
            System.exit(1);

            // Required to compile.
            return null;
        }
    }

    public static final String GETDATAMODELNAME = "getDataModelName";
    public static final String DATAMODELVERSION = "dataModelVersion";

    public static ComparableVersion dataModelVersion() {
        return new ComparableVersion(FIREFLY_VERSION);
    }

    /**
     * Return the FireflyIdFactory
     *
     * @return FireflyIdFactory
     */
    public FireflyIdFactory getIdFactory() {
        return idFactory;
    }

    public abstract String getDataModel();

    // Vertex functions.
    protected abstract Iterator<FireflyId> scanAllVertices();

    public abstract FireflyVertex writeVertex(final FireflyId idValue, final String label, final List<Map.Entry<String, Object>> properties);

    public abstract void bulkWriteEdgesToVertexCache(final FireflyId vertexId, final Direction direction,
                                                     final List<Value> edgeIds, final String edgeLabel);

    public abstract FireflyVertex readVertex(final FireflyId idValue);

    public abstract List<FireflyVertex> readVertices(final List<HasContainer> hasContainers, final List<FireflyId> vertexIds);

    public abstract FireflyVertex vertexFromRecord(final KeyRecord record);

    // Edge functions.
    public abstract FireflyEdge writeEdge(final FireflyId edgeId, final String label, final List<Map.Entry<String, Object>> properties, final FireflyVertex inVertex, final FireflyVertex outVertex);

    public abstract void bulkWriteEdge(final byte[] edgeId, final String label,
                                       final List<Map.Entry<String, Object>> properties, final Object inVertexId,
                                       final Object outVertexId, final boolean inVSupernode, final boolean outVSupernode);

    public abstract void removeEdgeById(final FireflyId edgeId);

    public abstract List<FireflyEdge> readEdges(final List<HasContainer> hasContainers, final List<FireflyId> edgeIds);

    // Graph variable functions.
    public abstract Set<String> readGraphVariableKeys();

    public abstract <V> void writeGraphVariable(final String key, final V value);

    public abstract <V> V readGraphVariable(final String key);

    public abstract void removeGraphVariable(final String key);

    public abstract <V> FireflyVertexProperty<V> writeVertexProperty(
            final FireflyId vertexPropertyId, final FireflyVertex vertex, final String key, final V value, final Object... keyValues);

    // Counting functions.
    public abstract long getVertexCount(final Expression expression);

    public abstract long getEdgeCount();

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
        final Iterator i = FireflyCloseableIteratorUtils.asIterator(keyValues);
        while (i.hasNext()) {
            i.next();
            FireflyHelper.validatePropertyValue(i.next());
        }

        // If a user-supplied id is provided and it is not supported, throw exception.
        if (ElementHelper.getIdValue(keyValues).isPresent() && !features.vertex().supportsUserSuppliedIds())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        final List<Map.Entry<String, Object>> properties = convertFullyQualified(
                this.features().vertex().supportsNullPropertyValues(), keyValues);

        // Create a new id or use the provided user-supplied id (if present and supported).
        FireflyId idValue;
        if (ElementHelper.getIdValue(keyValues).isEmpty()) {
            idValue = getIdFactory().createFromManager(this, FireflyVertex.class);
            Vertex v = null;
            while (v == null) {
                try {
                    v = writeVertex(idValue, label, properties);
                } catch (AerospikeException e) {
                    if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                        idValue = getIdFactory().createFromManager(this, FireflyVertex.class);
                    } else {
                        throw e;
                    }
                }
            }
            return v;
        } else {
            try {
                idValue = getIdFactory().createFromKeyValues(FireflyVertex.class, keyValues);
            } catch (IllegalArgumentException ignored) {
                // Invalid type for id.
                throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            }
            try {
                return writeVertex(idValue, label, properties);
            } catch (AerospikeException e) {
                if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                    throw Graph.Exceptions.vertexWithIdAlreadyExists(idValue.getUserId());
                } else {
                    throw e;
                }
            }
        }
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

    /**
     * This function finds TinkerPop Element objects in an object array and converts them to ids
     *
     * @param elements array of objects that might be Elements
     * @return array of raw ids
     */
    private List<Object> getIds(final List<Object> elements) {
        return elements.stream().map(e -> {
            if (Element.class.isAssignableFrom(e.getClass())) {
                return ((Element) e).id();
            } else {
                return e;
            }
        }).collect(Collectors.toList());
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIdsOrVertices) {
        return vertices(List.of(), vertexIdsOrVertices);
    }

    public Iterator<Vertex> vertices(final List<HasContainer> filters, final Object... vertexIdsOrVertices) {
        if (vertexIdsOrVertices.length == 1 && vertexIdsOrVertices[0] instanceof String) {
            if (vertexIdsOrVertices[0].equals(FIREFLY_CONFIGURATION_VARIABLE_NAME)) {
                return FireflyCloseableIteratorUtils.of(new FireflyMetadataVertex(this));
            }
            if (vertexIdsOrVertices[0].equals(GRAPH_SUMMARY_VERTEX)) {
                return FireflyCloseableIteratorUtils.of(new FireflyGraphSummaryVertex(this));
            }
            if (vertexIdsOrVertices[0].equals(FIREFLY_WARMUP_VARIABLE_NAME)) {
                WarmupUtil.create(configuration).preheat(1);
                return FireflyCloseableIteratorUtils.of(new FireflyMetadataVertex(this));
            }
        }

        final List<FireflyId> idList = getIds(Arrays.asList(vertexIdsOrVertices)).stream()
                .map(id -> getIdFactory().createId(id, FireflyVertex.class))
                .collect(Collectors.toList());

        // Create vertex iterator with graph and vertex id iterator.
        // If there are vertexIds present use them, otherwise read from database.
        return new FireflyBatchElementIterator<>(this, idList.isEmpty() ? scanAllVertices() : idList.iterator(), filters, this::readVertices);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return edges(List.of(), edgeIds);
    }

    private Iterator<Edge> edges(final List<HasContainer> filters, final Object... edgeIds) {
        // Create edge iterator with graph and edge id iterator.
        // If there are edgeIds present, convert them to an iterator of Longs, otherwise read edges from database.
        final List<Object> ids = getIds(List.of(edgeIds));
        final List<FireflyId> idList;
        try {
            idList = ids.stream()
                    .map(id -> getIdFactory().createId(id, FireflyEdge.class))
                    .collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            throw new NoSuchElementException(e.getMessage());
        }

        if (idList.isEmpty()) {
            return new FireflyBatchElementIterator<>(this, this.db.readElementIds(FireflyEdge.class), filters, this::readEdges);
        } else {
            return RelationalEdge.readEdges(this, idList).stream().map(fireflyEdge -> (Edge) fireflyEdge).iterator();
        }
    }

    /**
     * Create an Aerospike index Filter using the predicate and index info.
     *
     * @param predicate Predicate to use.
     * @param indexInfo Index info to use.
     * @return
     */
    public Filter predicateToFilter(final P<?> predicate, final FireflyIndexMetadata.IndexInfo indexInfo) {
        final String name;
        if (AerospikeConnection.LABEL.equals(indexInfo.key)) {
            name = AerospikeConnection.LABEL;
        } else if (indexInfo.setName.equals(getBaseGraph().VERTEX_AERO_SET)) {
            name = db.VERTEX_PROPERTY_NAME_TO_VALUE;
        } else if (indexInfo.setName.equals(getBaseGraph().EDGE_AERO_SET)) {
            name = db.PROPERTIES;
        } else {
            throw new IllegalArgumentException(
                    "Cannot create filter for index with unknown set name: " + indexInfo.setName + " and key " + indexInfo.key);
        }

        final IndexCollectionType type = AerospikeConnection.LABEL.equals(indexInfo.key) ?
                IndexCollectionType.DEFAULT : IndexCollectionType.MAPVALUES;
        final Object value = predicate.getValue();
        if (Number.class.isAssignableFrom(value.getClass())) {
            final Long casted;
            if (Integer.class.isAssignableFrom(value.getClass())) {
                casted = Long.valueOf((Integer) value);
            } else if (Long.class.isAssignableFrom(value.getClass())) {
                casted = (Long) value;
            } else {
                throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
            }

            if (predicate.getBiPredicate().equals(Compare.eq)) {
                return Filter.equal(name, casted, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.lt)) {
                return Filter.range(name, Long.MIN_VALUE, casted - 1, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.lte)) {
                return Filter.range(name, Long.MIN_VALUE, casted, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.gt)) {
                return Filter.range(name, casted - 1, Long.MAX_VALUE, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.gte)) {
                return Filter.range(name, casted, Long.MAX_VALUE, CTX.mapKey(Value.get(indexInfo.key)));
            } else {
                throw new RuntimeException(String.format("%s not a supported predicate", predicate));
            }
        } else {
            if (AerospikeConnection.LABEL.equals(indexInfo.key)) {
                return Filter.contains(name, type, (String) value);
            } else {
                return Filter.equal(name, (String) value, CTX.mapKey(Value.get(indexInfo.key)));
            }
        }
    }

    /**
     * Create an Aerospike Expression from the predicate, map key, and bin name.
     *
     * @param binName   Bin name to use.
     * @param mapKey    Map key to use.
     * @param predicate Predicate to use.
     * @return Expression.
     */
    private Exp predicateToExpression(final String binName,
                                      final String mapKey,
                                      final P<?> predicate) {
        // If the bin is the label bin, we can make a very simple predicate.
        if (AerospikeConnection.LABEL.equals(binName)) {
            return Exp.eq(Exp.stringBin(AerospikeConnection.LABEL), Exp.val((String) predicate.getValue()));
        }

        // Need to build a more complex expression for nested map values.
        final Object value = predicate.getValue();
        if (Number.class.isAssignableFrom(value.getClass())) {
            final Long casted;
            if (Integer.class.isAssignableFrom(value.getClass())) {
                casted = Long.valueOf((Integer) value);
            } else if (Long.class.isAssignableFrom(value.getClass())) {
                casted = (Long) value;
            } else {
                throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
            }

            if (predicate.getBiPredicate().equals(Compare.eq)) {
                return Exp.eq(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.lt)) {
                return Exp.lt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.lte)) {
                return Exp.le(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.gt)) {
                return Exp.gt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.gte)) {
                return Exp.ge(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else {
                throw new RuntimeException(String.format("%s not a supported predicate", predicate));
            }
        } else {
            return Exp.eq(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val((String) value));
        }
    }


    public Expression hasContainerListToExpression(final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        if (hasContainers.size() == 0) {
            return null;
        }
        final Exp[] exps = hasContainers.stream().map(h ->
                predicateToExpression(h.getKey().equals("~label") ?
                                AerospikeConnection.LABEL : FireflyVertex.class.isAssignableFrom(clazz) ?
                                db.VERTEX_PROPERTY_NAME_TO_VALUE : db.PROPERTIES,
                        h.getKey(),
                        h.getPredicate())).toArray(Exp[]::new);
        return exps.length == 1 ? Exp.build(exps[0]) : Exp.build(Exp.and(exps));
    }

    private Exp[] hasContainerListToExpArray(final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        return hasContainers.stream().map(h ->
                predicateToExpression(h.getKey().equals("~label") ?
                                AerospikeConnection.LABEL : FireflyVertex.class.isAssignableFrom(clazz) ?
                                db.VERTEX_PROPERTY_NAME_TO_VALUE : db.PROPERTIES,
                        h.getKey(),
                        h.getPredicate())).toArray(Exp[]::new);
    }

    /**
     * Execute query on index with predicate and return on the fly transformed iterator.
     *
     * @param indexInfo Index info to use.
     * @param predicate Predicate to use.
     * @param transform Transform to use.
     * @param <E>       Type of element to return.
     * @return Iterator of transformed elements.
     */
    public <E extends Element> Iterator<E> queryIndex(final FireflyIndexMetadata.IndexInfo indexInfo,
                                                      final P<?> predicate,
                                                      final TransformKeyRecord<E> transform,
                                                      final List<HasContainer> hasContainers,
                                                      final Class<? extends FireflyElement> clazz) {
        // Create query policy with expressions.
        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.filterExp = hasContainerListToExpression(hasContainers, clazz);

        // Query index.
        final Iterator<KeyRecord> keyRecordIterator = db.queryIndex(indexInfo.setName, indexInfo.indexName, predicateToFilter(predicate, indexInfo), queryPolicy);

        // Transform record to correct element.
        return FireflyCloseableIteratorUtils.map(keyRecordIterator, transform::transform);
    }

    /**
     * Execute query on index with predicate and return on the fly transformed iterator.
     *
     * @param indexInfo Index info to use.
     * @param predicate Predicate to use.
     * @param transform Transform to use.
     * @param <E>       Type of element to return.
     * @return Iterator of transformed elements.
     */
    public <E extends Element> Iterator<E> queryIndex(final FireflyIndexMetadata.IndexInfo indexInfo,
                                                      final P<?> predicate,
                                                      final TransformKeyRecord<E> transform) {
        return queryIndex(indexInfo, predicate, transform, Collections.emptyList(), null);
    }

    /**
     * Execute query on scan with predicate, map key, and return on the fly transformed iterator.
     *
     * @param mapKey        Map key to use.
     * @param setName       Set name to use.
     * @param binName       Bin name to use.
     * @param predicate     Predicate to use.
     * @param transform     Transform to use.
     * @param hasContainers HasContainers to use
     * @param <E>           Type of element to return.
     * @return Iterator of transformed elements.
     */
    public <E extends Element> Iterator<E> queryScan(final String mapKey,
                                                     final String setName,
                                                     final String binName,
                                                     final P<?> predicate,
                                                     final TransformKeyRecord<E> transform,
                                                     final List<HasContainer> hasContainers,
                                                     final Class<? extends FireflyElement> clazz) {
        // Build expression using predicate.
        final Exp exp = predicateToExpression(binName, mapKey, predicate);
        final Expression expression;
        if (hasContainers.size() > 0) {
            final Exp[] exps = hasContainerListToExpArray(hasContainers, clazz);
            final Exp[] allExps = new Exp[exps.length + 1];
            allExps[0] = exp;
            System.arraycopy(exps, 0, allExps, 1, exps.length);
            expression = Exp.build(Exp.and(allExps));
        } else {
            expression = Exp.build(exp);
        }

        db.getScanHitCounter().increment(mapKey);

        final ScanPolicy policy = new ScanPolicy();
        final Iterator<KeyRecord> keyRecordIterator = db.scanAllRecordsInSet(ReadContext.create(setName, binName, mapKey), expression, policy);

        // Transform record to correct element.
        return FireflyCloseableIteratorUtils.map(keyRecordIterator, kr -> transform.transform(kr));
    }

    /**
     * Execute query on scan with predicate, map key, and return on the fly transformed iterator.
     *
     * @param mapKey    Map key to use.
     * @param setName   Set name to use.
     * @param binName   Bin name to use.
     * @param predicate Predicate to use.
     * @param transform Transform to use.
     * @param <E>       Type of element to return.
     * @return Iterator of transformed elements.
     */
    public <E extends Element> Iterator<E> queryScan(final String mapKey,
                                                     final String setName,
                                                     final String binName,
                                                     final P<?> predicate,
                                                     final TransformKeyRecord<E> transform) {
        return queryScan(mapKey, setName, binName, predicate, transform, List.of(), FireflyVertex.class);
    }

    /**
     * Template to fill out to allow on the fly KeyRecord to Element mapping.
     *
     * @param <E> Type of element to return.
     */
    public interface TransformKeyRecord<E extends Element> {
        E transform(final KeyRecord keyRecord);
    }

    public interface GetElements<E extends Element> {
        Iterator<E> getFiltered(final List<HasContainer> hasContainers, final Object... ids);

        Iterator<E> getUnfiltered(final Object... ids);
    }

    /**
     * Function to create vertex property indexes using a list of property keys.
     *
     * @param vertexPropertyIndexes List of vertex property indexes to create.
     */
    private void createIndexes(final Class<? extends FireflyElement> elementClass, final String binName, final String prefix, final List<String> vertexPropertyIndexes) {
        final List<String> existingIndexes =
                AerospikeConnection.InfoOps.listExistingIndexes(db.getClient(), db.getNamespace()).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());

        // Add indexes specified in properties file.
        for (final String index : vertexPropertyIndexes) {
            // Create both string and numeric indexes for vertex properties.
            final String formattedIndex = String.format("%s_%s", prefix, index);
            db.createKeyValueSindex(existingIndexes, db.setFromElementType(elementClass),
                    formattedIndex + "_" + STRING, binName, index, STRING, IndexCollectionType.DEFAULT);
            db.createKeyValueSindex(existingIndexes, db.setFromElementType(elementClass),
                    formattedIndex + "_" + NUMERIC, binName, index, NUMERIC, IndexCollectionType.DEFAULT);
        }

        // Manually force metadata to update.
        fireflyIndexMetadata.updateMetadata();
    }

    @Override
    public Transaction tx() {
        throw new UnsupportedOperationException(UNIMPLEMENTED);
    }

    @Override
    public void close() {
        LOG.info("Closing FireflyGraph.");
        this.closed.set(true);
        this.fireflyCardinalityMetadataTask.cancel();
        this.fireflyIndexMetadataTask.cancel();
        this.fireflySummaryUpdater.close();
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

    @Override
    public ServiceRegistry getServiceRegistry() {
        return this.serviceRegistry;
    }
}
