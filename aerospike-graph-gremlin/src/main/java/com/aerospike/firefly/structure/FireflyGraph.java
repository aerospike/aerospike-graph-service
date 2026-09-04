/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.structure;

import ch.qos.logback.classic.Level;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.features.FireflyFeatures;
import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.AerospikeLogger;
import com.aerospike.firefly.io.aerospike.AerospikeOperations;
import com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry;
import com.aerospike.firefly.io.aerospike.query.FireflyExpressionIndex;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.io.aerospike.query.ReadInfo;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException;
import com.aerospike.firefly.process.computer.local.LocalGraphComputerView;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyStrategyBase;
import com.aerospike.firefly.process.traversal.strategy.util.FireflyStrategyUtil;
import com.aerospike.firefly.runtime.HttpServer;
import com.aerospike.firefly.runtime.tasks.FireflyConfigurationTask;
import com.aerospike.firefly.runtime.zipkin.OpenTelemetryZipkinExporter;
import com.aerospike.firefly.structure.transaction.FireflyTransaction;
import com.aerospike.firefly.structure.util.LogInfo;
import com.aerospike.firefly.util.SupernodesTraversedCounterUtil;
import com.aerospike.firefly.util.config.FireflyConfiguration;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.runtime.tasks.FireflyGraphSummaryUpdater;
import com.aerospike.firefly.runtime.tasks.FireflyMetadataTask;
import com.aerospike.firefly.runtime.tasks.FireflyUsageStats;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import com.aerospike.firefly.structure.iterator.FireflyBatchElementIterator;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeScanIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.util.FireflyTtlHandler;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.util.GraphFactory;
import com.aerospike.firefly.util.LoggerUtil;
import com.aerospike.firefly.util.PluginUtil;
import com.aerospike.firefly.util.concurrency.FireflyRecordLockHandler;
import com.aerospike.firefly.util.exceptions.TxNotEnabledException;
import com.aerospike.firefly.util.exceptions.VertexAlreadyExistsFailure;
import org.apache.commons.configuration2.Configuration;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getTypeHintOf;
import static com.aerospike.firefly.process.traversal.strategy.util.FireflyStrategyUtil.FIREFLY_STRATEGIES;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_DATA_SIZE;
import static com.aerospike.firefly.structure.FireflyEdge.IN_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.LABEL_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.OUT_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.PROPERTIES_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.TYPE_HINTS_POSITION;
import static com.aerospike.firefly.structure.FireflyVertex.SUPERNODE_PROPERTY_KEY;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.BULK_LOADER_FLAG;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.BULK_LOAD_ID_BUFFER_SIZE;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.HTTP_ENABLED;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.QUERY_TRACING_LOG_HOST;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.QUERY_TRACING_LOG_PORT;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.QUERY_TRACING_LOG_THRESHOLD;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.QUERY_TRACING_SAMPLE_PERCENT;


@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)

// GraphComputer OptOuts
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest", method = "g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX", reason = "stack overflow", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest", method = "*", reason = "REMOVE -- currently here for faster testing", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$GreedyMatchTraversals", method = "*", reason = "REMOVE -- currently here for faster testing", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$CountMatchTraversals", method = "*", reason = "REMOVE -- currently here for faster testing", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConnectedComponentTest$Traversals", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PageRankTest$Traversals", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PeerPressureTest$Traversals", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProgramTest$Traversals", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ShortestPathTest$Traversals", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgramTest", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgramTest", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathVertexProgramTest", method = "*", reason = "Firefly does not support persisting edges to new graph", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest$Traversals", method = "*", reason = "Firefly does not support arbitrary object starts", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionComputerTest", method = "*", reason = "Firefly does not support thread interruption ?? why not ??", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer", "com.aerospike.firefly.olap.structure.DistributedGraphComputer"})

// Tests that require lambda support
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV1Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV2Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GraphSONV3Test", method = "shouldSerializePath", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoV1Test", method = "shouldSerializePathAsDetached", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.SerializationTest$GryoV3Test", method = "shouldSerializePathAsDetached", reason = "Test requires Lambda support which is disabled for security.", computers = {"ALL"})

@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.TransactionTest", method = "*", reason = "MAKE ACTIVE WHEN TRANSACTIONS IMPLEMENTED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest", method = "*", reason = "MAKE ACTIVE WHEN PARALLEL SCAN RESULT ITERATOR IMPLEMENTED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest", method = "*", reason = "THROW PROPER EXCEPTIONS WHEN DESIRED FINAL FEATURE SET IS DETERMINED", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest", method = "*", reason = "THESE TESTS READ AND WRITE FROM 2 GRAPHS, BUT WHEN BACKED BY THE SAME AEROSPIKE INSTANCE, PRODUCE INVALID RESULTS", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest", method = "*", reason = "CURRENTLY DO NOT WORK, NEED TO FIX AND ENABLE", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldEvaluateConnectivityPatterns", reason = "This test fails due to caching.", computers = {"ALL"})

// Structure tests that only function on embedded Graphs - Arrays come through as primitives instead of expected ArrayLists from serialization
// Have to ignore the entire test suite for now due to a current issue in Tinkerpop where opting out of this parameterized test doesn't work
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.PropertyTest$PropertyFeatureSupportTest", method = "*", reason = "This test fails due to using arrays", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.VariablesTest$GraphVariablesFeatureSupportTest", method = "*", reason = "This test fails due to using arrays", computers = {"ALL"})

// Firefly does not support Float or Double IDs for Vertices
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentation", reason = "Firefly does not support Float ids", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentations", reason = "Firefly does not support Float ids", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentation", reason = "Firefly does not support Double ids", computers = {"ALL"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentations", reason = "Firefly does not support Double ids", computers = {"ALL"})

// Firefly does not support user-defined Edge IDs
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.structure.GraphTest", method = "shouldHaveExceptionConsistencyWhenFindEdgeByIdThatIsNonExistentViaIterator", reason = "Firefly does not expect Edge id lookups of random types", computers = {"ALL"})
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.util.star.StarGraphTest",
        method = "shouldCopyFromGraphAToGraphB",
        reason = "Test asserts ID equality.",
        computers = {"ALL"}
)

// Firefly does not support user-defined Vertex Property IDs
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.util.detached.DetachedGraphTest",
        method = "testAttachableCreateMethod",
        reason = "Test asserts ID equality.",
        computers = {"ALL"}
)
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.util.star.StarGraphTest",
        method = "shouldAttachWithCreateMethod",
        reason = "Test asserts ID equality.",
        computers = {"ALL"}
)

// Firefly does not support eventing
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest",
        method = "shouldTriggerAddVertexPropertyChangedViaMergeV",
        reason = "Custom Firefly MergeV step removed eventing due to incompatibility with multi-properties.",
        computers = {"ALL"}
)

// TODO: Should fix these tests in OLAP.
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest", method = "g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name", reason = "Temporary, will fix.", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})
@Graph.OptOut(test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest", method = "g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX", reason = "Temporary, will fix.", computers = {"com.aerospike.firefly.process.computer.local.LocalGraphComputer"})

public class FireflyGraph implements Graph, WrappedGraph<AerospikeConnection> {
    public static final String FIREFLY_CONFIGURATION_VARIABLE_NAME = "FIREFLY_CONFIGURATION";
    public static final String DATA_MODEL = "packed";
    public Object sparkSession = null;

    // AerospikeGraphService is a dummy class that allows us to instantiate a logger in FireflyGraph that says
    // AerospikeGraphService. We can eventually migrate to calling FireflyGraph AerospikeGraphService but this requires
    // docs changes, config updates, etc, and isn't worth it right now.
    public static final String PRODUCT_NAME = "Aerospike Graph";
    private static final Logger LOG = LoggerFactory.getLogger(PRODUCT_NAME);
    public static String FIREFLY_VERSION = "3.3.0-SNAPSHOT";

    // Doesn't use hidden key token ~ due to internal Tinkerpop MergeStep validation
    public static final String BULK_LOAD_VERTEX_ADD_KEY = "___bulkLoadMergeVIdentifier";
    public static final String BULK_LOAD_VERTEX_ADD_KEY_IS_SUPERNODE = "___bulkLoadMergeVIdentifierIsSuperNode";

    public final AtomicBoolean closed = new AtomicBoolean(false);
    private final Timer fireflyCardinalityMetadataTask = new Timer(true);
    private final Timer fireflyIndexMetadataTask = new Timer(true);
    private Timer configurationWatcherTask;
    private final FireflyFeatures features;
    private final Configuration configuration;
    public static String VP_INDEX_PREFIX = "VP";
    public static String VP_EXPRESSION_INDEX_PREFIX = "VE";
    public static String EP_INDEX_PREFIX = "EP";
    private final FireflyGraphVariables variables;
    protected final AerospikeConnection db;
    public final AerospikeOperations aerospikeOperations;
    private final FireflyIdFactory idFactory;
    public LocalGraphComputerView graphComputerView = null;
    public final boolean bulkLoaderFlag;
    public final long bulkLoadIdBufferSize;
    private final FireflyTtlHandler ttlHandler;
    public final FireflyCardinalityMetadata fireflyCardinalityMetadata;
    public final FireflyIndexMetadata fireflyIndexMetadata;
    public final FireflyGraphSummaryUpdater fireflySummaryUpdater;
    private final FireflyRecordLockHandler fireflyRecordLockHandler;
    private final ServiceRegistry serviceRegistry = new ServiceRegistry();
    private static volatile FireflyUsageStats usageStats;
    private AdminServiceRegistry adminServiceRegistry;
    private boolean httpStarted = false;
    private static final String GREMLIN_SERVER_YAML_PATH = "GREMLIN_SERVER_YAML_PATH";
    private final Settings gremlinServerSettings;
    public static ExitManager EXIT_MANAGER = new ExitManager();
    public static boolean NEED_PREHEAT = true;
    public final GraphQuery graphQuery;
    private final FireflyTransaction transaction;
    private boolean queryTracingEnabled = false;
    private OpenTelemetryZipkinExporter zipkinExporter;
    public LogInfo logInfo = null;
    private final SupernodesTraversedCounterUtil supernodesTraversedCounterUtil;

    public void logMessage(final String message, final Logger logger) {
        if (logInfo != null) {
            logInfo.debuggingMessage(message, logger);
        } else {
            logger.info(message);
        }
    }

    public static class ExitManager {
        public void exit(final int code) {
            System.exit(code);
        }
    }

    private static final AtomicBoolean INFO_PRINTED = new AtomicBoolean(false);
    private static final ThreadLocal<String> USER = ThreadLocal.withInitial(() -> "anonymous");

    static {
        synchronized (TraversalStrategies.GlobalCache.class) {
            TraversalStrategies.GlobalCache.registerStrategies(
                    FireflyGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                            .removeStrategies(CountStrategy.class)
                            .addStrategies(LambdaRestrictionStrategy.instance())
                            .addStrategies(FIREFLY_STRATEGIES.toArray(new FireflyStrategyBase[0]))
                            .addStrategies(OptionsStrategy.build().create()));
        }
    }

    public FireflyGraph(final AerospikeConnection db, final FireflyConfiguration conf, final Settings gremlinServerSettings) {
        try {
            this.gremlinServerSettings = gremlinServerSettings;
            this.configuration = conf;
            this.db = db;
            db.createGraphIndexes();
            this.aerospikeOperations = new AerospikeOperations(this);
            graphQuery = new GraphQuery(this);
            this.idFactory = db.getIdFactory();
            this.bulkLoaderFlag = db.getBulkLoaderFlag();
            this.bulkLoadIdBufferSize = ConfigurationHelper.getOrDefaultInt(BULK_LOAD_ID_BUFFER_SIZE, conf);

            this.variables = new FireflyGraphVariables(this);
            final String vpCardinalityString = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.VERTEX_PROPERTY_CARDINALITY, conf);
            final VertexProperty.Cardinality vpCardinality;
            if ("list".equals(vpCardinalityString)) {
                vpCardinality = VertexProperty.Cardinality.list;
            } else if ("set".equals(vpCardinalityString)) {
                vpCardinality = VertexProperty.Cardinality.set;
            } else {
                // Default to single.
                vpCardinality = VertexProperty.Cardinality.single;
            }
            this.features = new FireflyFeatures(vpCardinality, this.db.getConfig().transactionEnabled);

            // Create index metadata background task that will populate indexes for the named graph on the fly.
            fireflyIndexMetadata = new FireflyIndexMetadata(db);
            final TimerTask indexMetadataTimerTask = new FireflyMetadataTask(fireflyIndexMetadata);
            fireflyIndexMetadataTask.schedule(indexMetadataTimerTask, 0, db.getConfig().indexMetadataUpdateFrequency);

            // If bulk loading, only create indexes for the first bulk loader graph initialization. Otherwise they spam 1000's of times.
            if (db.shouldCreateIndexes()) {
                // Grab user defined vertex property indexes from the configuration and create them.
                final List<String> vertexPropertyIndexes = ConfigurationHelper.getOrDefaultList(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES, configuration);
                final List<String> vertexPropertyStringIndexes = ConfigurationHelper.getOrDefaultList(ConfigurationHelper.Keys.VERTEX_PROPERTY_STRING_INDEXES, configuration);
                final List<String> vertexPropertyNumericIndexes = ConfigurationHelper.getOrDefaultList(ConfigurationHelper.Keys.VERTEX_PROPERTY_NUMERIC_INDEXES, configuration);
                final Set<String> combinedStringVpIndexes = new HashSet<>(vertexPropertyIndexes);
                combinedStringVpIndexes.addAll(vertexPropertyStringIndexes);
                final Set<String> combinedNumericVpIndexes = new HashSet<>(vertexPropertyIndexes);
                combinedNumericVpIndexes.addAll(vertexPropertyNumericIndexes);
                createVertexPropertyIndexes(FireflyVertex.class, db.getConfig().vertexPropertyDataBin, db.getVpIndexPrefix(),
                        combinedStringVpIndexes, combinedNumericVpIndexes);

                final List<String> geoPropertyIndexes = ConfigurationHelper.getOrDefaultList(
                        ConfigurationHelper.Keys.VERTEX_PROPERTY_GEO_INDEXES, configuration);
                createGeoPropertyIndexes(new HashSet<>(geoPropertyIndexes));

                final String expressionIndexesString = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.VERTEX_PROPERTY_EXPRESSION_INDEXES, configuration);
                createExpressionIndexes(expressionIndexesString);

                // Grab user defined edge property indexes from the configuration and create them.
                final List<String> edgePropertyIndexes = ConfigurationHelper.getOrDefaultList(ConfigurationHelper.Keys.EDGE_PROPERTY_INDEXES, configuration);
                if (edgePropertyIndexes != null && !edgePropertyIndexes.isEmpty()) {
                    // TODO: Edge indexes.
                    throw new RuntimeException("Edge property indexes are not currently supported.");
                }
            }

            // Create ttl background task.
            this.ttlHandler = new FireflyTtlHandler(this);

            // Create cardinality metadata background task that will populate cardinality for the named graph on the fly.
            fireflyCardinalityMetadata = new FireflyCardinalityMetadata(db, db.getConfig().vLabelIndexName, db.getConfig().eLabelIndexName, fireflyIndexMetadata);
            final TimerTask cardinalityMetadataTimerTask = new FireflyMetadataTask(fireflyCardinalityMetadata);

            fireflyCardinalityMetadataTask.schedule(cardinalityMetadataTimerTask, 0, db.getConfig().cardinalityMetadataUpdateFrequency);
            fireflySummaryUpdater = new FireflyGraphSummaryUpdater(db);
            fireflyRecordLockHandler = new FireflyRecordLockHandler(db);
            supernodesTraversedCounterUtil = SupernodesTraversedCounterUtil.getInstance();

            if (conf.containsKey(ConfigurationHelper.Keys.PLUGIN)) {
                final String pluginConfigString = conf.getString(ConfigurationHelper.Keys.PLUGIN);
                final String[] plugins = pluginConfigString.split(",");
                for (final String plugin : plugins) {
                    PluginUtil.loadPlugin(plugin, conf, this);
                }
            }

            this.transaction = new FireflyTransaction(this);
            this.db.setTransaction(this.transaction);

            if (!db.getConfig().warmupMode && !db.getBulkLoaderFlag() && !db.getOlapFlag()) {
                // Create usage statistics background task. Only one per server,
                // and only when the operator has not disabled it via the
                // USAGE_STATS_ENABLED kill-switch.
                if (db.getConfig().usageStatsEnabled && usageStats == null) {
                    synchronized (this) {
                        if (usageStats == null) {
                            usageStats = new FireflyUsageStats(db);
                        }
                    }
                }

                // Register admin services graph metrics since it will bootstrap the server.
                adminServiceRegistry = new AdminServiceRegistry(this);

                // do not start http server if disabled in config or for bulk loader
                final boolean httpEnabled = ConfigurationHelper.getOrDefaultBool(HTTP_ENABLED, conf);
                if (httpEnabled) {
                    HttpServer.getInstance().start(this);
                    httpStarted = true;
                }

                final int queryTracingMinMillis = ConfigurationHelper.getOrDefaultInt(QUERY_TRACING_LOG_THRESHOLD, conf);
                if (queryTracingMinMillis >= 0) {
                    this.queryTracingEnabled = true;
                    final int queryTracingSamplePercent = ConfigurationHelper.getOrDefaultInt(QUERY_TRACING_SAMPLE_PERCENT, conf);
                    final String queryTracingLogHost = ConfigurationHelper.getOrDefaultString(QUERY_TRACING_LOG_HOST, conf);
                    final int queryTracingLogPort = ConfigurationHelper.getOrDefaultInt(QUERY_TRACING_LOG_PORT, conf);
                    this.zipkinExporter = OpenTelemetryZipkinExporter.create(db.getConfig().graphId, queryTracingLogHost,
                            queryTracingLogPort, queryTracingMinMillis, queryTracingSamplePercent);
                }

                if (db.getConfig().configUpdateEnabled) {
                    final FireflyConfigurationTask fireflyConfigurationTask = new FireflyConfigurationTask(db);
                    configurationWatcherTask = new Timer(true);
                    // start with delay
                    configurationWatcherTask.schedule(fireflyConfigurationTask, db.getConfig().configUpdateFrequency, db.getConfig().configUpdateFrequency);
                }
            }
        } catch (final Exception e) {
            close();
            throw e;
        }
    }

    public void setSparkSession(final Object sparkSession) {
        this.sparkSession = sparkSession;
    }

    public AdminServiceRegistry getAdminServiceRegistry() {
        return adminServiceRegistry;
    }

    public FireflyUsageStats getUsageStats() {
        return usageStats;
    }

    public String getUser() {
        return USER.get();
    }

    public boolean isQueryTracingEnabled() {
        return this.queryTracingEnabled;
    }

    public void incrementSupernodesTraversed() {
        supernodesTraversedCounterUtil.add();
    }

    public static FireflyGraph open(final Configuration conf) {
        final FireflyConfiguration fireflyConf = FireflyConfiguration.fromConfiguration(conf);
        ConfigurationHelper.validateConfig(fireflyConf);
        String logLevel;

        final boolean isTesting = Boolean.parseBoolean(System.getenv("FIREFLY_TESTING"));
        if (FIREFLY_VERSION != null && FIREFLY_VERSION.endsWith("SNAPSHOT") && !isTesting) {
            try {
                final String commitHash = getGitCommitHash();
                LOG.info("Built from git commit hash: {}", commitHash);
            } catch (final Exception e) {
                LOG.warn("Could not get the git commit hash: {}", e.getMessage());
            }
        }

        if (System.getenv("FIREFLY_TESTING") != null &&
                System.getenv("FIREFLY_TESTING").equalsIgnoreCase("true")) {
            logLevel = "WARN";
            // Tests that need to check output of logs.
            for (final StackTraceElement e : Thread.currentThread().getStackTrace()) {
                if (e.getClassName().contains("TestAuditLog") || e.getClassName().contains("TestWarmup")
                        || e.getClassName().contains("TestShutdown") || e.getClassName().contains("FireflyGraphSummaryUpdaterTest")) {
                    logLevel = "INFO";
                    break;
                }
            }
        } else {
            logLevel = ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.LOG_LEVEL, fireflyConf);
        }
        final boolean clientLogging = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.ASCLIENT_LOG_ENABLED, fireflyConf);
        try {
            // Prevent warmup from disabling the logger for Aerospike Client.
            if (clientLogging && !ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.WARMUP_MODE, fireflyConf)) {
                Log.setCallback(new AerospikeLogger());
                Log.setLevel(Log.Level.valueOf(logLevel));
            }
            LoggerUtil.setLogLevel(Level.toLevel(logLevel));
        } catch (final Exception e) {
            LOG.warn("Failed to set log level {}", e.getMessage());
        }
        try {
            // FIREFLY_TESTING is set strictly by surefire plugin so not in any customer systems.
            // This makes our testing logs 100x smaller.
            boolean infoPrinted = INFO_PRINTED.get();
            if (System.getenv("FIREFLY_TESTING") == null ||
                    !System.getenv("FIREFLY_TESTING").equalsIgnoreCase("true")
                            && !infoPrinted) {
                final Runtime javaRuntime = Runtime.getRuntime();
                LOG.info("Java Runtime: {} available processors.", javaRuntime.availableProcessors());
                LOG.info("Java Runtime: {} MB max memory.", javaRuntime.maxMemory() / (1024 * 1024));
                LOG.info("Java Runtime: {} MB total memory.", javaRuntime.totalMemory() / (1024 * 1024));
                LOG.info("Java Runtime: {} MB free memory.", javaRuntime.freeMemory() / (1024 * 1024));
                LOG.info("JVM Vendor: {}", System.getProperty("java.vm.vendor"));
                LOG.info("JVM Specification Vendor: {}.", System.getProperty("java.vm.specification.vendor"));
                LOG.info("Java Specification Version: {}.", System.getProperty("java.specification.version"));
                LOG.info("JVM Runtime: {}.", System.getProperty("java.runtime.name"));
                LOG.info("JVM Runtime Version: {}.", System.getProperty("java.runtime.version"));

                // Straight up printing out conf just provides a class name / memory address.
                final Iterator<String> keys = fireflyConf.getKeys();
                final Map<String, Object> configurationMap = new HashMap<>();
                while (keys.hasNext()) {
                    final String key = keys.next();
                    if (!key.contains("password") && !key.contains("secret") && !key.contains("token")) {
                        configurationMap.put(key, fireflyConf.getProperty(key));
                    } else {
                        configurationMap.put(key, "********");
                    }
                }
                LOG.info("Aerospike Graph Service configuration: {}.", configurationMap);
            }
            LOG.info("Starting Aerospike Graph Service v{}.", FIREFLY_VERSION.replace("-SNAPSHOT", ""));

            INFO_PRINTED.set(true);
            if (ConfigurationHelper.getOrDefaultBool(BULK_LOADER_FLAG, fireflyConf)) {
                // If we are in bulk load mode, sleep between 0 and 5 seconds to allow Aerospike time between spark
                // works initializing.
                final Random random = new Random();
                try {
                    Thread.sleep(random.nextInt(5000));
                } catch (final InterruptedException ignored) {
                    // Propagate the interrupt but ignore it for the context of the sleep.
                    Thread.currentThread().interrupt();
                }
            }

            // Reset strategy enable flags.
            FireflyStrategyUtil.resetStrategies();

            return GraphFactory.createGraph(AerospikeConnection.connect(fireflyConf), fireflyConf);
        } catch (final Exception e) {
            if (ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.WARMUP_MODE, fireflyConf)) {
                ConfigurationHelper.restoreLogLevel(fireflyConf);
                throw e;
            }
            if (ConfigurationHelper.getOrDefaultBool(BULK_LOADER_FLAG, fireflyConf)) {
                LOG.error("Failed to start an Aerospike Graph Service instance during Bulk Loading: {}", e.getMessage());
                throw e;
            }
            LOG.error("=================== FAILED TO START AEROSPIKE GRAPH SERVICE ===================");
            LOG.error("========== Aerospike Graph Service failing to start is usually a result of an incorrect configuration.");
            if (e.getMessage() != null) {
                LOG.error("========== See Error message for more details: {}", e.getMessage());
            } else {
                LOG.error("========== Error did not contain message; please submit this stack trace to support", e);
            }

            // Signal to gremlin-server to shut down.
            EXIT_MANAGER.exit(1);

            // Required to compile.
            return null;
        }
    }

    public static ComparableVersion dataModelVersion() {
        return new ComparableVersion(FIREFLY_VERSION);
    }

    private static Settings GREMLIN_SERVER_SETTINGS = null;

    public static synchronized String getGremlinServerYamlFile() {
        final String gremlinServerYamlPath = System.getenv(GREMLIN_SERVER_YAML_PATH);
        if ((gremlinServerYamlPath == null || gremlinServerYamlPath.isEmpty()) ||
                !(new File(gremlinServerYamlPath).exists())) {
            throw new RuntimeException("Failed to load docker settings file, " +
                    "must be running docker image to read the gremlin-server yaml.");
        }
        return gremlinServerYamlPath;
    }

    public static synchronized Settings getGremlinServerSettings() {
        if (GREMLIN_SERVER_SETTINGS == null) {
            try {
                final String yamlLocation = getGremlinServerYamlFile();
                GREMLIN_SERVER_SETTINGS = Settings.read(yamlLocation);
            } catch (final Exception e) {
                final String testing = System.getenv("FIREFLY_TESTING");
                final String bulkLoading = System.getenv("BULK_LOADING");
                if ((!"true".equalsIgnoreCase(testing)) && "true".equalsIgnoreCase(bulkLoading)) {
                    LOG.error("Failed to load gremlin-server settings file.", e);
                }
                GREMLIN_SERVER_SETTINGS = new Settings();
            }
        }
        return GREMLIN_SERVER_SETTINGS;
    }

    private String configFilePath = "/opt/conf/aerospike-graph-graph.properties";

    public String getConfigFilePath() {
        return configFilePath;
    }

    public void setConfigFilePath(final String configFilePath) {
        this.configFilePath = configFilePath;
    }

    /**
     * Return the FireflyIdFactory
     *
     * @return FireflyIdFactory
     */
    public FireflyIdFactory getIdFactory() {
        return idFactory;
    }

    public String getDataModel() {
        return FireflyGraph.getDataModelName();
    }

    /**
     * Function to write vertex to Aerospike.
     *
     * @param idValue    Id of vertex.
     * @param label      Label of vertex.
     * @param properties List of vertex properties.
     * @return Vertex.
     */
    public FireflyVertex writeVertex(final FireflyId idValue,
                                     final String label,
                                     final List<Map.Entry<String, Object>> properties) {
        // If the supernode property flag is set on the vertex write, remove it from the write steam and assign it.
        final Map.Entry supernodeFlag = properties.stream().filter(e -> e.getKey().equals(SUPERNODE_PROPERTY_KEY)).findFirst().orElse(null);
        if (supernodeFlag != null) {
            properties.remove(supernodeFlag);
        }
        final boolean isEdgeCacheOverflowed = !this.db.getConfig().globalEdgeCacheEnabledFlag ||
                this.db.getConfig().onRecordIdLimit <= 0 || supernodeFlag != null;
        return aerospikeOperations.writeVertex(idValue, label, properties, true, isEdgeCacheOverflowed);
    }

    public void bulkWriteMergeVertex(final Object id,
                                     final String label,
                                     final List<Map.Entry<String, Object>> properties,
                                     final int partitionId,
                                     final Map<String, VertexProperty.Cardinality> vpCardinalities,
                                     final boolean isEdgeCacheOverflowed) {
        final Map<Object, Object> onCreate = new HashMap<>();
        final Map<Object, Object> onMatch = new HashMap<>();
        final List<Map.Entry<String, Object>> listProperties = new ArrayList<>();
        final List<Map.Entry<String, Object>> setProperties = new ArrayList<>();
        onCreate.put(BULK_LOAD_VERTEX_ADD_KEY_IS_SUPERNODE, isEdgeCacheOverflowed);
        onCreate.put(BULK_LOAD_VERTEX_ADD_KEY, partitionId);
        onCreate.put(T.label, label);
        // We can add one instance of each property key into the MergeV maps to leverage the strategy's optimizations.
        for (final Map.Entry<String, Object> entry : properties) {
            if (VertexProperty.Cardinality.list.equals(vpCardinalities.get(entry.getKey()))) {
                // Use onMatch simply because it's a smaller map
                if (!onMatch.containsKey(entry.getKey())) {
                    onCreate.put(entry.getKey(), VertexProperty.Cardinality.list(entry.getValue()));
                    onMatch.put(entry.getKey(), VertexProperty.Cardinality.list(entry.getValue()));
                } else {
                    listProperties.add(entry);
                }
            } else if (VertexProperty.Cardinality.set.equals(vpCardinalities.get(entry.getKey()))) {
                if (!onMatch.containsKey(entry.getKey())) {
                    onCreate.put(entry.getKey(), VertexProperty.Cardinality.set(entry.getValue()));
                    onMatch.put(entry.getKey(), VertexProperty.Cardinality.set(entry.getValue()));
                } else {
                    setProperties.add(entry);
                }
            } else {
                onCreate.put(entry.getKey(), entry.getValue());
                onMatch.put(entry.getKey(), entry.getValue());
            }
        }
        int tryCount = 0;
        while (true) {
            try {
                GraphTraversal t = traversal().mergeV(CollectionUtil.asMap(T.id, id))
                        .option(Merge.onCreate, onCreate).option(Merge.onMatch, onMatch);
                for (final Map.Entry<String, Object> entry : listProperties) {
                    t = t.property(VertexProperty.Cardinality.list, entry.getKey(), entry.getValue());
                }
                for (final Map.Entry<String, Object> entry : setProperties) {
                    t = t.property(VertexProperty.Cardinality.set, entry.getKey(), entry.getValue());
                }
                t.iterate();
                break;
            } catch (final IllegalArgumentException e) {
                if (!e.getMessage().contains("Vertex with id already exists")) {
                    throw e;
                }
                // A mergeE can fail due to vertex with id already existing b/c it is not a true transaction and we may
                // be inserting many updates to the vertex. However once the vertex does exist this should not fail a second time.
                // Testing has shown that if this happens it tends to happen a lot so it is best left not logged and let the error come out later if
                // the implementation is wrong.
                if (tryCount > 0) {
                    throw e;
                }
                tryCount++;
            } catch (final AerospikeGraphException age) {
                throw new FireflyLoadingException(age);
            }
        }
    }

    public List<Boolean> bulkVertexExists(final Object[] ids) {
        try {
            // We do not use ~supernode flag to allow forcing a vertex to a supernode when bulk loading since it impacts our
            // bulk loader flow and also we already have to check for this regardless inside the bulk loader.
            return aerospikeOperations.verticesExist(ids);
        } catch (final AerospikeGraphException e) {
            throw new FireflyLoadingException(e);
        }
    }

    public void bulkWriteVertex(final FireflyId idValue,
                                final String label,
                                final List<Map.Entry<String, Object>> properties,
                                final boolean supernode,
                                final int partitionId,
                                final Optional<Map<String, List<FireflyId>>> toEdgeCache,
                                final Optional<Map<String, List<FireflyId>>> fromEdgeCache) {
        try {
            // We do not use ~supernode flag to allow forcing a vertex to a supernode when bulk loading since it impacts our
            // bulk loader flow and also we already have to check for this regardless inside the bulk loader.
            aerospikeOperations.writeVertex(idValue, label, properties, true, supernode, partitionId, toEdgeCache, fromEdgeCache);
        } catch (final AerospikeGraphException e) {
            // Can throw exception if vertex already exists - this is fine because there may be duplicates.
            if (e.getCause() instanceof AerospikeException) {
                final AerospikeException ae = (AerospikeException) e.getCause();
                if (ae.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                    return;
                }
            }
            throw new FireflyLoadingException(e);
        }
    }

    public void writeDuplicateVertexId(final Object vertexId, final long count) {
        final FireflyId id = getIdFactory().createVertexId(vertexId);
        final Key key = new Key(db.getConfig().namespace, db.getConfig().bulkLoadDuplicateVidSet, Value.get(id.getStorageId()));
        final Bin addBin = new Bin(db.getConfig().counterBin, count);
        final WritePolicy policy = new WritePolicy();
        policy.sendKey = true;
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        try {
            this.db.writeOperate(policy, key, Operation.add(addBin));
        } catch (final AerospikeGraphException e) {
            // Do not retry this or fail because this list being slightly incorrect is inconsequential and will reduce
            // bulk load speed
            LOG.warn("Error recording duplicate Vertex ID details ID: " + vertexId);
        }
    }

    public Iterator<Map<String, Object>> readDuplicateVertexIdErrors() {
        return this.graphQuery.scanSet(null, db.getConfig().bulkLoadDuplicateVidSet, null, null, keyRecord -> {
            final Map<String, Object> errorInfo = new HashMap<>();
            errorInfo.put("id", keyRecord.key.userKey.getObject());
            errorInfo.put("count", keyRecord.record.getLong(db.getConfig().counterBin));
            return errorInfo;
        }, settings().evaluationTimeout);
    }

    public void writeBadEntry(final String row, final String fileName) {
        final Key key = new Key(db.getConfig().namespace, db.getConfig().bulkLoadBadEntrySet, Value.get(UUID.randomUUID().toString()));
        final Bin rowBin = new Bin(db.getConfig().blRowBin, row);
        final Bin fileBin = new Bin(db.getConfig().blFileBin, fileName);
        final WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        try {
            this.db.writeOperate(policy, key, Operation.put(rowBin), Operation.put(fileBin));
        } catch (final AerospikeGraphException e) {
            // Do not retry this or fail because this list being slightly incorrect is inconsequential and will reduce
            // bulk load speed
            LOG.warn("Error recording bad entry row \"" + row + "\" in file \"" + fileName + "\"");
        }
    }

    public Iterator<Map<String, String>> readBadEntryErrors() {
        return this.graphQuery.scanSet(null, db.getConfig().bulkLoadBadEntrySet, null, null, keyRecord -> {
            final Map<String, String> errorInfo = new HashMap<>();
            errorInfo.put("row", keyRecord.record.getString(db.getConfig().blRowBin));
            errorInfo.put("file", keyRecord.record.getString(db.getConfig().blFileBin));
            return errorInfo;
        }, settings().evaluationTimeout);
    }

    public Iterator<Map<String, Object>> readBadEdgeErrors() {
        return this.graphQuery.scanSet(null, db.getConfig().bulkLoadBadEdgeSet, null, null, keyRecord -> {
            final Map<String, Object> errorInfo = new HashMap<>();
            errorInfo.put("bad-vertex-id", keyRecord.key.userKey.getObject());
            errorInfo.put("count", keyRecord.record.getLong(db.getConfig().counterBin));
            return errorInfo;
        }, settings().evaluationTimeout);
    }

    /**
     * Function to bulk write edges to a vertex's edge cache
     *
     * @param vertexId  Vertex label.
     * @param direction Direction of the edges.
     * @param edgeIds   List of edge IDs.
     * @param edgeLabel Label of all edges in edge ID list.
     */
    public void bulkWriteEdgesToVertexCache(final FireflyId vertexId, final Direction direction,
                                            final List<Value> edgeIds, final String edgeLabel) {
        // Get the key.
        final Key key = FireflyRecord.getKey(db, this.db.getConfig().vertexAeroSet, vertexId);

        // Get direction and counter keys. Direction must be IN or OUT.
        final String directionBinName = direction == Direction.IN ? this.db.getConfig().inEdgesBin : this.db.getConfig().outEdgesBin;

        // Create the operations.
        final ListPolicy preventDuplicates = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);
        final Operation appendEdgeId = ListOperation.appendItems(
                preventDuplicates,
                directionBinName,
                edgeIds,
                CTX.mapKeyCreate(Value.get(edgeLabel), MapOrder.KEY_ORDERED)
        );

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            this.db.writeOperate(writePolicy, key, appendEdgeId);
        } catch (final AerospikeGraphException e) {
            throw new FireflyLoadingException(e);
        }
    }

    /**
     * Function to read vertex from Aerospike.
     *
     * @param idValue Id of vertex.
     * @return Vertex.
     */
    public FireflyVertex readVertex(final FireflyId idValue) {
        final List<FireflyVertex> vertices = readVertices(List.of(), List.of(idValue), null);
        if (vertices.isEmpty()) {
            return null;
        } else {
            return vertices.get(0);
        }
    }

    public List<FireflyVertex> readVertices(final List<HasContainer> hasContainers,
                                            final List<FireflyId> idValues,
                                            final List<String> requiredProperties,
                                            final boolean areEdgesRequired) {
        final ReadInfo readInfo = ReadInfo.create().
                set(db.getConfig().vertexAeroSet).
                ids(idValues).
                reqProps(requiredProperties).
                exp(hasContainers, db, FireflyVertex.class).
                areEdgesRequired(areEdgesRequired).
                build();
        return aerospikeOperations.readVertices(readInfo);
    }

    public List<FireflyVertex> readVertices(final List<HasContainer> hasContainers,
                                            final List<FireflyId> idValues,
                                            final List<String> requiredProperties) {
        return readVertices(hasContainers, idValues, requiredProperties, true);
    }

    /**
     * Function to create vertex from a KeyRecord.
     *
     * @param keyRecord KeyRecord to use.
     * @return Vertex.
     */
    public FireflyVertex vertexFromRecord(final KeyRecord keyRecord) {
        return FireflyVertexFactory.create(keyRecord, this);
    }

    public FireflyId vertexIdFromRecord(final KeyRecord keyRecord) {
        return getIdFactory().createVertexIdFromRecord(keyRecord);
    }

    // This function is used via reflection in Upgrade.java. Removing will cause issues.
    public static String getDataModelName() {
        return DATA_MODEL;
    }

    public void bulkWriteEdges(final List<byte[]> edgeIds, final List<String> labels, final List<List<Map.Entry<String, Object>>> phatProperties,
                               final List<Object> inVertexIds, final List<Object> outVertexIds, final List<Boolean> inVSupernodes,
                               final List<Boolean> outVSupernodes, final List<Integer> partitionIds) {
        FireflyGraph.LOG.debug("Writing edge {} [({})-({})->({})] {}.", edgeIds, outVertexIds, labels, inVertexIds, phatProperties);
        final List<Operation> operations = new ArrayList<>();
        for (int i = 0; i < edgeIds.size(); i++) {
            generateWriteEdgeOperations(edgeIds.get(i), labels.get(i), phatProperties.get(i), inVertexIds.get(i),
                    outVertexIds.get(i), inVSupernodes.get(i), outVSupernodes.get(i), operations);
        }

        // All ids have same packing id, so just use 0.
        final Key key = getKey(db, db.getConfig().edgeAeroSet, getIdFactory().createEdgeId(edgeIds.get(0)));
        try {
            getBaseGraph().writeOperate(null, key, operations.toArray(new Operation[0]));
        } catch (final AerospikeGraphException e) {
            throw new FireflyLoadingException(e);
        }
        for (int i = 0; i < edgeIds.size(); i++) {
            final String label = labels.get(i);
            final List<Map.Entry<String, Object>> properties = phatProperties.get(i);
            final int partitionId = partitionIds.get(i);
            fireflySummaryUpdater.stageEdgeWriteToQueue(label, properties.stream().map(Map.Entry::getKey).collect(Collectors.toSet()), partitionId);
        }
    }

    private void generateWriteEdgeOperations(final byte[] edgeId, final String label, final List<Map.Entry<String, Object>> properties,
                                             final Object inVertexId, final Object outVertexId, final boolean inVSupernode,
                                             final boolean outVSupernode, final List<Operation> operations) {
        final boolean isAttachedToSupernode = inVSupernode || outVSupernode;
        // CREATE and UPDATE are both okay since this is idempotent.
        final MapPolicy edgeMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final FireflyPhatEdgeId id = getIdFactory().createEdgeId(edgeId);
        final FireflyId inId = getIdFactory().createVertexId(inVertexId);
        final FireflyId outId = getIdFactory().createVertexId(outVertexId);

        final Map<String, Object> propertyMap = new TreeMap<>();
        final Map<String, Object> typeHints = new HashMap<>();
        properties.forEach(property -> {
            final String key = property.getKey();
            final Object originalValue = FireflyHelper.validatePropertyValue(property.getValue());
            final Object aerospikeWritableValue = FireflyHelper.convertValueToAerospikeWriteable(originalValue);

            if (originalValue == null) {
                propertyMap.remove(key);
                typeHints.remove(key);
            } else {
                propertyMap.put(key, aerospikeWritableValue);
                final Object typeHint = getTypeHintOf(originalValue);
                if (typeHint != null) {
                    typeHints.put(key, typeHint);
                }
            }
        });
        final HashMap<Long, Object> typeHintsDisk = new HashMap<>();
        db.schemaManager.populateEdgePropertyStringMapToSchemaMap(typeHints, typeHintsDisk);

        if (!isAttachedToSupernode) {
            final List<Value> edgeData = new ArrayList<>(EDGE_DATA_SIZE);
            // Add label to Edge data.
            edgeData.add(LABEL_POSITION, Value.get(db.schemaManager.getEdgeLabelWrite(label)));
            // Add IN and OUT to Edge data.
            edgeData.add(IN_V_POSITION, Value.get(inId.getUserId()));
            edgeData.add(OUT_V_POSITION, Value.get(outId.getUserId()));

            // Add properties and type hints to Edge data.
            final TreeMap<Long, Object> propertyMapDisk = new TreeMap<>();
            db.schemaManager.populateEdgePropertyStringMapToSchemaMap(propertyMap, propertyMapDisk);
            edgeData.add(PROPERTIES_POSITION, Value.get(propertyMapDisk));
            edgeData.add(TYPE_HINTS_POSITION, Value.get(typeHintsDisk));

            // Create Operation for writing Edge data.
            final Operation createIndividualEdgeMap = MapOperation.put(edgeMapPolicy, db.getConfig().edgeDataBin,
                    Value.get(edgeId), Value.get(edgeData));
            operations.add(createIndividualEdgeMap);
        } else {
            // If the Edge is attached to a supernode, rest of the data has to exist elsewhere so store only type hint
            final Operation createEdgeToTypeHint = MapOperation.put(edgeMapPolicy, db.getConfig().edgeDataBin, Value.get(edgeId),
                    Value.get(typeHintsDisk));
            operations.add(createEdgeToTypeHint);
        }

        // Write to filterable supernode bin if necessary.
        operations.addAll(getAerospikeOperations().createFilterableSupernodeOperations(id, outVSupernode,
                inVSupernode, outId, inId, label, propertyMap));
    }

    public void bulkWriteEdge(final byte[] edgeId, final String label, final List<Map.Entry<String, Object>> properties,
                              final Object inVertexId, final Object outVertexId, final boolean inVSupernode,
                              final boolean outVSupernode, final int partitionId) {
        FireflyGraph.LOG.debug("Writing edge {} [({})-({})->({})] {}.", edgeId, outVertexId, label, inVertexId, properties);

        final List<Operation> operationList = new ArrayList<>();
        generateWriteEdgeOperations(edgeId, label, properties, inVertexId, outVertexId, inVSupernode, outVSupernode, operationList);
        final WritePolicy writePolicy = new WritePolicy();
        final Key key = getKey(db, db.getConfig().edgeAeroSet, getIdFactory().createEdgeId(edgeId));
        try {
            db.writeOperate(writePolicy, key, operationList.toArray(new Operation[0]));
        } catch (final AerospikeGraphException e) {
            throw new FireflyLoadingException(e);
        }
        fireflySummaryUpdater.stageEdgeWriteToQueue(label, properties.stream().map(Map.Entry::getKey).collect(Collectors.toSet()), partitionId);
    }

    /**
     * Function to read edges from Aerospike.
     *
     * @param edgeIds Edge ids.
     * @return Edge.
     */
    public List<FireflyEdge> readEdges(final List<HasContainer> hasContainers, final List<FireflyId> edgeIds, final List<String> requiredProperties, final boolean areEdgesRequired) {
        if (!hasContainers.isEmpty()) {
            throw new RuntimeException("Pushdown is not currently supported for Edges.");
        }
        if (requiredProperties != null && !requiredProperties.isEmpty()) {
            // Should never happen.
            throw new RuntimeException("Required properties are not currently supported for Edges.");
        }
        return aerospikeOperations.readEdges(edgeIds);
    }

    public List<FireflyEdge> readEdges(final List<HasContainer> hasContainers, final List<FireflyId> edgeIds, final List<String> requiredProperties) {
        return readEdges(hasContainers, edgeIds, requiredProperties, true);
    }

    /**
     * Return a Set of all the Graph variable names
     *
     * @return Set of Graph variable keys
     */
    public Set<String> readGraphVariableKeys() {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db,
                db.getConfig().graphVariablesSet,
                idFactory.createGraphVariableId(db.getConfig().graphVariablesRecKey));
        if (fireflyRecord == null) return new HashSet<>();
        final Map<String, ?> m = (Map<String, ?>) fireflyRecord.record().getMap(db.getConfig().graphVariablesBin);
        return m.keySet();
    }

    /**
     * Write a Graph variable
     *
     * @param key   Graph variable key
     * @param value Graph variable value to write
     * @param <V>   Graph variable value type
     */
    public <V> void writeGraphVariable(final String key, final V value) {
        db.writeTypeHintedGraphVariable(db.getConfig().graphVariablesSet,
                db.getIdFactory().createGraphVariableId(db.getConfig().graphVariablesRecKey),
                db.getConfig().graphVariablesBin,
                key,
                value,
                db.getConfig().typeHintsBin);
    }

    /**
     * Return a Graph variable value by name
     *
     * @param key Graph variable key
     * @param <V> type
     * @return Graph variable value
     */
    public <V> V readGraphVariable(final String key) {
        if (Objects.equals(key, FIREFLY_CONFIGURATION_VARIABLE_NAME)) {
            return (V) this.configuration();
        }
        return db.readTypeHintedValueFromMap(
                db.getConfig().graphVariablesSet,
                db.getIdFactory().createGraphVariableId(db.getConfig().graphVariablesRecKey),
                db.getConfig().graphVariablesBin,
                key,
                db.getConfig().typeHintsBin);
    }

    /**
     * Remove a Graph variable
     *
     * @param key Graph variable key to remove
     */
    public void removeGraphVariable(final String key) {
        db.removeTypeHintedValueFromMap(
                db.getConfig().graphVariablesSet,
                db.getIdFactory().createGraphVariableId(db.getConfig().graphVariablesRecKey),
                db.getConfig().graphVariablesBin,
                key,
                db.getConfig().typeHintsBin);
    }

    public long getVertexCount(final List<HasContainer> hasContainers, final Long evaluationTimeout) {
        return FireflyCloseableIteratorUtils.count(this.graphQuery.scanVertexIds(hasContainers, evaluationTimeout));
    }

    public long getEdgeCount(final Long evaluationTimeout) {
        return FireflyCloseableIteratorUtils.count(this.graphQuery.scanEdgeIds(evaluationTimeout));
    }

    /**
     * Count edges matching a specific label without materializing full FireflyEdge objects.
     * Scans phat edge records and checks the on-disk label value directly.
     */
    public long getEdgeCountByLabel(final String label, final Long evaluationTimeout) {
        final Long targetLabel = db.schemaManager.getEdgeLabelRead(label);
        if (targetLabel == null) {
            return 0;
        }
        final Long schemaLabelKey = db.schemaManager.getEdgePropertyRead(FireflyEdge.EDGE_SUPERNODE_LABEL_KEY);
        final Iterator<KeyRecord> records = this.graphQuery.scanEdgeRecords(evaluationTimeout);

        long count = 0;
        try {
            while (records.hasNext()) {
                count += countEdgesInRecordByLabel(records.next(), targetLabel, schemaLabelKey);
            }
        } finally {
            CloseableIterator.closeIterator(records);
        }
        return count;
    }

    private long countEdgesInRecordByLabel(final KeyRecord keyRecord, final Long targetLabel,
                                           final Long schemaLabelKey) {
        final Map<?, ?> edgeDataMap = (Map<?, ?>) keyRecord.record.getMap(db.getConfig().edgeDataBin);
        long count = 0;
        boolean hasSupernodeEdges = false;

        for (final Object value : edgeDataMap.values()) {
            if (value instanceof List) {
                if (targetLabel.equals(((List<?>) value).get(FireflyEdge.LABEL_POSITION))) {
                    count++;
                }
            } else {
                hasSupernodeEdges = true;
            }
        }

        if (hasSupernodeEdges) {
            count += countSupernodeEdgesByLabel(keyRecord, targetLabel, schemaLabelKey);
        }
        return count;
    }

    @SuppressWarnings("unchecked")
    private long countSupernodeEdgesByLabel(final KeyRecord keyRecord, final Long targetLabel,
                                            final Long schemaLabelKey) {
        final Set<Long> counted = new HashSet<>();
        for (final String binName : new String[]{db.getConfig().supernodesOutBin, db.getConfig().supernodesInBin}) {
            final Map<String, Object> bin = (Map<String, Object>) keyRecord.record.getMap(binName);
            if (bin == null) {
                continue;
            }
            for (final Object vertexData : bin.values()) {
                final Map<Long, Object> propMap = (Map<Long, Object>) vertexData;
                final Map<Long, Long> labelMap = (Map<Long, Long>) propMap.get(schemaLabelKey);
                if (labelMap == null) {
                    continue;
                }
                for (final Map.Entry<Long, Long> entry : labelMap.entrySet()) {
                    if (targetLabel.equals(entry.getValue()) && counted.add(entry.getKey())) {
                        // counted.add returns true if the element was new (not a duplicate)
                    }
                }
            }
        }
        return counted.size();
    }

    @Override
    public AerospikeConnection getBaseGraph() {
        return db;
    }

    public AerospikeOperations getAerospikeOperations() {
        return aerospikeOperations;
    }

    public FireflyRecordLockHandler getRecordLockHandler() {
        return fireflyRecordLockHandler;
    }

    @Override
    public Features features() {
        return features;
    }

    private Optional<Vertex> returnVertexIfEquivalent(final FireflyId id, final String label, final List<Map.Entry<String, Object>> properties) {
        try {
            final FireflyVertex v = readVertex(id);
            if (v != null) {
                // Check that all properties match.
                if (!v.label().equals(label)) {
                    return Optional.empty();
                }

                int count = 0;
                for (final Map.Entry<String, Object> entry : properties) {
                    if (entry.getKey().startsWith("~") || entry.getKey().startsWith("___")) {
                        // Skip special internal properties.
                        continue;
                    }

                    // If there is a property mismatch, then the key already existing is not valid.
                    count++;
                    final Iterator<VertexProperty<Object>> ps = v.properties(entry.getKey());
                    if (!ps.hasNext()) {
                        return Optional.empty();
                    }
                    final VertexProperty<Object> p = ps.next();
                    if (!p.value().equals(entry.getValue()) || ps.hasNext()) {
                        return Optional.empty();
                    }
                }

                // Unskipped property count must match actual property count if they are equivalent.
                if (IteratorUtils.count(v.properties()) != count) {
                    return Optional.empty();
                }

                // Check the edge caches for edges.
                if (!v.inEdgeIds.isEmpty() || !v.outEdgeIds.isEmpty()) {
                    // If the edge caches contain anything, then the key already existing is not valid.
                    return Optional.empty();
                }
                final Map.Entry<String, Object> supernodeFlag = properties.stream().filter(entry -> entry.getKey().equals(SUPERNODE_PROPERTY_KEY)).findFirst().orElse(null);
                if (!v.isEdgeCacheOverflowed) {
                    // A vertex must be a supernode if this was set, but not the other way around.
                    if (supernodeFlag != null) {
                        // A vertex must be a supernode if this is true, but not the other way around.
                        if ((supernodeFlag.getValue() instanceof Boolean) && (boolean) supernodeFlag.getValue()) {
                            return Optional.empty();
                        }
                    }
                } else {
                    // No choice but to pull the index.
                    for (final Direction direction : List.of(Direction.IN, Direction.OUT)) {
                        try (final CloseableIterator<FireflyId> edgeIdItty
                                     = CloseableIterator.of(v.getSupernodeEdgeIds(direction, Collections.emptySet(), Collections.emptyList()))) {
                            if (edgeIdItty.hasNext()) {
                                return Optional.empty();
                            }
                        }
                    }
                }
                return Optional.of(v);
            } else {
                // Tried to read it back and it isn't there. Could be a concurrent delete.
                return Optional.empty();
            }
        } catch (final AerospikeGraphException age) {
            // This is such a bad scenario, just throw the error back to the user.
            throw new VertexAlreadyExistsFailure(age.getMessage());
        }
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        // Validate key value pairs are valid for TinkerPop.
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        final List<Map.Entry<String, Object>> properties = convertFullyQualified(
                this.features().vertex().supportsNullPropertyValues(),
                keyValues);

        // Create a new id or use the provided user-supplied id (if present and supported).
        final Optional<Object> id = ElementHelper.getIdValue(keyValues);
        final boolean isUserSuppliedId = id.isPresent();
        FireflyId idValue = isUserSuppliedId ? getIdFactory().createVertexId(id.get()) : getIdFactory().generateId(this, FireflyVertex.class);
        while (true) {
            try {
                final Vertex v = writeVertex(idValue, label, properties);
                if (db.getConfig().isAuditLogEnabled) {
                    LOG.info("[{}] created vertex with id: {}", USER.get(), idValue.getUserId());
                }
                return v;
            } catch (final AerospikeGraphException e) {
                if (e.errorCode == ResultCode.KEY_EXISTS_ERROR) {
                    if (!(e.getCause() instanceof AerospikeException)) {
                        // This should never happen.
                        throw e;
                    }

                    final AerospikeException ae = (AerospikeException) e.getCause();
                    // We got a straightforward ID collision.
                    if (!ae.getInDoubt()) {
                        if (isUserSuppliedId) {
                            throw Graph.Exceptions.vertexWithIdAlreadyExists(idValue.getUserId());
                        } else {
                            // Retry with next ID.
                            idValue = getIdFactory().generateId(this, FireflyVertex.class);
                            continue;
                        }
                    }

                    // The ID collision could have been caused by the client retrying, so we do our best to check.
                    final Optional<Vertex> equivalentVertex = returnVertexIfEquivalent(idValue, label, properties);
                    if (equivalentVertex.isPresent()) {
                        return equivalentVertex.get();
                    } else {
                        if (isUserSuppliedId) {
                            // User-supplied ID and it already exists but is not equivalent.
                            throw Graph.Exceptions.vertexWithIdAlreadyExists(idValue.getUserId());
                        } else {
                            idValue = getIdFactory().generateId(this, FireflyVertex.class);
                        }
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    public void setUser(final String user) {
        USER.set(user);
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
            properties.add(new AbstractMap.SimpleEntry<>(key, value));
        }

        return properties;
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        try {
            final Class<C> clazz = (Class<C>) Class.forName("com.aerospike.firefly.olap.structure.DistributedGraphComputer");
            return clazz.getConstructor(FireflyGraph.class, Object.class).newInstance(this, sparkSession);
        } catch (final InvocationTargetException | NoSuchMethodException | ClassNotFoundException |
                       InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new IllegalStateException("ERROR: To use Aerospike Graph Analytics, use the docker image with Aerospike Graph Analytics support or a Spark cluster.", e);
        }
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        // path to olap jars
        Arrays.stream(System.getProperty("java.class.path").split(":")).forEach(s -> {
            if (s.contains("olap")) {
                System.out.println(s);
            }
        });
        try {
            final Class<? extends GraphComputer> clazz = (Class<? extends GraphComputer>) Class.forName("com.aerospike.firefly.olap.structure.DistributedGraphComputer");
            return clazz.getConstructor(FireflyGraph.class, Object.class).newInstance(this, sparkSession);
        } catch (final InvocationTargetException | NoSuchMethodException | ClassNotFoundException |
                       InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new IllegalStateException("ERROR: To use Aerospike Graph Analytics, use the docker image with Aerospike Graph Analytics support or a Spark cluster.", e);
        }
    }

    /**
     * This function finds TinkerPop Element objects in an object array and converts them to ids
     *
     * @param elements array of objects that might be Elements
     * @return array of raw ids
     */
    private List<Object> getIds(final List<Object> elements) {
        return elements.stream().map(e -> {
            if (e != null && Element.class.isAssignableFrom(e.getClass())) {
                return ((Element) e).id();
            } else {
                return e;
            }
        }).collect(Collectors.toList());
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIdsOrVertices) {
        final Iterator<Vertex> iterator = vertices(List.of(), null, vertexIdsOrVertices);
        // TODO: GRAPH COMPUTER INTERCEPTION
        return FireflyHelper.inComputerMode(this) ?
                FireflyCloseableIteratorUtils.filter(iterator, vertex -> this.graphComputerView.legalVertex(vertex)) :
                iterator;
    }

    public Iterator<Vertex> vertices(final List<HasContainer> filters, final List<String> requiredProperties, final Object... vertexIdsOrVertices) {
        final Iterator<FireflyId> idsIterator;
        if (vertexIdsOrVertices.length == 0) {
            idsIterator = this.graphQuery.scanVertexIds(settings().evaluationTimeout);
        } else {
            idsIterator = getIds(Arrays.asList(vertexIdsOrVertices)).stream()
                    .filter(Objects::nonNull)
                    .map(id -> getIdFactory().createVertexId(id)).iterator();
        }

        // Create vertex iterator with graph and vertex id iterator.
        // If there are vertexIds present use them, otherwise read from database.
        return new FireflyBatchElementIterator<>(this, idsIterator, filters, this::readVertices, requiredProperties);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        final Iterator<Edge> iterator;
        if (edgeIds.length == 0) {
            iterator = new FireflyPhatEdgeScanIterator(
                    this.graphQuery.scanEdgeRecords(settings().evaluationTimeout), this);
        } else {
            final List<FireflyId> idList = new ArrayList<>(edgeIds.length);
            for (final Object edgeId : edgeIds) {
                final Object rawId = (edgeId instanceof Element) ? ((Element) edgeId).id() : edgeId;
                if (rawId != null) {
                    idList.add(getIdFactory().createEdgeId(rawId));
                }
            }
            iterator = (Iterator<Edge>) (Iterator<?>) aerospikeOperations.readEdges(idList).iterator();
        }
        // TODO: GRAPH COMPUTER INTERCEPTION
        return FireflyHelper.inComputerMode(this) ?
                FireflyCloseableIteratorUtils.filter(iterator, edge -> this.graphComputerView.legalEdge(edge.outVertex(), edge)) :
                iterator;
    }

    public interface TransformKeyRecord<E> {
        E transform(final KeyRecord keyRecord);
    }

    public void createIndexes(final Class<? extends FireflyElement> elementClass,
                              final String binName,
                              final String prefix,
                              final Collection<String> vertexPropertyIndexes,
                              final IndexType indexType,
                              final boolean errorOnDuplicate) {
        final List<String> existingIndexes =
                AerospikeConnection.InfoOps.listExistingIndexes(db).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());

        // Add indexes specified in properties file.
        for (final String index : vertexPropertyIndexes) {
            // Create both string and numeric indexes for vertex properties.
            final String formattedIndex = String.format("%s_%s", prefix, index);
            final Long indexSchema = db.schemaManager.getVertexPropertyWrite(index);
            db.createIndexBackground(existingIndexes, db.setFromElementType(elementClass),
                    formattedIndex + "_" + indexType, binName, indexType, IndexCollectionType.MAPKEYS, errorOnDuplicate,
                    CTX.mapKey(Value.get(indexSchema)));
        }

        // Manually force metadata to update.
        fireflyIndexMetadata.updateMetadata();
    }

    public void createExpressionIndexes(final String expressionIndexesString) {
        if (expressionIndexesString == null || expressionIndexesString.isBlank()) {
            return;
        }

        db.validateExpressionIndexSupport();

        final List<String> existingIndexes =
                AerospikeConnection.InfoOps.listExistingIndexes(db).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());

        final String[] expressionIndexes = expressionIndexesString.split(";");
        for (final String expressionIndex : expressionIndexes) {
            if (expressionIndex.isBlank()) {
                continue;
            }
            final FireflyExpressionIndex index = FireflyExpressionIndex.fromConfigString(db, expressionIndex.strip());
            db.createExpIndex(existingIndexes, index);
        }

        // Manually force metadata to update.
        fireflyIndexMetadata.updateMetadata();
    }

    public void createVertexPropertyIndexes(final Class<? extends FireflyElement> elementClass,
                                            final String binName,
                                            final String prefix,
                                            final Set<String> stringIndexes,
                                            final Set<String> numericIndexes) {
        createIndexes(elementClass, binName, prefix, stringIndexes, IndexType.STRING, false);
        createIndexes(elementClass, binName, prefix, numericIndexes, IndexType.NUMERIC, false);
    }

    public void createGeoPropertyIndexes(final Collection<String> geoPropertyIndexes) {
        if (geoPropertyIndexes == null || geoPropertyIndexes.isEmpty()) {
            return;
        }
        final List<String> existingIndexes =
                AerospikeConnection.InfoOps.listExistingIndexes(db).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());
        for (final String index : geoPropertyIndexes) {
            createGeoPropertyIndex(existingIndexes, index, false);
        }
        fireflyIndexMetadata.updateMetadata();
    }

    public void createGeoPropertyIndexIfNeeded(final String propertyKey) {
        final List<String> existingIndexes =
                AerospikeConnection.InfoOps.listExistingIndexes(db).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());
        createGeoPropertyIndex(existingIndexes, propertyKey, false);
        fireflyIndexMetadata.updateMetadata();
    }

    private void createGeoPropertyIndex(final List<String> existingIndexes,
                                        final String propertyKey,
                                        final boolean errorOnDuplicate) {
        final String formattedIndex = String.format("%s_%s", db.getVpIndexPrefix(), propertyKey);
        final Long indexSchema = db.schemaManager.getGeoPropertyWrite(propertyKey);
        db.createIndexBackground(existingIndexes, db.setFromElementType(FireflyVertex.class),
                formattedIndex + "_" + IndexType.GEO2DSPHERE, db.getConfig().geoDataBin,
                IndexType.GEO2DSPHERE, IndexCollectionType.LIST, errorOnDuplicate,
                CTX.mapKey(Value.get(indexSchema)));
    }

    public void createGeoPropertyIndexAdmin(final String propertyKey) {
        final List<String> existingIndexes =
                AerospikeConnection.InfoOps.listExistingIndexes(db).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());
        createGeoPropertyIndex(existingIndexes, propertyKey, true);
        fireflyIndexMetadata.updateMetadata();
    }

    public boolean isEmpty() {
        final FireflyGraphSummaryUpdater.FireflyElementMetadata metadata = this.fireflySummaryUpdater.getFireflyStatistics();
        return metadata.totalEdgeCount() == 0 && metadata.totalVertexCount() == 0 && metadata.totalSupernodeCount() == 0;
    }

    public void exportQuery(final DefaultTraversalMetrics metrics, final String scopeName, final String traversal) {
        if (this.zipkinExporter == null) {
            // This should never happen.
            LOG.error("Query tracing was not enabled but usage was attempted. Please contact support.");
            return;
        }
        this.zipkinExporter.exportQuery(metrics, scopeName, traversal);
    }

    @Override
    public FireflyTransaction tx() {
        return this.transaction;
    }

    /**
     * Enter transaction state for the current thread.
     *
     * @param timeout Timeout in seconds. -1 to use FireflyGraph's configured default timeout.
     */
    public void enterTransactionState(final long timeout) {
        if (!this.getBaseGraph().getConfig().transactionEnabled) {
            throw new TxNotEnabledException(this.getBaseGraph().getConfig().graphId);
        }
        LOG.atDebug().addArgument(() -> Thread.currentThread().getId()).log("enterTransactionState on Thread: {}");
        this.transaction.enterTransactionState(timeout);
    }

    public void exitTransactionState() {
        LOG.atDebug().addArgument(() -> Thread.currentThread().getId()).log("exitTransactionState on Thread: {}");
        this.transaction.exitTransactionState();
    }

    @Override
    public void close() {
        try {
            // GremlinServer try to close Graph 2 times, we should be prepared
            if (this.closed.getAndSet(true)) {
                return;
            }

            LOG.info("Closing FireflyGraph {}.", getBaseGraph().getConfig().graphId);

            this.fireflyCardinalityMetadataTask.cancel();
            this.fireflyIndexMetadataTask.cancel();
            if (this.fireflySummaryUpdater != null) {
                this.fireflySummaryUpdater.close();
            }

            if (this.configurationWatcherTask != null) {
                this.configurationWatcherTask.cancel();
            }

            if (!db.getConfig().warmupMode && !db.getBulkLoaderFlag() && !db.getOlapFlag() && this.usageStats != null) {
                synchronized (this) {
                    if (this.usageStats != null) {
                        this.usageStats.close();
                        this.usageStats = null;
                    }
                }
            }

            if (httpStarted) {
                HttpServer.getInstance().close();
            }

            if (this.ttlHandler != null) {
                this.ttlHandler.close();
            }

            if (this.transaction != null) {
                this.transaction.close();
            }

            if (this.zipkinExporter != null) {
                this.zipkinExporter.close();
            }

            this.db.close();
        } finally {
            // Restore log level if this was a warmup graph.
            if (this.db.getConfig().warmupMode) {
                ConfigurationHelper.restoreLogLevel(this.configuration);
            }
        }
    }

    @Override
    public Variables variables() {
        return variables;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    public Settings settings() {
        return gremlinServerSettings;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, db.toString());
    }

    @Override
    public ServiceRegistry getServiceRegistry() {
        return this.serviceRegistry;
    }

    public static String getGitCommitHash() {
        final Properties gitProperties = new Properties();
        try (InputStream in = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream("git.properties")) {
            if (in == null) {
                throw new IllegalStateException("git.properties not found on classpath");
            }
            gitProperties.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load git.properties", e);
        }
        return gitProperties.getProperty("git.commit.id");
    }
}
