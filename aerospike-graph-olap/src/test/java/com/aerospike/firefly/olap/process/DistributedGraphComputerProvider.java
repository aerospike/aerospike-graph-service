package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.structure.DistributedGraphComputer;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyGraphVariables;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.aerospike.firefly.olap.Tokens.INTEGRATION_TEST_PROPERTIES;

@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest",
        method = "*",
        reason = "The io() step is not supported generally by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest",
        method = "*",
        reason = "The io() step is not supported generally by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",
        method = "*",
        reason = "todo: fix profile serialization stack overflow")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_injectXg_VX1X_propertiesXnameX_nextX_value",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeTest",
        method = "*",
        reason = "Tree is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest",
        method = "*",
        reason = "Sack encoding is not supported.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest",
        method = "allShortestPaths",
        reason = "LinkedHashMap in Path is not supported.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectTest",
        method = "g_withSideEffectXa__linkedhashmapX_V_out_groupCountXaX_byXlabelX_out_out_capXaX",
        reason = "Tests that include lambdas are not supported.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest",
        method = "classicRecommendation",
        reason = "Require GRATEFUL graph with multi-properties.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest",
        method = "coworkerSummary",
        reason = "unstable test, check later.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategyProcessTest",
        method = "shouldSeedGlobalSample",
        reason = "Require GRATEFUL graph with multi-properties.")

@GraphProvider.Descriptor(computer = DistributedGraphComputer.class)
public class DistributedGraphComputerProvider extends AbstractGraphProvider {

    private FireflyGraph TEST_GRAPH; // don't close database, simply drop database between tests
    private static final Random RANDOM = TestHelper.RANDOM;
    private static final Configuration config;

    private static final Set<Class> IMPLEMENTATION = new HashSet<>() {{
        add(FireflyGraph.class);
        add(FireflyGraphVariables.class);
        add(FireflyEdge.class);
        add(FireflyVertex.class);
        add(FireflyVertexProperty.class);
    }};

    static {
        // config = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_CLUSTER_PROPERTIES);
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);

        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
        config.setProperty(ConfigurationHelper.Keys.AUTO_PRE_HEAT.toLowerCase(), "false");
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_CARDINALITY, "list");
    }

    @Override
    public Graph openTestGraph(final Configuration config) {
        if (null == TEST_GRAPH) {
            TEST_GRAPH = (FireflyGraph) GraphFactory.open(config);
        }
        return TEST_GRAPH;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName,
                                                    final Class<?> test,
                                                    final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {
        // Load config map with base config
        final HashMap<String, Object> configMap = new HashMap<>();
        config.getKeys().forEachRemaining(key -> configMap.put(key, config.get(Object.class, key)));

        return configMap;
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) {
        if (TEST_GRAPH == null || TEST_GRAPH.closed.get())
            TEST_GRAPH = FireflyGraph.open(configuration);
        TEST_GRAPH.getBaseGraph().dropDatabase(TEST_GRAPH, false);
        System.gc();
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        return graph.traversal().withStrategies(
                VertexProgramStrategy.build()
                        .workers(RANDOM.nextInt(3) + 1)            // number of parallel threads
                        .graphComputer(RANDOM.nextBoolean() ?      // verifying semantics of api
                                GraphComputer.class :
                                DistributedGraphComputer.class).create());
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }
}
