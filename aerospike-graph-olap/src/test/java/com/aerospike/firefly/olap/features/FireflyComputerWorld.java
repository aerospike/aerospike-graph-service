package com.aerospike.firefly.olap.features;


import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import io.cucumber.java.Scenario;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.MessagePassingReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.javatuples.Pair;
import org.junit.AssumptionViolatedException;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

// todo: consider creating module for common test code
public class FireflyComputerWorld implements World {
    private static final String configLocation = "../conf/integration-test-settings-packed.properties";

    private static final List<String> TAGS_TO_IGNORE = Arrays.asList(
            "@StepDrop",
            "@StepInject",
            "@StepV",
            "@StepE",
            "@GraphComputerVerificationOneBulk",
            "@GraphComputerVerificationStrategyNotSupported",
            "@GraphComputerVerificationMidVNotSupported",
            "@GraphComputerVerificationInjectionNotSupported",
            "@GraphComputerVerificationStarGraphExceeded",
            "@GraphComputerVerificationReferenceOnly",
            "@TinkerServiceRegistry",
            "@StepMatch"); // problem with labels

    private static final String skipReasonHang = "This test hangs.";
    private static final String skipStackOverflow = "Stack overflow.";
    private static final String skipUnion = "Union is not supported.";
    private static final String skipIndex = "Index is not supported.";
    private static final String skipPathEncoding = "Path encoding is not supported."; //g.V().has("person","name","marko").path().as("a").union(identity(),identity()).select("a").unfold().toList();
    private static final String skipSack = "Sack encoding is not supported.";
    private static final List<Pair<String, String>> SKIP_TESTS = new ArrayList<>() {
        {
            add(Pair.with("g_V_repeatXboth_simplePathX_timesX3X_path", skipReasonHang));

            add(Pair.with("g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX", skipStackOverflow));

            add(Pair.with("g_unionXX", skipUnion));

            add(Pair.with("g_V_hasLabelXsoftwareX_index_unfold", skipIndex));
            add(Pair.with("g_V_hasLabelXsoftwareX_order_byXnameX_index_withXmapX", skipIndex));
            add(Pair.with("g_V_hasLabelXsoftwareX_name_fold_orderXlocalX_index_unfold_order_byXtailXlocal_1XX", skipIndex));
            add(Pair.with("g_V_hasLabelXpersonX_name_fold_orderXlocalX_index_withXmapX", skipIndex));
            add(Pair.with("g_VX1X_valuesXageX_index_unfold_unfold", skipIndex));

            add(Pair.with("g_V_hasXperson_name_markoX_elementMapXnameX_asXaX_unionXidentity_identityX_selectXaX_selectXnameX", skipPathEncoding));
            add(Pair.with("g_V_hasXperson_name_markoX_path_asXaX_unionXidentity_identityX_selectXaX_unfold", skipPathEncoding));

            add(Pair.with("g_withSackX0X_V_outE_sackXsumX_byXweightX_inV_sack_sum", skipSack));
            add(Pair.with("g_withSackX0X_V_repeatXoutE_sackXsumX_byXweightX_inVX_timesX2X_sack", skipSack));
            add(Pair.with("g_V_sackXassignX_byXageX_sack", skipSack));
            add(Pair.with("g_withSackXhelloX_V_outE_sackXassignX_byXlabelX_inV_sack", skipSack));
        }
    };


    private static final FireflyGraph modern;
    private static final FireflyGraph crew;
    private static final FireflyGraph sink;
    private static final FireflyGraph grateful;

    static {
        modern = createFireflyGraph("modern", TinkerFactory.createModern());
        crew = createFireflyGraph("crew", TinkerFactory.createTheCrew());
        sink = createFireflyGraph("sink", TinkerFactory.createKitchenSink());
        // kryo failed on grateful construction, so commented out for now
        grateful = null; //createFireflyGraph("grateful", TinkerFactory.createGratefulDead());
    }

    @Override
    public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
        if (null == graphData)
            throw new AssumptionViolatedException("GraphComputer does not support mutation");
        else if (graphData == LoadGraphWith.GraphData.CREW)
            return crew.traversal().withComputer().withoutStrategies(MessagePassingReductionStrategy.class);
        else if (graphData == LoadGraphWith.GraphData.MODERN)
            return modern.traversal().withComputer().withoutStrategies(MessagePassingReductionStrategy.class);
        else if (graphData == LoadGraphWith.GraphData.SINK)
            return sink.traversal().withComputer().withoutStrategies(MessagePassingReductionStrategy.class);
        else if (graphData == LoadGraphWith.GraphData.GRATEFUL)
            throw new AssumptionViolatedException("grateful graph contains vertices with multi property not supported by Firefly.");
        else if (graphData == LoadGraphWith.GraphData.CLASSIC)
            throw new AssumptionViolatedException("Classic graph contains Float property values not supported by Firefly.");
        else
            throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());
    }

    protected static Configuration getConfiguration(final String graphName) {
        final Configuration config = ConfigurationHelper.loadFromFile(Path.of(configLocation));

        config.setProperty(ConfigurationHelper.Keys.GRAPH_ID, graphName);
        config.setProperty(ConfigurationHelper.Keys.TRAVERSAL_NAME, "g" + graphName);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
        config.setProperty(ConfigurationHelper.Keys.AUTO_PRE_HEAT.toLowerCase(), "false");

        return config;
    }

    protected static FireflyGraph createFireflyGraph(final String graphName, final Graph graph) {
        final FireflyGraph firefly = FireflyGraph.open(getConfiguration(graphName));
        firefly.getBaseGraph().dropDatabase(firefly, false);
        GraphHelper.cloneElements(graph, firefly);
        return firefly;
    }

    @Override
    public String changePathToDataFile(final String pathToFileFromGremlin) {
        return ".." + File.separator + pathToFileFromGremlin;
    }

    @Override
    public void beforeEachScenario(final Scenario scenario) {
        final List<String> ignores = TAGS_TO_IGNORE.stream().filter(t -> scenario.getSourceTagNames().contains(t)).collect(Collectors.toList());
        if (!ignores.isEmpty())
            throw new AssumptionViolatedException(String.format("This scenario is not supported with GraphComputer: %s", ignores));

        final Optional<Pair<String, String>> skipped = SKIP_TESTS.stream().
                filter(s -> s.getValue0().equals(scenario.getName())).findFirst();
        if (skipped.isPresent())
            throw new AssumptionViolatedException(skipped.get().getValue1());
    }

    @Override
    public String convertIdToScript(final Object id, final Class<? extends Element> type) {
        if (Edge.class.isAssignableFrom(type))
            return "'" + id.toString() + "'";
        return id.toString();
    }
}
