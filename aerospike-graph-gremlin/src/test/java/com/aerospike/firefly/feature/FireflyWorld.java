package com.aerospike.firefly.feature;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import io.cucumber.java.Scenario;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.features.World;
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
import java.util.List;
import java.util.Optional;

public class FireflyWorld implements World {
    private static final String configLocation = "../conf/integration-test-settings-packed.properties";

    private static final String skipReasonOnCreate = "FireflyMergeEdgeStep always verify onCreate.";
    private static final String skipReasonErrorMessage = "Error message includes step name.";

    private static final List<Pair<String, String>> skip = new ArrayList<>() {{
        add(Pair.with("g_mergeEXlabel_knows_out_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated", skipReasonOnCreate));
        add(Pair.with("g_mergeEXout_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated", skipReasonOnCreate));
        add(Pair.with("g_V_mergeEXlabel_knows_out_marko_in_vadasX_optionXonMatch_sideEffectXpropertyXweight_0XX_constantXemptyXX", skipReasonErrorMessage));
    }};

    private static final FireflyGraph empty;
    private static final FireflyGraph modern;
    private static final FireflyGraph crew;
    private static final FireflyGraph sink;
    private static final FireflyGraph grateful;

    static {
        empty = FireflyGraph.open(getConfiguration("empty"));
        modern = createFireflyGraph("modern", TinkerFactory.createModern());
        crew = createFireflyGraph("crew", TinkerFactory.createTheCrew());
        sink = createFireflyGraph("sink", TinkerFactory.createKitchenSink());
        grateful = createFireflyGraph("grateful", TinkerFactory.createGratefulDead());
    }

    private static Configuration getConfiguration(final String graphName) {
        final Configuration config = ConfigurationHelper.loadFromFile(Path.of(configLocation));
        config.setProperty(ConfigurationHelper.Keys.GRAPH_ID, graphName);
        config.setProperty(ConfigurationHelper.Keys.TRAVERSAL_NAME, "g" + graphName);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");

        return config;
    }

    private static FireflyGraph createFireflyGraph(final String graphName, final Graph graph) {
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
    public GraphTraversalSource getGraphTraversalSource(final GraphData graphData) {
        if (null == graphData) {
            empty.getBaseGraph().dropDatabase(empty, false);
            return empty.traversal();
        } else if (graphData == GraphData.CREW)
            return crew.traversal();
        else if (graphData == GraphData.MODERN)
            return modern.traversal();
        else if (graphData == GraphData.SINK)
            return sink.traversal();
        else if (graphData == GraphData.GRATEFUL)
            return grateful.traversal();
        else if (graphData == GraphData.CLASSIC)
            throw new UnsupportedOperationException("Classic graph contains Float property values not supported by Firefly.");
        else
            throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());
    }

    @Override
    public void beforeEachScenario(final Scenario scenario) {
        final Optional<Pair<String, String>> skipped = skip.stream().
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
