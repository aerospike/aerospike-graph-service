package com.aerospike.firefly.feature;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

import java.io.File;
import java.nio.file.Path;

public class FireflyWorld implements World {
    private static final String configLocation = "../conf/integration-test-settings-packed.properties";

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
        }
        else if (graphData == GraphData.CREW)
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
    public String convertIdToScript(final Object id, final Class<? extends Element> type) {
        if (Edge.class.isAssignableFrom(type))
            return "'" + id.toString() + "'";
        return id.toString();
    }
}
