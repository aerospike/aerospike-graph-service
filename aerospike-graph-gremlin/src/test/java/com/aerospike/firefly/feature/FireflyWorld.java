package com.aerospike.firefly.feature;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

public class FireflyWorld extends FireflyWorldBase {
    private static final FireflyGraph empty;
    private static final FireflyGraph modern;
    private static final FireflyGraph crew;
    private static final FireflyGraph sink;
    private static final FireflyGraph grateful;

    static {
        empty = FireflyGraph.open(getConfiguration("empty", false, false));
        modern = createFireflyGraph("modern", TinkerFactory.createModern(), false, false);
        crew = createFireflyGraph("crew", TinkerFactory.createTheCrew(), false, false);
        sink = createFireflyGraph("sink", TinkerFactory.createKitchenSink(), false, false);
        grateful = createFireflyGraph("grateful", TinkerFactory.createGratefulDead(), false, false);
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
}
