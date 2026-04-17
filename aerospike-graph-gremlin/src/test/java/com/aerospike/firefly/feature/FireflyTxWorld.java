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

package com.aerospike.firefly.feature;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

public class FireflyTxWorld extends FireflyWorldBase {
    private static final FireflyGraph empty;
    private static final FireflyGraph modern;
    private static final FireflyGraph crew;
    private static final FireflyGraph sink;
    private static final FireflyGraph grateful;

    static {
        empty = FireflyGraph.open(getConfiguration("empty", false, true));
        modern = createFireflyGraph("modern", TinkerFactory.createModern(), false, true);
        crew = createFireflyGraph("crew", TinkerFactory.createTheCrew(), false, true);
        sink = createFireflyGraph("sink", TinkerFactory.createKitchenSink(), false, true);
        grateful = createFireflyGraph("grateful", TinkerFactory.createGratefulDead(), false, true);
    }

    @Override
    public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
        if (null == graphData) {
            empty.getBaseGraph().dropDatabase(empty, false);
            return empty.traversal();
        } else if (graphData == LoadGraphWith.GraphData.CREW)
            return crew.traversal();
        else if (graphData == LoadGraphWith.GraphData.MODERN)
            return modern.traversal();
        else if (graphData == LoadGraphWith.GraphData.SINK)
            return sink.traversal();
        else if (graphData == LoadGraphWith.GraphData.GRATEFUL)
            return grateful.traversal();
        else if (graphData == LoadGraphWith.GraphData.CLASSIC)
            throw new UnsupportedOperationException("Classic graph contains Float property values not supported by Firefly.");
        else
            throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());
    }
}
