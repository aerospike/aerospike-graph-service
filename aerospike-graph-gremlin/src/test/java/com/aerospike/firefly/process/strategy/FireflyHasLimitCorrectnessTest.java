package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class FireflyHasLimitCorrectnessTest {

    @Test
    public void testHasLimitCorrectness() {
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            for (int i = 0; i < 1000; i++) {
                g.addV().property(T.id, i).property("name", "vertex" + i).property("name2", 1.0).next();
            }

            for (int i = 0; i < 1000; i++) {
                g.addE("edge").from(__.V(i)).to(__.V((i + 1) % 1000)).property("weight", i).property("name2", 1.0).next();
            }
            GraphTraversal t1 = g.V().order().by(T.id).out().limit(1).has("name", "vertex500").has("name2", 1.0);
            GraphTraversal t2 = g.V().order().by(T.id).out().has("name", "vertex500").has("name2", 1.0).limit(1);

            t1.asAdmin().applyStrategies();
            t2.asAdmin().applyStrategies();

            List<Vertex> vs1 = g.V().order().by(T.id).out().limit(1).has("name", "vertex500").has("name2", 1.0).toList();
            List<Vertex> vs2 = g.V().order().by(T.id).out().has("name", "vertex500").limit(1).toList();
            List<Vertex> vs3 = g.V().order().by(T.id).out().has("name", "vertex500").limit(1).has("name2", 1.0).toList();
            List<Vertex> vs4 = g.V().order().by(T.id).out().has("name", "vertex500").limit(50).toList();

            List<Edge> es1 = g.V().order().by(T.id).outE().limit(1).has("weight", 500).has("name2", 1.0).toList();
            List<Edge> es2 = g.V().order().by(T.id).outE().has("weight", 500).limit(1).has("name2", 1.0).toList();

            Assert.assertTrue(vs1.isEmpty() && vs2.size() == 1 && vs2.get(0).id().equals(500) && vs3.size() == 1 && vs3.get(0).id().equals(500));
            Assert.assertTrue(es1.isEmpty() && es2.size() == 1);

        }

    }
}
