package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestFireflyCompositeIdStrategyIntegration {

    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    public static void beforeAll() {
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase();
        SETUP_GRAPH.close();
    }

    @Test
    public void test() {
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g = graph.traversal();
            Vertex foo = g.addV("foo").next();
            Vertex bar = g.addV("bar").next();
            Edge baz = foo.addEdge("baz", bar);

            System.out.println("g.V().hasLabel('foo').out()");
            List<Vertex> vertexList = g.V().hasLabel("foo").out().toList();
            System.out.println("g.V().hasLabel('foo').in()");
            vertexList = g.V().hasLabel("foo").in().toList();

            System.out.println("g.V().hasLabel('foo').inE()");
            List<Edge> edgeList = g.V().hasLabel("foo").inE().toList();
            System.out.println("g.V().hasLabel('foo').outE()");
            edgeList = g.V().hasLabel("foo").outE("baz").toList();

            System.out.println("g.V().hasLabel('foo').out('baz')");
            vertexList = g.V().hasLabel("foo").out().toList();
            System.out.println("g.V().hasLabel('foo').in('baz')");
            vertexList = g.V().hasLabel("foo").in("baz").toList();

            System.out.println("g.V().hasLabel('foo').inE('baz')");
            edgeList = g.V().hasLabel("foo").inE("baz").toList();
            System.out.println("g.V().hasLabel('foo').outE('baz')");
            edgeList = g.V().hasLabel("foo").outE("baz").toList();
            Assert.assertEquals(1, vertexList.size());
        }
    }
}
