package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestPropertiesLabelIndexed extends TestProperties {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    static public void beforeAll() {
        CONFIG.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED.toLowerCase(), true);
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
    }

    @AfterClass
    static public void afterAll() {
        CONFIG.clearProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED.toLowerCase());
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
        graph = FireflyGraph.open(CONFIG);
        final GraphTraversalSource g = graph.traversal();
        Vertex person = g.addV("person").next();
        Vertex car = g.addV("vehicle").next();
        g.addE("bought")
                .property("year", "2022")
                .property("month", "dec")
                .from(person).to(car).iterate();
        g.addE("owns")
                .property("year", "2023")
                .property("month", "jan")
                .from(person).to(car).iterate();
    }

    @After
    public void afterEach() {
        graph.getBaseGraph().dropDatabase();
    }
}
