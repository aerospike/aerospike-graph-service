package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class FireflyVertexIdTest {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    static public void beforeAll() {
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
    }

    @AfterClass
    static public void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
    }

    @Test
    public void addUserIdDouble() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        try {
            g.addV("label").property(T.id, 1.2).iterate();
            Assert.fail("Adding Vertex with double ID should have failed");
        } catch (final UnsupportedOperationException e) {
            Assert.assertEquals(Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported().getMessage(), e.getMessage());
        }
    }

    @Test
    public void addUserIdBigDecimal() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        try {
            g.addV("label").property(T.id, new BigDecimal(12)).iterate();
            Assert.fail("Adding Vertex with BigDecimal ID should have failed");
        } catch (final UnsupportedOperationException e) {
            Assert.assertEquals(Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported().getMessage(), e.getMessage());
        }
    }

    @Test
    public void readUserIdDouble() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        try {
            g.V(1.2).next();
            Assert.fail("Reading Vertex with double ID should have failed");
        } catch (final UnsupportedOperationException e) {
            Assert.assertEquals(Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported().getMessage(), e.getMessage());
        }
    }

    @Test
    public void readUserIdBigDecimal() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        try {
            g.V(new BigDecimal(12)).next();
            Assert.fail("Reading Vertex with BigDecimal ID should have failed");
        } catch (final UnsupportedOperationException e) {
            Assert.assertEquals(Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported().getMessage(), e.getMessage());
        }
    }
}
