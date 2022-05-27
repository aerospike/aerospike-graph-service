package com.aerospike.firefly.process;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAerospikeGraphIntegration  {
    Logger logger = LoggerFactory.getLogger(TestAerospikeGraphIntegration.class);

    private static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
    }

    private AerospikeConnection db;
    private FireflyGraph graph;


    @BeforeEach
    void openGraph() {
        this.db = AerospikeConnection.connect(ConfigurationHelper.aerospikeHost(config),
                ConfigurationHelper.aerospikePort(config),
                ConfigurationHelper.aerospikeNamespace(config));
        graph = FireflyGraph.open(config);
        db.dropDatabase();
    }

    @AfterEach
    void closeGraphClearData() {
        db.dropDatabase();
        graph.close();
    }
    public void printTraversalForm(final Traversal traversal) {
        logger.info("   pre-strategy:" + traversal);
        if (!traversal.asAdmin().isLocked()) traversal.asAdmin().applyStrategies();
        logger.info("  post-strategy:" + traversal);
    }
    @Test
    @Disabled //requires user supplied ids
    public void g_V_out_out_path_byXnameX_byXageX() {
        Graph tg = TinkerFactory.createModern();

        GraphHelper.cloneElements(tg,graph);
        GraphTraversalSource g = graph.traversal();
        Traversal<Vertex, org.apache.tinkerpop.gremlin.process.traversal.Path> traversal =
                g.V().out().out().path().by("name").by("age");

        this.printTraversalForm(traversal);
        int counter = 0;

        while(traversal.hasNext()) {
            ++counter;
            org.apache.tinkerpop.gremlin.process.traversal.Path path = (Path)traversal.next();
            Assert.assertEquals(3L, (long)path.size());
            Assert.assertEquals("marko", path.get(0));
            Assert.assertEquals(32, (int)path.get(1));
            Assert.assertTrue(path.get(2).equals("lop") || path.get(2).equals("ripple"));
        }

        Assert.assertEquals(2L, (long)counter);
    }



}
