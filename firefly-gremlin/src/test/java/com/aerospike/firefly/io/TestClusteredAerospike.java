package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.firefly.process.TestAerospikeGraphIntegration;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_CLUSTER_PROPERTIES;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestClusteredAerospike {
    private static Configuration configuration;
    private static AerospikeConnection db;
    Logger LOG = LoggerFactory.getLogger(TestClusteredAerospike.class);
    @BeforeClass
    public static void setup() {
        configuration = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_CLUSTER_PROPERTIES);
        db = AerospikeConnection.connect(configuration);
    }

    @Before
    public void clearGraph() {
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES))) {
            if (graph.traversal().V().count().next() > 0 || graph.traversal().E().count().next() > 0)
                LoggerFactory.getLogger("clearGraph").warn("nonzero vertex or edge count at start of test");
            graph.traversal().V().drop().iterate();
        }
    }

    @AfterClass
    public static void cleanup() {
        db.close();
    }

    //@Test
    public void basicReadWrite() {
        FireflyId id1 = FireflyId.of(FireflyVertex.class, 1);
        FireflyRecord.write(db, db.TEST_SET, id1, new Bin("a", "b"));
        FireflyRecord r = FireflyRecord.read(db, db.TEST_SET, id1);
        assertEquals(r.record().getString("a"), "b");
    }
}
