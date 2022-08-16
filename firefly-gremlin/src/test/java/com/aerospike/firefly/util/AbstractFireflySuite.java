package com.aerospike.firefly.util;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class AbstractFireflySuite {
    protected static final Configuration config;
    protected Logger LOG;
    protected static AerospikeConnection db;
    protected static FireflyGraph graph;

    static {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    }

    @BeforeClass
    public static void openGraph() {
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
    }

    @Before
    public void beforeTest() {
        Util.clearGraph(graph);
        LOG = LoggerFactory.getLogger(this.getClass());
    }

    @AfterClass
    public static void closeGraphClearData() {
        Util.clearGraph(graph);
        graph.close();
        db.close();
    }
}
