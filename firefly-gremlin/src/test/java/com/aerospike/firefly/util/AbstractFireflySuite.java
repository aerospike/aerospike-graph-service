package com.aerospike.firefly.util;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.GraphFactory;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class AbstractFireflySuite {
    @Rule public TestName testName = new TestName();
    private Instant start = Instant.now();
    private boolean isTestStarted = true;
    protected static final Configuration config;
    protected static Logger LOG;
    protected static AerospikeConnection db;
    protected static FireflyGraph graph;


    protected abstract boolean clearData();
    protected boolean runTest() {
        return true;
    }

    static {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    }

    @BeforeClass
    public static void openGraph() {
        LOG = LoggerFactory.getLogger(AbstractFireflySuite.class);
        db = AerospikeConnection.connect(config);
        db.dropDatabase();
        graph = GraphFactory.createGraph(db,config);
    }

    @Before
    public void beforeTest() {
        // Test check to see if we should run this test.
        Assume.assumeTrue(runTest());
        LOG.info("===> Running " + testName.getMethodName() + " <===");
        if (clearData()) {
            this.isTestStarted = false;
            Util.cleanAndVerifyGraph(graph);
        }
        this.isTestStarted = true;
        start = Instant.now();
    }

    @After
    public void printTestTime() {
        if (isTestStarted) {
            LOG.info("===> " + testName.getMethodName() + " - " + Duration.between(start, Instant.now()).toMillis() + " ms <===");
        } else {
            LOG.error("===> " + testName.getMethodName() + " did not start <===");
        }
    }

    @AfterClass
    public static void closeGraphClearData() {
        Util.cleanAndVerifyGraph(graph);
        graph.close();
        db.close();
    }
}
