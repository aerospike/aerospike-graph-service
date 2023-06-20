package com.aerospike.firefly.io;

import com.aerospike.firefly.io.impl.Upgrade;
import com.aerospike.firefly.io.impl.relational.RelationalGraph;
import com.aerospike.firefly.io.impl.relational.packed.PackedGraph;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.fail;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestDataModelVersioning {
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    protected static Configuration config;
    final private Logger LOG = LoggerFactory.getLogger(TestDataModelVersioning.class);
    protected static AerospikeConnection db;
    protected static FireflyGraph graph;
    private static GraphTraversalSource g;

    static {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_READ_THROUGH_CACHE.toLowerCase(), "false");
        config.setProperty(ConfigurationHelper.Keys.ENABLE_PREFETCH_STRATEGY.toLowerCase(), "false");
    }

    @After
    public void cleanDataModelTest() {
        // This can cause failures later if you don't clean it up.
        db = AerospikeConnection.connect(config);
        db.dropDatabase(graph, false);
    }

    public static void openGraphStarPacked() {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.FIREFLY_DATA_MODEL.toLowerCase(), StarPackedGraph.DATA_MODEL);
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        g = graph.traversal();
    }

    public static void openGraphLinkedNewVersion() {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.FIREFLY_DATA_MODEL.toLowerCase(), PackedGraph.DATA_MODEL);
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        g = graph.traversal();
    }

    public static void openGraphPacked() {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.FIREFLY_DATA_MODEL.toLowerCase(), PackedGraph.DATA_MODEL);
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        g = graph.traversal();
    }

    @Test
    public void TestFailOnDiffModel() {
        db = AerospikeConnection.connect(config);
        db.dropDatabase(graph, false);
        openGraphStarPacked();
        graph.close();
        db.close();
        exit.expectSystemExitWithStatus(1);
        openGraphPacked();
    }


    public static class FakeGraph extends RelationalGraph {

        private static final String DATA_MODEL = "FAKE";
        private static String version = "0.0.1";

        /**
         * Constructor for PackedGraph.
         *
         * @param db   AerospikeConnection.
         * @param conf Configuration.
         */
        public FakeGraph(AerospikeConnection db, Configuration conf) {
            super(db, conf);
        }

        @Override
        protected int getTypeHint() {
            return 0;
        }

        public static ComparableVersion dataModelVersion() {
            return new ComparableVersion(FakeGraph.version);
        }

        @Override
        public String getDataModel() {
            return getDataModelName();
        }

        public static String getDataModelName() {
            return DATA_MODEL;
        }

        @Override
        public <V> FireflyVertexProperty<V> writeVertexProperty(FireflyId vertexPropertyId, FireflyVertex vertex, String key, V value, Object... args) {
            return null;
        }
    }

    public static class TestTask implements UpgradeTask {
        public TestTask(){

        }
        public static boolean complete = false;

        @Override
        public Map.Entry<String, String> upgradePath() {
            return new AbstractMap.SimpleEntry<>("0.0.1", "0.0.2");
        }

        @Override
        public Class<? extends FireflyGraph> dataModel() {
            return FakeGraph.class;
        }

        @Override
        public void performUpgrade(AerospikeConnection db) throws Exception {
            TestTask.complete = true;
        }
    }

    @Test
    public void testVersionMismatchErrors() throws Exception {
        // Start graph with version 0.0.1.
        db = AerospikeConnection.connect(config);
        db.dropDatabase(graph, false);
        FakeGraph.version = "0.0.1";
        Assert.assertFalse(Upgrade.checkNeedsUpgrade(FakeGraph.class, db));

        // Now try to open version 0.0.2, this should return that an upgrade is required, however since upgrades
        // are not currently supported, it should throw an exception when the performUpgrade function is called.
        db.close();
        FakeGraph.version = "0.0.2";
        db = AerospikeConnection.connect(config);
        Assert.assertTrue(Upgrade.checkNeedsUpgrade(FakeGraph.class, db));
        Assert.assertThrows(RuntimeException.class, () -> Upgrade.performUpgrade(FakeGraph.class, db));


        // Now knock out the current data and place it to version 0.0.2.
        db.dropDatabase(graph, false);
        Assert.assertFalse(Upgrade.checkNeedsUpgrade(FakeGraph.class, db));

        // Try to open version 0.0.1, this should throw an exception since you cannot upgrade from 0.0.2 to 0.0.1.
        FakeGraph.version = "0.0.1";
        db.close();
        db = AerospikeConnection.connect(config);
        Assert.assertThrows(RuntimeException.class, () -> Upgrade.checkNeedsUpgrade(FakeGraph.class, db));

    }

    @Test
    @Ignore
    // TODO: Once we implement upgrade, add this test back.
    public void TestTriggerUpgrade() throws Exception {

        db = AerospikeConnection.connect(config);
        db.dropDatabase(graph, false);

        Upgrade.registerUpgradeTask(TestTask.class);

        FakeGraph.version = "0.0.1";
        if (Upgrade.checkNeedsUpgrade(FakeGraph.class, db))
            Upgrade.performUpgrade(FakeGraph.class, db);
        db.close();

        FakeGraph.version = "0.0.2";
        db = AerospikeConnection.connect(config);
        if (Upgrade.checkNeedsUpgrade(FakeGraph.class, db))
            Upgrade.performUpgrade(FakeGraph.class, db);

        if (!TestTask.complete)
            fail("TestTask should be marked complete");

    }

    @Test
    public void TestFailOnLaterVersion() throws Exception {
        db = AerospikeConnection.connect(config);
        db.dropDatabase(graph, false);
        FakeGraph.version = "0.0.2";
        if (Upgrade.checkNeedsUpgrade(FakeGraph.class, db))
            Upgrade.performUpgrade(FakeGraph.class, db);
        db.close();

        boolean success = false;
        FakeGraph.version = "0.0.1";
        try {
            db = AerospikeConnection.connect(config);
            if (Upgrade.checkNeedsUpgrade(FakeGraph.class, db))
                Upgrade.performUpgrade(FakeGraph.class, db);
        } catch (Exception e) {
            success = true;
        }
        if (!success)
            fail("should throw exception if on disk model is greater then program model");
    }

}
