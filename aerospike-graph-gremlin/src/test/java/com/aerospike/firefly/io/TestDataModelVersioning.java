package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.DataModelVersioning;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.DataModelVersionMismatchException;
import com.aerospike.firefly.util.exceptions.GraphError;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestDataModelVersioning {
    private static Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private AerospikeConnection db;

    @AfterClass
    static public void afterAll() {
        try (final AerospikeConnection db = AerospikeConnection.connect(CONFIG)) {
            db.dropDatabase(null, false);
        }
    }

    private static String getAdjustedVersion(final int versionIndex, final int delta) {
        String fireflyVersion = FireflyGraph.FIREFLY_VERSION.toUpperCase();
        if (fireflyVersion.endsWith("-SNAPSHOT")) {
            fireflyVersion = fireflyVersion.replace("-SNAPSHOT", "");
        }
        String[] splitVersion = fireflyVersion.split("\\.");
        // This isn't a problem since major version at time of writing is >= 2.
        // For minor version, we only test by adjusting this positively.
        // For patch version, this is the only time this can become negative when on x.x.0, but it doesn't matter since it doesn't affect compatibility.
        splitVersion[versionIndex] = String.valueOf(Math.abs(Integer.parseInt(splitVersion[versionIndex]) + delta));
        return StringUtils.join(splitVersion, '.');
    }

    private static String getAdjustedMajorVersion(final int delta) {
        return getAdjustedVersion(0, delta);
    }

    private static String getAdjustedMinorVersion(final int delta) {
        return getAdjustedVersion(1, delta);
    }

    private static String getAdjustedPatchVersion(final int delta) {
        return getAdjustedVersion(2, delta);
    }

    @Before
    public void beforeEach() {
        // Set graph to current version.
        db = AerospikeConnection.connect(CONFIG);
        db.dropDatabase(null, false);
        db.setGraphMetadata(FireflyGraph.getDataModelName(), FireflyGraph.dataModelVersion().toString());
        DataModelVersioning.checkVersionCompatibility(db);
    }

    @After
    public void afterEach() {
        // This can cause failures later if you don't clean it up.
        db.dropDatabase(null, false);
        db.close();
    }

    @Test
    public void testMajorVersionMatch() {
        // Now try to open with disk patch version being older. This should return no issue since the major version matches.
        db.setGraphMetadata(FireflyGraph.getDataModelName(), getAdjustedPatchVersion(-1));
        DataModelVersioning.checkVersionCompatibility(db);

        // Now try to open with disk patch version being newer. This should return no issue since the major version matches.
        db.setGraphMetadata(FireflyGraph.getDataModelName(), getAdjustedPatchVersion(1));
        DataModelVersioning.checkVersionCompatibility(db);
    }

    @Test
    public void TestFailOnLaterMajorVersion() {
        db.setGraphMetadata(FireflyGraph.getDataModelName(), getAdjustedMajorVersion(-1));
        try {
            DataModelVersioning.checkVersionCompatibility(db);
            Assert.fail("Should not have passed check when AGS major version > disk version.");
        } catch (final AerospikeGraphException e) {
            Assert.assertEquals(GraphError.DATA_MODEL_VERSION_MISMATCH.code, e.errorCode);
        }
    }

    @Test
    public void TestFailOnEarlierMajorVersion() {
        db.setGraphMetadata(FireflyGraph.getDataModelName(), getAdjustedMajorVersion(1));
        try {
            DataModelVersioning.checkVersionCompatibility(db);
            Assert.fail("Should not have passed check when AGS major version < disk version.");
        } catch (final AerospikeGraphException e) {
            Assert.assertEquals(GraphError.DATA_MODEL_VERSION_MISMATCH.code, e.errorCode);
        }
    }

    @Test
    public void TestFailOnEarlierMinorVersion() {
        db.setGraphMetadata(FireflyGraph.getDataModelName(), getAdjustedMinorVersion(1));
        try {
            DataModelVersioning.checkVersionCompatibility(db);
            Assert.fail("Should not have passed check when AGS minor version < disk version.");
        } catch (final AerospikeGraphException e) {
            Assert.assertEquals(GraphError.DATA_MODEL_VERSION_MISMATCH.code, e.errorCode);
        }
    }

    @Test
    public void TestClearDataConfig() {
        final Configuration CLEAR_CONFIG = ConfigurationHelper.loadFromFile("../conf/aerospike-graph.properties");
        //first set to a 2 level model and add data to server
        db.setGraphMetadata("packed", "2.0.0");

        Key key = new Key(db.getNamespace(), "demo", "user1");
        Bin nameBin = new Bin("name", "alice");
        Bin ageBin = new Bin("age", 30);
        db.checkedPut(null, key, nameBin, ageBin);

        //then change model to 3 level and try to start (throws error)
        db.setGraphMetadata("packed", "3.0.0");
        try {
            final FireflyGraph graph = FireflyGraph.open(CONFIG);
            Assert.fail("Should have failed due to model mismatch");
        } catch (final DataModelVersionMismatchException e) {
            Assert.assertEquals(GraphError.DATA_MODEL_VERSION_MISMATCH.code, e.errorCode);
        }

        //then change config to clear enabled and try again
        FireflyGraph clearGraph = null;
        try {
            clearGraph = FireflyGraph.open(CLEAR_CONFIG);
            Assert.fail("Should have failed due to model mismatch");
        } catch (Exception e) {
            Assert.fail("Opening graph should not have thrown, but got: " + e);
        }
        Assert.assertNotNull("Graph instance should be non-null", clearGraph);
        clearGraph.close();
    }
}
