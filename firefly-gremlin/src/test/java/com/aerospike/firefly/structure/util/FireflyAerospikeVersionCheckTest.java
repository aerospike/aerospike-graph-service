package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.fail;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyAerospikeVersionCheckTest {
    // This test should only be called with an earlier version of Aerospike in it's own workflow and is not included
    // in the standard test suite.
    // mvn test -pl firefly-gremlin -Dtest=FireflyAerospikeVersionCheckTest -DfailIfNoTests=false -Dintegration.test.properties=packed --no-transfer-progress

    @Test
    public void testVersionCheckFails() {
        final Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try {
            AerospikeConnection.connect(conf);
            fail("Connecting to Aerospike with old version should have thrown an exception");
        } catch (final RuntimeException e) {
            Assert.assertTrue(e.getMessage().startsWith("Aerospike version"));
            Assert.assertTrue(e.getMessage().contains("is not supported. Minimum version is"));
        }
    }

    private String getVersionString(final int major, final int minor, final int revision, final int extension, final String extra) {
        return String.format("%d.%d.%d.%d%s", major, minor, revision, extension, extra);
    }

    @Test
    public void testVersion() {
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 0, 7, ""))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 0, 8, ""))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 1, 0, ""))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 3, 0, 0, ""))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(7, 0, 0, 0, ""))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 0, 7, "-RC1"))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 0, 8, "-RC1"))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 1, 0, "-RC1"))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 3, 0, 0, "-RC1"))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(7, 0, 0, 0, "-RC1"))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 0, 7, "_1"))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 0, 8, "_1"))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 1, 0, "_1"))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 3, 0, 0, "_1"))));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(7, 0, 0, 0, "_1"))));

        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6,2,0,6,""))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6,1,0,7,""))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(5,1,0,7,""))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6,2,0,6,"-RC1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6,1,0,7,"-RC1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(5,1,0,7,"-RC1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6,2,0,6,"_1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6,1,0,7,"_1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(5,1,0,7,"_1"))));
        Assert.assertThrows(IllegalArgumentException.class, () -> FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(null)));
    }
}
