package com.aerospike.firefly.process.util;

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
}
