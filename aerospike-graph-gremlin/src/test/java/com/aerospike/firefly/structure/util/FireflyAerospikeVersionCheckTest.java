/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.FireflyAerospikeVersionCheck;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.fail;

public class FireflyAerospikeVersionCheckTest {
    // This test should only be called with an earlier version of Aerospike in its own workflow and is not included
    // in the standard test suite.
    // mvn test -pl aerospike-graph-gremlin -Dtest=FireflyAerospikeVersionCheckTest  -Dintegration.test.properties=packed --no-transfer-progress

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

        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 0, 6, ""))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 1, 0, 7, ""))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(5, 1, 0, 7, ""))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 0, 6, "-RC1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 1, 0, 7, "-RC1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(5, 1, 0, 7, "-RC1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 2, 0, 6, "_1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(6, 1, 0, 7, "_1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(5, 1, 0, 7, "_1"))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(5, 1, 0, 17, ""))));
        Assert.assertFalse(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(getVersionString(5, 1, 0, 17, "_1738"))));

        Assert.assertThrows(IllegalArgumentException.class, () -> FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck(null)));
    }

    @Test
    public void testEpochSemantics() {
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck("80.1.0-start-1-gabd7d79")));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck("90.2.9-end-gakl2d01")));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck("80.1.6")));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck("90.2.19")));
        Assert.assertTrue(FireflyAerospikeVersionCheck.validateVersion(new FireflyAerospikeVersionCheck("90.2.19-daiuwyghda-2")));
    }
}
