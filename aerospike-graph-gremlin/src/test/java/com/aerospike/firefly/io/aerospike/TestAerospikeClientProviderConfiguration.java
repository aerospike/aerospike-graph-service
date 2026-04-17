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

package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.policy.AuthMode;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.AUTH_MODE;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.CLIENT_SERVICES_ALTERNATE;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.CLUSTER_NAME;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.PHAT_EDGE_SIZE;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.VALIDATE_CLUSTER_NAME;

public class TestAerospikeClientProviderConfiguration {

    @Test
    public void testSettingAuthMode() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(AUTH_MODE, "external_insecure");
        try (final FireflyGraph firefly = FireflyGraph.open(config)) {
            final AerospikeConnection db = firefly.getBaseGraph();
            Assert.assertEquals(AuthMode.EXTERNAL_INSECURE, db.getAuthMode());
        }
    }

    @Test
    public void testSettingUseServiceAlternate() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(CLIENT_SERVICES_ALTERNATE, "true");
        try (final FireflyGraph firefly = FireflyGraph.open(config)) {
            final AerospikeConnection db = firefly.getBaseGraph();
            // The flag is private so best we can do is check that the client is alive.
            Assert.assertTrue(db.getClusterIsActive());
        }
    }

    @Test
    public void testSettingClusterName() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(CLUSTER_NAME, "test");
        config.setProperty(VALIDATE_CLUSTER_NAME, "false");
        try (final FireflyGraph firefly = FireflyGraph.open(config)) {
            final AerospikeConnection db = firefly.getBaseGraph();
            Assert.assertEquals("test", db.getClusterName());
        }
    }

    @Test
    public void testNumericSettingInvalid() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(PHAT_EDGE_SIZE, "notNumeric");
        try {
            ConfigurationHelper.getOrDefaultInt(PHAT_EDGE_SIZE, config);
            Assert.fail("Non numeric config value should have failed.");
        } catch (final Exception e) {
            Assert.assertEquals("Value provided, \"notNumeric\", for configuration key, \"aerospike.graph.phat.edge.size\", is invalid due to not being numeric.", e.getMessage());
        }
    }

    @Test
    public void testNumericSettingOverMax() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(PHAT_EDGE_SIZE, "101");
        try {
            ConfigurationHelper.getOrDefaultInt(PHAT_EDGE_SIZE, config);
            Assert.fail("> max numeric config value should have failed.");
        } catch (final Exception e) {
            Assert.assertEquals("Value provided, \"101\", for configuration key, \"aerospike.graph.phat.edge.size\", is above the maximum acceptable value, \"100\".", e.getMessage());
        }
    }

    @Test
    public void testNumericSettingUnderMin() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(PHAT_EDGE_SIZE, "0");
        try {
            ConfigurationHelper.getOrDefaultInt(PHAT_EDGE_SIZE, config);
            Assert.fail("< min numeric config value should have failed.");
        } catch (final Exception e) {
            Assert.assertEquals("Value provided, \"0\", for configuration key, \"aerospike.graph.phat.edge.size\", is below the minimum acceptable value, \"1\".", e.getMessage());
        }
    }
}
