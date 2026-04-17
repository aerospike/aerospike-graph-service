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

package com.aerospike.firefly.ttl;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestAerospikeTTL {
    static boolean exited = false;

    class ExitManagerTest extends FireflyGraph.ExitManager {
        @Override
        public void exit(final int code) {
            exited = true;
        }
    }

    @Test
    public void test() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        FireflyGraph.EXIT_MANAGER = new ExitManagerTest();
        final FireflyGraph graph = FireflyGraph.open(config);
        Assert.assertNull(graph);
        FireflyGraph.EXIT_MANAGER = new FireflyGraph.ExitManager();
        Assert.assertTrue(exited);
    }
}
