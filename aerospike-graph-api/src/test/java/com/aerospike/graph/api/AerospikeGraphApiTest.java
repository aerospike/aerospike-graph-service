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

package com.aerospike.graph.api;

import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;

public class AerospikeGraphApiTest {
    static private final String HOST = "172.17.0.1";
    static private final int PORT = 3000;
    static private final String NAMESPACE = "test";

    @Test
    public void testBuilderBarebonesConfig() throws Exception {
        AerospikeGraphApiBuilder builder = AerospikeGraphApi.builder();
        try (final AerospikeGraphApi graphApi =
                     builder.withConfiguration("./src/test/resources/basic/config.properties").build()) {
            var g = graphApi.traversal();
            g.addV("vertex").iterate();
            Assert.assertEquals(1, (long) g.V().count().next());
            g.V().drop().iterate();
        }

        builder = AerospikeGraphApi.builder();
        try (final AerospikeGraphApi graphApi =
                     builder.withConfiguration(Path.of("./src/test/resources/basic/config.properties")).build()) {
            var g = graphApi.traversal();
            g.addV("vertex").iterate();
            Assert.assertEquals(1, (long) g.V().count().next());
            g.V().drop().iterate();
        }

        builder = AerospikeGraphApi.builder();
        final Configuration config = ConfigurationHelper.loadFromFile("./src/test/resources/basic/config.properties");
        try (final AerospikeGraphApi graphApi = builder.withConfiguration(config).build()) {
            var g = graphApi.traversal();
            g.addV("vertex").iterate();
            Assert.assertEquals(1, (long) g.V().count().next());
            g.V().drop().iterate();
        }
    }

    @Test
    public void testGraphApiCastableToTinkerpopGraph() throws Exception {
        final AerospikeGraphApiBuilder builder = AerospikeGraphApi.builder();
        try (final AerospikeGraphApi graphApi =
                     builder.withConfiguration("./src/test/resources/basic/config.properties").build()) {
            Assert.assertTrue(graphApi instanceof Graph);
        }
    }

    @Test
    public void testUsingBuilderWithSteps() {
        AerospikeGraphApiBuilder builder = AerospikeGraphApi.builder().withHost(HOST).withPort(PORT);
        try (final AerospikeGraphApi graphApi = builder.build()) {
            Assert.fail("Should not be able to build without namespace");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        builder = AerospikeGraphApi.builder().withHost(HOST).withNamespace(NAMESPACE);
        try (final AerospikeGraphApi graphApi = builder.build()) {
            Assert.fail("Should not be able to build without namespace");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        builder = AerospikeGraphApi.builder().withPort(PORT).withNamespace(NAMESPACE);
        try (final AerospikeGraphApi graphApi = builder.build()) {
            Assert.fail("Should not be able to build without namespace");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        builder = AerospikeGraphApi.builder().withHost(HOST).withPort(PORT).withNamespace(NAMESPACE);
        try (final AerospikeGraphApi graphApi = builder.build()) {
        } catch (final Exception e) {
            Assert.fail("Failed despite providing all required builder configs: " + e);
        }
    }

    @Test
    public void testBuilderAdvancedConfig() throws Exception {
        AerospikeGraphApiBuilder builder = AerospikeGraphApi.builder()
                .withConfiguration("./src/test/resources/advanced/config.properties");
        try (final AerospikeGraphApi graphApi = builder.build()) {
            var g = graphApi.traversal();
            final FireflyVertex v = (FireflyVertex) g.addV("vertex").next();
            Assert.assertTrue(v.isEdgeCacheOverflowed());
            g.V().drop().iterate();
        }

        builder = AerospikeGraphApi.builder().withProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, "10")
                .withConfiguration("./src/test/resources/advanced/config.properties");
        try (final AerospikeGraphApi graphApi = builder.build()) {
            var g = graphApi.traversal();
            final FireflyVertex v = (FireflyVertex) g.addV("vertex").next();
            Assert.assertFalse(v.isEdgeCacheOverflowed());
            g.V().drop().iterate();
        }
    }

    @Test
    public void testBuilderIncompleteConfig() throws Exception {
        AerospikeGraphApiBuilder builder = AerospikeGraphApi.builder()
                .withConfiguration("./src/test/resources/incomplete/config.properties");
        try (final AerospikeGraphApi graphApi = builder.build()) {
            Assert.fail("Should not be able to build with an incomplete config");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }

        builder = builder.withHost(HOST).withProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, "0");
        try (final AerospikeGraphApi graphApi = builder.build()) {
            var g = graphApi.traversal();
            final FireflyVertex v = (FireflyVertex) g.addV("vertex").next();
            Assert.assertTrue(v.isEdgeCacheOverflowed());
            g.V().drop().iterate();
        }
    }
}
