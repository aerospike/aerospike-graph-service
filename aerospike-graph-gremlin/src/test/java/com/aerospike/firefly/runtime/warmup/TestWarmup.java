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

package com.aerospike.firefly.runtime.warmup;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.OutputCapturer;
import com.aerospike.firefly.util.TestLogging;
import com.aerospike.firefly.util.WarmupUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestWarmup extends AbstractFireflySuite {
    @Override
    protected boolean clearData() {
        return false;
    }

    private TestLogging.MemoryAppender memoryAppender;

    @Before
    public void setup() {
        Logger logger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        memoryAppender = new TestLogging.MemoryAppender();
        memoryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logger.addAppender(memoryAppender);
        memoryAppender.start();
        FireflyGraph.NEED_PREHEAT = true;
    }

    @Test
    public void testWarmupDoesNotCreateIndex() {
        Configuration wc = ConfigurationUtils.cloneConfiguration(config);
        wc.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), "true");
        graph.getBaseGraph().dropDatabase(graph, true);
        List<Map.Entry<String, String>> indexesA = AerospikeConnection.InfoOps.listExistingIndexes(db);
        assertEquals(0, indexesA.stream().filter(it ->
                it.getKey().contains(WarmupUtil.getWarmupArenaName()) || it.getValue().contains(WarmupUtil.getWarmupArenaName())).count());

        graph = FireflyGraph.open(wc);
        WarmupUtil w = WarmupUtil.create(wc);
        w.preheat(2);
        List<Map.Entry<String, String>> indexesB = AerospikeConnection.InfoOps.listExistingIndexes(db);
        assertEquals(0, indexesB.stream().filter(it ->
                it.getKey().contains(WarmupUtil.getWarmupArenaName()) || it.getValue().contains(WarmupUtil.getWarmupArenaName())).count());
    }

    @Test
    public void testWarmupQuery() {
        String edgeLabel = "l";
        Vertex va = graph.addVertex();
        Vertex vb = graph.addVertex();
        Edge ea = graph.traversal().V(va).addE(edgeLabel).to(vb).next();
        assertEquals((Long) 1L, graph.traversal().V(va.id()).count().next());
        assertEquals((Long) 1L, graph.traversal().V(vb.id()).count().next());
        assertEquals((Long) 1L, graph.traversal().E(ea.id()).count().next());
        assertEquals(edgeLabel, graph.traversal().E(ea.id()).label().next());
    }

    @Test
    public void testWarmupSuppressesLogOutput() {
        WarmupUtil w = WarmupUtil.create(config);
        w.preheat(2);

        assertEquals(0, memoryAppender.countEventsForLogger(LoggerFactory.getLogger(FireflyGraph.class).getName()));
    }

    @Test
    public void testWarmupWithAuth() throws IOException, InterruptedException {
        FireflyServer server = null;

        try (final OutputCapturer outputCapturer = new OutputCapturer()) {
            server = FireflyServer.start(new String[]{"../conf/credentials-config/authenticator-with-warmup.yaml"});

            Thread.sleep(10);
            final String[] logList = outputCapturer.getLines();

            boolean warmupComplete = false;
            for (final String line : logList) {
                if (line.startsWith("Error during warmup:")) {
                    fail("Should not fail during warmup.");
                } else if (line.contains("Warmup is complete.")) {
                    warmupComplete = true;
                    break;
                }
            }

            assertTrue(warmupComplete);
        } finally {
            if (server != null) {
                server.stop().join();
            }
        }
    }

    @Test
    public void testWarmupGraphId() throws IOException, InterruptedException {
        FireflyServer server = null;

        try (final OutputCapturer outputCapturer = new OutputCapturer()) {
            config.addProperty(ConfigurationHelper.Keys.LOG_WARMUP_SETS, "true");
            config.setProperty(ConfigurationHelper.Keys.AUTO_PRE_HEAT, "true");
            WarmupUtil.create(config).preheat(WarmupUtil.passes);
            Thread.sleep(10);
            final String[] logList = outputCapturer.getLines();

            boolean foundVertex = false;
            boolean foundEdge = false;
            for (final String line : logList) {
                if (line.startsWith("Warmup Vertex Set: FIREFLYWARMUP_2")) {
                    foundVertex = true;
                }
                if (line.startsWith("Warmup Edge Set: FIREFLYWARMUP_1")) {
                    foundEdge = true;
                }

                if(foundEdge && foundVertex) break;
            }
            Assert.assertTrue(foundVertex);
            Assert.assertTrue(foundEdge);

        } finally {
            if (server != null) {
                server.stop().join();
            }
        }
    }
}
