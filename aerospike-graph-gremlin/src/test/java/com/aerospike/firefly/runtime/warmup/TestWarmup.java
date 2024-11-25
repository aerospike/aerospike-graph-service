package com.aerospike.firefly.runtime.warmup;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.TestLogging;
import com.aerospike.firefly.util.WarmupUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestWarmup extends AbstractFireflySuite {
    @Override
    protected boolean clearData() {
        return false;
    }

    private TestLogging.MemoryAppender memoryAppender;

    // https://www.baeldung.com/junit-asserting-logs
    private class MemoryAppender extends ListAppender<ILoggingEvent> {
        public void reset() {
            this.list.clear();
        }

        public boolean contains(String string, Level level) {
            return this.list.stream()
                    .anyMatch(event -> event.toString().contains(string)
                            && event.getLevel().equals(level));
        }

        public int countEventsForLogger(String loggerName) {
            return (int) this.list.stream()
                    .filter(event -> event.getLoggerName().contains(loggerName))
                    .count();
        }

        public List<ILoggingEvent> search(String string) {
            return this.list.stream()
                    .filter(event -> event.toString().contains(string))
                    .collect(Collectors.toList());
        }

        public List<ILoggingEvent> search(String string, Level level) {
            return this.list.stream()
                    .filter(event -> event.toString().contains(string)
                            && event.getLevel().equals(level))
                    .collect(Collectors.toList());
        }

        public int getSize() {
            return this.list.size();
        }

        public List<ILoggingEvent> getLoggedEvents() {
            return Collections.unmodifiableList(this.list);
        }
    }


    @Before
    public void setup() {
        Logger logger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        memoryAppender = new TestLogging.MemoryAppender();
        memoryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logger.addAppender(memoryAppender);
        memoryAppender.start();
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
    public void testWarmupWithAuth() {
        PrintStream originalOut = System.out;
        FireflyServer server = null;

        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baos));

            server = FireflyServer.start(new String[]{"../conf/credentials-config/authenticator-with-warmup.yaml"});

            final List<String> logList = Arrays.asList(baos.toString().split("\n"));

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
            System.setOut(originalOut);
        }
    }
}
