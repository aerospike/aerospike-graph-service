package com.aerospike.firefly.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestLogging {
    private MemoryAppender memoryAppender;

    // https://www.baeldung.com/junit-asserting-logs
    public static class MemoryAppender extends ListAppender<ILoggingEvent> {
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
        memoryAppender = new MemoryAppender();
        memoryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logger.addAppender(memoryAppender);
        memoryAppender.start();
    }

    @Test
    public void testConfigureLogLevel() {
        org.slf4j.Logger LOG = LoggerFactory.getLogger(TestLogging.class);
        LoggerUtil.setLogLevel(Level.OFF);
        LOG.info("This should not be logged");
        assertEquals(0, memoryAppender.countEventsForLogger(LOG.getName()));
        LoggerUtil.setLogLevel(Level.INFO);
        LOG.debug("This should not be logged");
        assertEquals(0, memoryAppender.countEventsForLogger(LOG.getName()));
        LOG.info("This should be logged");
        assertEquals(1, memoryAppender.countEventsForLogger(LOG.getName()));
    }

    @Test
    public void testFireflyLogConfiguration() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        conf.setProperty(ConfigurationHelper.Keys.LOG_LEVEL.toLowerCase(), "OFF");
        FireflyGraph graph = FireflyGraph.open(conf);
        org.slf4j.Logger LOG = LoggerFactory.getLogger(TestLogging.class);
        LOG.info("This should not be logged");
        assertEquals(0, memoryAppender.countEventsForLogger(LOG.getName()));
        graph.close();
        conf.setProperty("log.level", "INFO");
        graph = FireflyGraph.open(conf);
        LoggerUtil.setLogLevel(Level.INFO);
        LOG.debug("This should not be logged");
        assertEquals(0, memoryAppender.countEventsForLogger(LOG.getName()));
        LOG.info("This should be logged");
        assertEquals(1, memoryAppender.countEventsForLogger(LOG.getName()));
        graph.close();
    }

    @Test
    public void testCanSetASClientLogLevel() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        conf.setProperty(ConfigurationHelper.Keys.LOG_LEVEL.toLowerCase(), "DEBUG");
        conf.setProperty(ConfigurationHelper.Keys.ASCLIENT_LOG_ENABLED.toLowerCase(), "true");
        FireflyGraph graph = FireflyGraph.open(conf);
        final AerospikeClient client = graph.getBaseGraph().getClient();
        final Key key = new Key("test", "test", "test");
        client.put(null, key, new Bin("test", "test"));
        final Key keyDoesNotExist = new Key("test", "negative", "negative");
        client.get(null, keyDoesNotExist);
        AtomicBoolean passed = new AtomicBoolean(false);
        final ArrayList<ILoggingEvent> eventSnapshot = new ArrayList<>();
        eventSnapshot.addAll(memoryAppender.getLoggedEvents());
        eventSnapshot.forEach(event -> {
            if (event.getLoggerName().equals(AerospikeClient.class.getName()) && event.getLevel().toString().equals("DEBUG"))
                passed.set(true);
        });
        assertTrue(passed.get());
    }
}
