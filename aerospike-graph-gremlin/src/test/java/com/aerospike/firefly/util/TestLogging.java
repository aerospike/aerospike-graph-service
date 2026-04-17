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

package com.aerospike.firefly.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLogging {
    private MemoryAppender memoryAppender;

    // https://www.baeldung.com/junit-asserting-logs
    public static class MemoryAppender extends AppenderBase<ILoggingEvent> {
        public Queue<ILoggingEvent> queue = new ConcurrentLinkedQueue<>();

        protected void append(ILoggingEvent e) {
            this.queue.add(e);
        }

        public void reset() {
            this.queue.clear();
        }

        public boolean contains(String string, Level level) {
            return this.queue.stream()
                    .anyMatch(event -> event.toString().contains(string)
                            && event.getLevel().equals(level));
        }

        public int countEventsForLogger(String loggerName) {
            return (int) this.queue.stream()
                    .filter(event -> event.getLoggerName().contains(loggerName))
                    .count();
        }

        public List<ILoggingEvent> search(String string) {
            return this.queue.stream()
                    .filter(event -> event.toString().contains(string))
                    .collect(Collectors.toList());
        }

        public List<ILoggingEvent> search(String string, Level level) {
            return this.queue.stream()
                    .filter(event -> event.toString().contains(string)
                            && event.getLevel().equals(level))
                    .collect(Collectors.toList());
        }

        public int getSize() {
            return this.queue.size();
        }

        public List<ILoggingEvent> getLoggedEvents() {
            return new ArrayList<>(this.queue);
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

        org.slf4j.Logger LOG = LoggerFactory.getLogger(TestLogging.class);

        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            LOG.info("This should not be logged");
            assertEquals(0, memoryAppender.countEventsForLogger(LOG.getName()));
        }

        conf.setProperty(ConfigurationHelper.Keys.LOG_LEVEL, "INFO");
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            LoggerUtil.setLogLevel(Level.INFO);
            LOG.debug("This should not be logged");
            assertEquals(0, memoryAppender.countEventsForLogger(LOG.getName()));
            LOG.info("This should be logged");
            assertEquals(1, memoryAppender.countEventsForLogger(LOG.getName()));
        }
    }

    @Test
    @Ignore //@todo
    public void testCanSetASClientLogLevel() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        conf.setProperty(ConfigurationHelper.Keys.LOG_LEVEL.toLowerCase(), "DEBUG");
        conf.setProperty(ConfigurationHelper.Keys.ASCLIENT_LOG_ENABLED.toLowerCase(), "true");
        final FireflyGraph graph = FireflyGraph.open(conf);
        final AerospikeConnection db = graph.getBaseGraph();
        final Key key = new Key("test", "test", "test");
        db.checkedPut(null, key, new Bin("test", "test"));
        final Key keyDoesNotExist = new Key("test", "negative", "negative");
        db.checkedPut(null, keyDoesNotExist);
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
