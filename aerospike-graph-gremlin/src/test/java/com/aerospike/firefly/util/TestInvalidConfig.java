package com.aerospike.firefly.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.fail;

public class TestInvalidConfig {

    TestLogging.MemoryAppender memoryAppender;

    @Before
    public void setup() {
        Logger logger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        memoryAppender = new TestLogging.MemoryAppender();
        memoryAppender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logger.addAppender(memoryAppender);
        memoryAppender.start();
    }

    @Test
    public void testInvalidConfig() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        final String INVALID_KEY_1 = "aerospike.invalid";
        final String INVALID_KEY_2 = "invalid";
        config.setProperty(INVALID_KEY_1, INVALID_KEY_2);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            Assert.assertTrue(memoryAppender.contains(ConfigurationHelper.UNKNOWN_KEY_MESSAGE, Level.INFO));
            Assert.assertTrue(memoryAppender.contains(INVALID_KEY_1, Level.INFO));
            Assert.assertTrue(memoryAppender.contains(INVALID_KEY_2, Level.INFO));
        }
    }
}
