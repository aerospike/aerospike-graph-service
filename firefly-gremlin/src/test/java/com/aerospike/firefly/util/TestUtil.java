package com.aerospike.firefly.util;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestUtil {

    @Test
    void canCopyFileFromResources() {

        try {
            final String resourceName = "grateful-dead.kryo";
            final Path tempPath = Files.createTempDirectory("firefly-test").toAbsolutePath();
            tempPath.toFile().deleteOnExit();
            Util.copyResourceToDirectory("grateful-dead.kryo", tempPath);
            assertTrue(Files.list(tempPath).anyMatch( it -> it.getFileName().toString().equals(resourceName)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
