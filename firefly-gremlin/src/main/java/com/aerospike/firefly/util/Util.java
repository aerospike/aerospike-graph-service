package com.aerospike.firefly.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Util {
    public static void copyResourceToDirectory(String resourceName, Path filePath) {
        try (InputStream is = Util.class.getClassLoader().getResourceAsStream(resourceName)) {
            Files.copy(is, filePath.resolve(resourceName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
