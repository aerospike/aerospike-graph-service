package com.aerospike.firefly.util;

import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Executable;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestUtil {

    @Test
    public void canCopyFileFromResources() {

        try {
            final String resourceName = "integration-test-settings.properties";
            final Path tempPath = Files.createTempDirectory("firefly-test").toAbsolutePath();
            tempPath.toFile().deleteOnExit();
            IOUtil.copyResourceToDirectory(resourceName, tempPath);
            assertTrue(Files.list(tempPath).anyMatch(it -> it.getFileName().toString().equals(resourceName)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static <T extends Throwable> void assertThrows(Class<T> exceptionType, LambdaFunc func) {
        try {
            func.func();
            fail("Expected exception to be thrown.");
        } catch (Throwable actualException) {
            if (!exceptionType.isInstance(actualException)) {
                fail("Exception type thrown (" + actualException.getClass().getName() + ") does not match exception type expected (" + exceptionType.getName() + "). Exception message: '" + actualException.getMessage() + "'.");
            }
        }
    }

    public interface LambdaFunc {
        void func();
    }
}
