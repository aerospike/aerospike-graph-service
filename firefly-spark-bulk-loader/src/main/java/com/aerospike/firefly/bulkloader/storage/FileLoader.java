package com.aerospike.firefly.bulkloader.storage;

import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import org.apache.commons.configuration2.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileLoader implements ObjectLoader, Serializable {
    private static FileLoader fileLoader;
    private FileLoader() {}

    /**
     * Obtain singleton instance of FileLoader to be used across all distributed spark map transformations
     * @return
     */
    public static synchronized FileLoader getInstance(){
        if (fileLoader == null) {
            fileLoader = new FileLoader();
        }
        return fileLoader;
    }

    /**
     * Function to load config file from local/unix FileSystem.
     *
     * @param configPath Path to config on local filesystem.
     * @return Configuration object built from config file.
     */
    @Override
    public Configuration loadConfiguration(final String configPath) {
        final Path path = Path.of(configPath);
        return BulkLoaderConfigHelper.getConfig(path);
    }

    /**
     * Function to get a set of valid subdirectory strings of vertices or edges from a master directory.
     *
     * @param directory The master directory.
     * @return The set of valid subdirectories.
     */
    @Override
    public Set<String> getObjectList(final String directory) throws RuntimeException, IOException {
        final File file = new File(directory);
        checkIfDirectoryEmpty(file);
        final File[] directories = file.listFiles(File::isDirectory);

        if ( directories == null ) {
            throw new RuntimeException("Failed to start bulk loader due to empty data subdirectories in " + directory);
        }
        if (directories.length != 0) {
            for (File dir : directories)
                checkIfDirectoryEmpty(dir);
            return Arrays.stream(directories).map(File::getAbsolutePath).collect(Collectors.toSet());
        }
        return Collections.singleton(directory);
    }

    static private void checkIfDirectoryEmpty(File directory) throws RuntimeException, IOException {
        try(Stream<Path> path = Files.list(Paths.get(directory.getPath()))) {
            if (path.findAny().isEmpty())
                throw new IOException("Empty directory found for path: " + directory);
        }
    }
}
