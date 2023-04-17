package com.aerospike.firefly.bulkloader.storage;

import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import org.apache.commons.configuration2.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class FileLoader implements ObjectLoader, Serializable {
    private static FileLoader fileLoader;
    private FileLoader() {}

    /**
     * Obtain singleton instance of FileLoader to be used across all distributed spark map transformations
     * @return
     */
    public static synchronized FileLoader getInstance() {
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
     * Function to get the paths of csv files of vertices or edges from a master directory.
     *
     * @param directory The master directory.
     * @return List of paths of the csv files.
     */
    @Override
    public List<String> getCsvPaths(final String directory) throws IOException {
        final File file = new File(directory);
        try (final Stream<Path> path = Files.list(Paths.get(file.getPath()))) {
            if (path.findAny().isEmpty())
                return Collections.emptyList();
        }
        return getAllPaths(file);
    }

    private List<String> getAllPaths(final File file) {
        final List<String> paths = new ArrayList<>();
        final File[] directories = file.listFiles(File::isDirectory);
        final File[] csvs = file.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
        for (final File csv : csvs) {
            paths.add(csv.getAbsolutePath());
        }
        for (final File directory : directories) {
            paths.addAll(getAllPaths(directory));
        }
        return paths;
    }
}
