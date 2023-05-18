package com.aerospike.firefly.bulkloader.storage;

import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileLoader implements ObjectLoader, Serializable {
    private static FileLoader fileLoader;

    private FileLoader() {
    }

    /**
     * Obtain singleton instance of FileLoader to be used across all distributed spark map transformations
     *
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
    public Map<String, Object> loadConfiguration(final String configPath) {

        final Path path = Path.of(configPath);
        return ((MapConfiguration) BulkLoaderConfigHelper.getConfig(path)).getMap();
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
        return FileUtils.listFiles(file, new String[]{"csv"}, true)
                .stream().map(File::getAbsolutePath).collect(Collectors.toList());
    }


}
