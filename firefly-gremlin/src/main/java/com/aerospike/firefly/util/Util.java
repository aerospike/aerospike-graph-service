package com.aerospike.firefly.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

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
    public static void loadKryoDataFromResources(GraphTraversalSource g, String resourceName) {
        final Path tempPath;
        try {
            tempPath = Files.createTempDirectory("firefly-test").toAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tempPath.toFile().deleteOnExit();
        Util.copyResourceToDirectory(resourceName, tempPath);
        String resourcePath = tempPath.resolve(resourceName).toAbsolutePath().toString();
        g.io(resourcePath).read().iterate();
    }

    public static void loadGraphmlFromData(Graph graph, String resourceName) throws IOException {
        Path dataPath = Path.of("../data");
        String resourcePath = dataPath.resolve(resourceName).toAbsolutePath().toString();
        graph.io(graphml()).readGraph(resourcePath);
    }
}
