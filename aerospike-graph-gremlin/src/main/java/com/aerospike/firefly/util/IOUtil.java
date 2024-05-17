package com.aerospike.firefly.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class IOUtil {
    private IOUtil(){}
    public static void copyResourceToDirectory(String resourceName, Path filePath) {
        try (InputStream is = IOUtil.class.getClassLoader().getResourceAsStream(resourceName)) {
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
        IOUtil.copyResourceToDirectory(resourceName, tempPath);
        String resourcePath = tempPath.resolve(resourceName).toAbsolutePath().toString();
        g.io(resourcePath).read().iterate();
    }

    public static void loadGraphmlFromData(Graph graph, String resourceName) throws IOException {
        Path dataPath = Path.of("../data");
        String resourcePath = dataPath.resolve(resourceName).toAbsolutePath().toString();
        graph.io(graphml()).readGraph(resourcePath);
    }

    public static void downloadFileFromURL(URL source, File dest) {
        try (BufferedInputStream in = new BufferedInputStream(source.openStream());
             FileOutputStream fileOutputStream = new FileOutputStream(dest)) {
            byte dataBuffer[] = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toHex(final byte[] bytes){
        final StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }
}
