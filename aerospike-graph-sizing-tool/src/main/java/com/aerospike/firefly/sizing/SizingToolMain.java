package com.aerospike.firefly.sizing;

import com.aerospike.firefly.schema.EdgeSchema;
import com.aerospike.firefly.schema.GraphSchema;
import com.aerospike.firefly.schema.PropertySchema;
import com.aerospike.firefly.schema.VertexSchema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class SizingToolMain implements Callable<Exception> {
    @CommandLine.Parameters(index = "0", description = "Input Schema File.")
    private File file;
    @CommandLine.Parameters(index = "1", description = "Output File Path (Absolute). Should end in '.json', '.yaml', or '.csv'.")
    private String outputFilePathAbsolute;

    /**
     * Main function.
     */
    @Override
    public Exception call() {
        if (!file.exists()) {
            throw new RuntimeException("File does not exist: " + file.toPath());
        }

        final File outputFile = new File(outputFilePathAbsolute);
        if (!outputFile.exists()) {
            try {
                outputFile.createNewFile();
            } catch (final IOException e) {
                throw new RuntimeException("Cannot create file: " + outputFilePathAbsolute + ".", e);
            }
        }
        if (!outputFile.canWrite()) {
            throw new RuntimeException("Cannot write to file: " + outputFilePathAbsolute + ".");
        } else if (outputFile.isDirectory()) {
            throw new RuntimeException("File path is directory: " + outputFilePathAbsolute + ".");
        }

        final GraphSchema graphSchema = parseYAML(file.toPath().toAbsolutePath());
        if (graphSchema.replicationFactor == null) {
            throw new RuntimeException("replicationFactor is required.");
        }
        final SizingTool sizingTool = new SizingTool(graphSchema);
        sizingTool.formatToFile(outputFilePathAbsolute);

        return null;
    }

    /**
     * Function to get GraphSchema from a YAML file.
     *
     * @param yamlSchemaPath Path to the YAML file.
     * @return GraphSchema object.
     */
    public static GraphSchema parseYAML(final Path yamlSchemaPath) {
        final File file = yamlSchemaPath.toFile();
        try {
            final String yamlText = Files.readAllLines(file.toPath(), Charset.defaultCharset()).stream()
                    .reduce("", (a, b) -> a + "\n" + b);
            final Yaml yaml = new Yaml(new Constructor(GraphSchema.class, new LoaderOptions()));
            return yaml.load(yamlText);
        } catch (IOException e) {
            throw new RuntimeException("Could not read file: " + file.toPath(), e);
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new SizingToolMain()).execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
        System.out.println("Success.");
    }

    // Add graphson/graph input support.

    public static GraphSchema fromGraph(final Graph graph) {
        final GraphTraversalSource g = graph.traversal();
        return fromGraphTraversalSource(g);
    }

    private static List<VertexSchema> getVertexSchema(final GraphTraversalSource g) {
        final List<Map<Object, Object>> vertexElementMap = g.V().elementMap().toList();
        final List<VertexSchema> vertexSchemas = new ArrayList<>();
        for (final Map<Object, Object> vertexMap : vertexElementMap) {
            if (vertexMap.get(T.id).equals("~metadata")) {
                continue;
            }

            final VertexSchema vertexSchema = new VertexSchema();
            final String label = getValue(vertexMap, T.label, String.class);
            if (!vertexMap.containsKey(label + ".count")) {
                throw new RuntimeException("Vertex '" + vertexMap.get(T.label) + "' does not have a '" + label + ".count' property.");
            } else if (!(vertexMap.get(label + ".count") instanceof Number)) {
                throw new RuntimeException("Vertex '" + vertexMap.get(T.label) + "' has a '" + label + ".count' property that is not a Number.");
            }
            vertexSchema.count = getValue(vertexMap, label + ".count", Number.class);

            final Set<Object> keys = new HashSet<>(vertexMap.keySet());
            keys.removeIf(key -> key.equals(T.id) || key.equals(T.label) ||
                    key.equals(label + ".count") ||
                    key.toString().endsWith(".valueSize") ||
                    key.toString().endsWith(".sindexed"));

            for (final Object key : keys) {
                final PropertySchema propertySchema = new PropertySchema();
                final String keyString = key.toString();
                propertySchema.key = keyString;
                propertySchema.type = vertexMap.get(keyString).toString();
                if (vertexMap.containsKey(keyString + ".valueSize")) {
                    propertySchema.size = getValue(vertexMap, keyString + ".valueSize", Number.class);
                }
                if (vertexMap.containsKey(keyString + ".sindexed")) {
                    propertySchema.sindexed = getValue(vertexMap, keyString + ".sindexed", Boolean.class);
                }
                if (vertexMap.containsKey(keyString + ".likelihood")) {
                    propertySchema.likelihood = getValue(vertexMap, keyString + ".likelihood", Double.class);
                }
                vertexSchema.properties.add(propertySchema);
            }
            vertexSchema.label = (String) vertexMap.get(T.label);
            vertexSchemas.add(vertexSchema);
        }
        return vertexSchemas;
    }

    private static <T> T getValue(final Map<Object, Object> map, final Object key, final Class<T> clazz) {
        final Object value = map.get(key);
        if (value == null) {
            throw new RuntimeException("Expected key '" + key + "' but only found '" + map.keySet() + "'.");
        }
        if (clazz.isAssignableFrom(value.getClass())) {
            return (T) value;
        }
        throw new RuntimeException("Expected type " + clazz + " for key " + key + " but got " + value.getClass() + ".");
    }

    private static List<EdgeSchema> getEdgeSchema(final GraphTraversalSource g) {
        final List<Map<Object, Object>> edgeElementMap = g.E().elementMap().toList();
        final List<EdgeSchema> edgeSchemas = new ArrayList<>();
        for (final Map<Object, Object> edgeMap : edgeElementMap) {
            // Skip vertex mapping.
            edgeMap.remove(Direction.IN);
            edgeMap.remove(Direction.OUT);

            final EdgeSchema edgeSchema = new EdgeSchema();
            final String label = getValue(edgeMap, T.label, String.class);
            if (!edgeMap.containsKey(label + ".count")) {
                throw new RuntimeException("Edge '" + edgeMap.get(T.label) + "' does not have a '" + label + ".count' property.");
            } else if (!(edgeMap.get(label + ".count") instanceof Number)) {
                throw new RuntimeException("Edge '" + edgeMap.get(T.label) + "' has a '" + label + ".count' property that is not a Number.");
            }
            edgeSchema.count = getValue(edgeMap, label + ".count", Number.class);

            final Set<Object> keys = new HashSet<>(edgeMap.keySet());
            keys.removeIf(key -> key.equals(T.id) || key.equals(T.label) ||
                    key.equals(label + ".count") ||
                    key.toString().endsWith(".valueSize") ||
                    key.toString().endsWith(".sindexed"));

            for (final Object key : keys) {
                final PropertySchema propertySchema = new PropertySchema();
                final String keyString = key.toString();
                propertySchema.key = keyString;
                propertySchema.type = edgeMap.get(keyString).toString();
                if (edgeMap.containsKey(keyString + ".valueSize")) {
                    propertySchema.size = getValue(edgeMap, keyString + ".valueSize", Number.class);
                }
                if (edgeMap.containsKey(keyString + ".sindexed")) {
                    propertySchema.sindexed = getValue(edgeMap, keyString + ".sindexed", Boolean.class);
                }
                if (edgeMap.containsKey(keyString + ".likelihood")) {
                    propertySchema.likelihood = getValue(edgeMap, keyString + ".likelihood", Double.class);
                }
                edgeSchema.properties.add(propertySchema);
            }
            edgeSchema.label = (String) edgeMap.get(T.label);
            edgeSchemas.add(edgeSchema);
        }
        return edgeSchemas;
    }

    private static void getMetadata(final GraphTraversalSource g, final GraphSchema graphSchema) {
        final Map<Object, Object> metadata = g.V("~metadata").elementMap().next();
        if (!metadata.containsKey("replicationFactor")) {
            throw new RuntimeException("Graph does not have a '~metadata' vertex with a 'replicationFactor' property.");
        }
        graphSchema.replicationFactor = ((Number) metadata.get("replicationFactor")).longValue();
        if (metadata.containsKey("maxEdgeCacheSize")) {
            graphSchema.maxEdgeCacheSize = ((Number) metadata.get("maxEdgeCacheSize")).longValue();
        }
        if (metadata.containsKey("maxEdgeCacheSize")) {
            graphSchema.maxEdgeCacheSize = ((Number) metadata.get("maxEdgeCacheSize")).longValue();
        }
        if (metadata.containsKey("edgePackSize")) {
            graphSchema.edgePackSize = ((Number) metadata.get("edgePackSize")).longValue();
        }
    }

    public static GraphSchema fromGraphTraversalSource(final GraphTraversalSource g) {
        // We do not know if the graph is remote or local, so we need to make sure the queries are valid for either.
        final GraphSchema graphSchema = new GraphSchema();
        graphSchema.vertexSchema = getVertexSchema(g);
        graphSchema.edgeSchema = getEdgeSchema(g);
        getMetadata(g, graphSchema);
        final SizingTool sizingTool = new SizingTool(graphSchema);
        sizingTool.formatToFile("output.json");
        return graphSchema;
    }

    public static GraphSchema fromGraphson(final Path pathToGraphson) {
        if (!pathToGraphson.toFile().exists()) {
            throw new RuntimeException("The provided graphson file '" + pathToGraphson.toAbsolutePath() + "' does not exist.");
        }
        try (final Graph graph = TinkerGraph.open()) {
            // Load graph with graphson.
            graph.traversal().io(pathToGraphson.toAbsolutePath().toString()).read().iterate();

            // Get graph schema.
            return fromGraph(graph);
        } catch (final Exception e) {
            throw new RuntimeException("Could not load graphson file '" + pathToGraphson.toAbsolutePath() + "'.", e);
        }
    }

    public static List<Exception> testMain(String[] args) {
        final CommandLine cmd = new CommandLine(new SizingToolMain());
        final AtomicReference<Exception> x = new AtomicReference<>();
        cmd.setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
            x.set(ex);
            return 0;
        });
        cmd.execute(args);
        return x.get() == null ? List.of() : List.of(x.get());
    }

    private static Map<String, Long> fromGraphToMap(final Graph graph) {
        final GraphSchema graphSchema = fromGraph(graph);
        final SizingTool sizingTool = new SizingTool(graphSchema);
        return sizingTool.asMap();
    }

    public Map<String, Long> size(final Graph graph) {
        return fromGraphToMap(graph);
    }
}
