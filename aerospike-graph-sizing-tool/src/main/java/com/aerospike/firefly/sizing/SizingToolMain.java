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
        } catch (final IOException e) {
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

    public static GraphSchema fromGraph(final Graph graph) {
        final GraphTraversalSource g = graph.traversal();
        return fromGraphTraversalSource(g);
    }

    private static String sanitizeType(final String type) {
        // Remove annotation (looks like String<JFaker.name.fullname>).
        if (type.toLowerCase().startsWith("list")) {
            if (!type.endsWith(">")) {
                throw new RuntimeException("Type '" + type + "' has an annotation opening '<' but no closing '>'.");
            }
            String subType = type.substring(type.indexOf("<") + 1, type.length() - 1);
            if (subType.contains("<")) {
                subType = subType.substring(0, subType.indexOf("<"));
            }
            return type.toLowerCase().substring(0, type.indexOf("<")) + "<" + subType.toLowerCase() + ">";
        } else if (type.contains("<")) {
            if (!type.endsWith(">")) {
                throw new RuntimeException("Type '" + type + "' has an annotation opening '<' but no closing '>'.");
            }
            return type.substring(0, type.indexOf("<")).toLowerCase();
        }
        return type.toLowerCase();
    }

    private static List<VertexSchema> getVertexSchema(final GraphTraversalSource g) {
        final List<Map<Object, Object>> vertexElementMap = g.V().elementMap().toList();
        final List<VertexSchema> vertexSchemas = new ArrayList<>();
        for (final Map<Object, Object> vertexMap : vertexElementMap) {
            if (vertexMap.get(T.id).equals("~metadata")) {
                continue;
            }

            final VertexSchema vertexSchema = new VertexSchema();
            final String label = getAndSanitizeLabel(vertexMap, "Vertex");
            vertexSchema.count = getValue(vertexMap, label + ".count", Number.class);
            final Set<Object> keys = getSanitizedKeySet(vertexMap, label);
            for (final Object key : keys) {
                final PropertySchema propertySchema = new PropertySchema();
                populatePropertySchema(propertySchema, vertexMap, key.toString());
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
            final String label = getAndSanitizeLabel(edgeMap, "Edge");
            edgeSchema.count = getValue(edgeMap, label + ".count", Number.class);

            final Set<Object> keys = getSanitizedKeySet(edgeMap, label);
            for (final Object key : keys) {
                final PropertySchema propertySchema = new PropertySchema();
                populatePropertySchema(propertySchema, edgeMap, key.toString());
                edgeSchema.properties.add(propertySchema);
            }
            edgeSchema.label = (String) edgeMap.get(T.label);
            edgeSchemas.add(edgeSchema);
        }
        return edgeSchemas;
    }

    private static void populatePropertySchema(final PropertySchema propertySchema, final Map<Object, Object> map, final String key) {
        propertySchema.key = key;
        propertySchema.type = sanitizeType(map.get(key).toString());
        assignCountSize(propertySchema, map, key);
        if (map.containsKey(key + ".sindexed")) {
            propertySchema.sindexed = getValue(map, key + ".sindexed", Boolean.class);
        }
        if (map.containsKey(key + ".likelihood")) {
            propertySchema.likelihood = getValue(map, key + ".likelihood", Double.class);
        }
        if (propertySchema.type.startsWith("list")) {
            if (propertySchema.count == null) {
                throw new RuntimeException("List property '" + key + "' does not have required '" + key + ".size' field.");
            }

            if (propertySchema.type.contains("string") &&
                    propertySchema.size == null) {
                throw new RuntimeException("List property '" + key + "' does not have size annotation." +
                        " List<String>> specification requires format List<String<X>> for size to be set.");
            }
        }
    }

    private static String getAndSanitizeLabel(final Map<Object, Object> map, final String type) {
        final String label = getValue(map, T.label, String.class);
        if (!map.containsKey(label + ".count")) {
            throw new RuntimeException(type + " '" + map.get(T.label) + "' does not have a '" + label + ".count' property.");
        } else if (!(map.get(label + ".count") instanceof Number)) {
            throw new RuntimeException(type + " '" + map.get(T.label) + "' has a '" + label + ".count' property that is not a Number.");
        }
        return label;
    }

    private static Set<Object> getSanitizedKeySet(final Map<Object, Object> map, final String label) {
        final Set<Object> keys = new HashSet<>(map.keySet());
        keys.removeIf(key -> key.equals(T.id) || key.equals(T.label) ||
                key.equals(label + ".count") ||
                key.toString().endsWith(".size") ||
                key.toString().endsWith(".size.min") ||
                key.toString().endsWith(".size.max") ||
                key.toString().endsWith(".sindexed") ||
                key.toString().endsWith(".likelihood"));
        return keys;
    }

    private static void assignCountSize(final PropertySchema propertySchema, final Map<Object, Object> map, final String key) {
        // If we have something like list<string>
        if (propertySchema.type.startsWith("list")) {
            final String rawMapValue = ((String) map.get(key)).toLowerCase();
            if (rawMapValue.contains("string")) {
                String annotation = rawMapValue.substring(rawMapValue.indexOf("string"));
                if (!annotation.contains("<")) {
                    throw new RuntimeException("List property '" + key + "' does not have size annotation." +
                            " List<String>> specification requires format List<String<X>> for size to be set.");
                }
                annotation = annotation.substring(annotation.indexOf("<") + 1, annotation.indexOf(">"));
                if (annotation.contains(",")) {
                    annotation = annotation.substring(0, annotation.indexOf(","));
                }
                if (annotation.contains("-")) {
                    final String[] split = annotation.split("-");
                    if (split.length != 2) {
                        throw new RuntimeException("Invalid property size specification '" + rawMapValue +
                                "', must have two numbers if a '-' is used.");
                    }
                    propertySchema.size = (Long.parseLong(split[1]) + Long.parseLong(split[0])) / 2;
                } else {
                    propertySchema.size = Long.parseLong(annotation);
                }
            }
        }

        // Can either have just size, or size.min and size.max.
        if (map.containsKey(key + ".size")) {
            if (map.containsKey(key + ".size.max") || map.containsKey(key + ".size.min")) {
                throw new RuntimeException("Invalid property size specification, either specify size.min and size.max or just size.");
            }
            if (propertySchema.type.startsWith("list")) {
                propertySchema.count = getValue(map, key + ".size", Number.class);
            } else {
                propertySchema.size = getValue(map, key + ".size", Number.class);
            }
        } else {
            if (map.containsKey(key + ".size.max") && map.containsKey(key + ".size.min")) {
                if (propertySchema.type.startsWith("list")) {
                    propertySchema.count = (getValue(map, key + ".size.max", Number.class).longValue() +
                            getValue(map, key + ".size.min", Number.class).longValue()) / 2;
                } else {
                    propertySchema.size = (getValue(map, key + ".size.max", Number.class).longValue() +
                            getValue(map, key + ".size.min", Number.class).longValue()) / 2;
                }
            } else if (map.containsKey(key + ".size.max") || map.containsKey(key + ".size.min")) {
                throw new RuntimeException("Invalid property size specification, either specify size.min and size.max or just size.");
            }
        }
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
