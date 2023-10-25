package com.aerospike.firefly;

import com.aerospike.firefly.schema.GraphSchema;
import com.aerospike.firefly.schema.VertexSchema;
import com.aerospike.firefly.sizing.SizingTool;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class Main implements Callable<Exception> {
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
        int exitCode = new CommandLine(new Main()).execute(args);
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

    public static GraphSchema fromGraphTraversalSource(final GraphTraversalSource g) {
        // We do not know if the graph is remote or local, so we need to make sure the queries are valid for either.
        final List<Map<Object, Object>> vertexElementMap = g.V().elementMap().toList();
        for (final Map<Object, Object> vertexMap : vertexElementMap) {
            final VertexSchema vertexSchema = new VertexSchema();
            final Set<Object> keys = vertexMap.keySet();
            keys.removeIf(key -> key.equals(T.id.toString()) ||
                    key.toString().endsWith(".valueSize") ||
                    key.toString().endsWith(".sindexed"));
            vertexSchema.label = (String) vertexMap.get(T.label);

        }
        final GraphSchema graphSchema = null;
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
        final CommandLine cmd = new CommandLine(new Main());
        final AtomicReference<Exception> x = new AtomicReference<>();
        cmd.setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
            x.set(ex);
            return 0;
        });
        cmd.execute(args);
        return x.get() == null ? List.of() : List.of(x.get());
    }
}