package com.aerospike.firefly;

import com.aerospike.firefly.schema.GraphSchema;
import com.aerospike.firefly.sizing.SizingTool;
import org.apache.tinkerpop.gremlin.structure.Graph;
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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

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

    public static void fromGraph(final Graph graph) {

    }

    public static void fromGraphson(final Path pathToGraphson) {
        // TODO: This should open tinkergraph.

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