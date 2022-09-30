package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.io.FireflyLoader;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.DROP_DATABASE_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.DROP_INDEXES_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.getConfig;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.getOrDefault;

public class BulkLoader {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoader.class);
    private static final String DEFAULT_CONFIG_PATH = "conf/bulk-loader-conf/config.properties";

    public static void main(final String[] args) {
        final Path path;
        if (args.length < 1) {
            // TODO: Remove this before a built jar is distributed for use
            path = Path.of(DEFAULT_CONFIG_PATH);
        } else {
            path = Path.of(args[0]);
        }
        final Configuration config = getConfig(path);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            LOG.info("FireflyGraph instantiation successful,");
            if (Boolean.parseBoolean(getOrDefault(DROP_DATABASE_KEY, config))) {
                graph.getBaseGraph().dropDatabase(Boolean.parseBoolean(getOrDefault(DROP_INDEXES_KEY, config)));
            }
            final FireflyLoader loader = new FireflyLoader(graph, config);

            // TODO: Remove this before a built jar is distributed for use - currently just for dev purposes
            loader.load();
            loader.logMetrics();
            final GraphTraversalSource g = graph.traversal();
            final var elements =
                    g.V().has("name", "Pat Rohan").out("Follows").toList();
            LOG.info("Pat Rohan follows this amount of people: " + elements.size());
            for (final var element : elements) {
                LOG.info((String) element.property("name").value());
            }
        }
    }
}

