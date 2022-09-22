package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.io.FireflyLoader;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.getConfig;

public class BulkLoader {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoader.class);
    private static final String DEFAULT_CONFIG_PATH = "conf/bulk-loader-conf/config.properties";

    public static void main(final String[] args) {
        final Path path;
        if (args.length < 1) {
            path = Path.of(DEFAULT_CONFIG_PATH);
        } else {
            path = Path.of(args[0]);
        }
        final Configuration config = getConfig(path);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            LOG.info("FireflyGraph instantiation successful,");
            // TODO: Make this optional
            try {
                graph.getBaseGraph().dropDatabase(false);
            } catch (final Exception e) {
                LOG.error(e.toString());
                // Truncation failure - do nothing?
            }
            final FireflyLoader loader = new FireflyLoader(graph, config);
            loader.load();
            loader.logMetrics();
            final GraphTraversalSource g = graph.traversal();
            final var elements =
                    g.V().has("name", "Pat Rohan").out("Follows").toList();
            for (final var element : elements) {
                LOG.info((String) element.property("name").value());
            }
        }
    }
}

