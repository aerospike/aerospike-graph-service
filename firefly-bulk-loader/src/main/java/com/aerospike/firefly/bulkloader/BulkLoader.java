package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.io.FireflyLoader;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;

public class BulkLoader {
    private static final Logger LOG = LoggerFactory.getLogger(BulkLoader.class);

    public static void main(final String[] args) {
        // TODO: Input parameters for config
        final Configuration config = getConfig();
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            LOG.info("FireflyGraph instantiation successful,");
            // TODO: Make this optional
            try {
                graph.getBaseGraph().dropDatabase(false);
            } catch (final Exception e) {
                // Truncation failure - do nothing?
            }
            final File vertexDirectory =
                    new File("c:/Repos/firefly/firefly-bulk-loader/src/main/resources/sampledata/vertexes");
            final File edgeDirectory =
                    new File("c:/Repos/firefly/firefly-bulk-loader/src/main/resources/sampledata/edges");
            final FireflyLoader loader = new FireflyLoader(graph, edgeDirectory, vertexDirectory, 50);
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

    private static Configuration getConfig() {
        return new MapConfiguration(new HashMap<>() {{
            put(ConfigurationHelper.Keys.AEROSPIKE_HOST.toLowerCase(), "localhost");
            put(ConfigurationHelper.Keys.AEROSPIKE_PORT.toLowerCase(), 3000);
            put(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE.toLowerCase(), "test");
            put("firefly_data_model", "packed");
        }});
    }
}

