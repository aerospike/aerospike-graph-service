package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.io.EdgeReader;
import com.aerospike.firefly.bulkloader.io.FireflyLoader;
import com.aerospike.firefly.bulkloader.io.VertexReader;
import com.aerospike.firefly.bulkloader.structure.BulkLoaderEdge;
import com.aerospike.firefly.bulkloader.structure.BulkLoaderVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;

public class BulkLoader {
    private static Logger LOG;

    public static void main(final String[] args) {
        LOG = LoggerFactory.getLogger(BulkLoader.class);

        // TODO: Input parameters for config
        final Configuration config = getConfig();
        try (final FireflyGraph graph = FireflyGraph.open(config);
             final FireflyLoader loader = new FireflyLoader(graph)) {
            LOG.info("FireflyGraph instantiation successful,");
            // TODO: Make this optional
            graph.getBaseGraph().dropDatabase();

            final File vertexDirectory =
                    new File("c:/Repos/firefly/firefly-bulk-loader/src/main/resources/sampledata/vertexes");
            final VertexReader vertexReader = new VertexReader(vertexDirectory, true);
            final List<BulkLoaderVertex> vertexes = vertexReader.read();
            final List<FireflyVertex> loadedVertexes = loader.loadVertexes(vertexes);
            final File edgeDirectory =
                    new File("c:/Repos/firefly/firefly-bulk-loader/src/main/resources/sampledata/edges");
            final EdgeReader edgeReader =
                    new EdgeReader(edgeDirectory, true, loadedVertexes, vertexReader.getIdMap());
            final List<BulkLoaderEdge> edges = edgeReader.read();
            loader.loadEdges(edges);
            final GraphTraversalSource g = graph.traversal();
            final List<Vertex> gremlinVertexes =
                    g.V().has("name", "Pat Rohan").out("Follows").toList();
            for (final Vertex v : gremlinVertexes) {
                LOG.info((String) v.property("name").value());
            }
        }
    }

    private static Configuration getConfig() {
        return new MapConfiguration(new HashMap<>() {{
            put(ConfigurationHelper.Keys.AEROSPIKE_HOST.toLowerCase(), "localhost");
            put(ConfigurationHelper.Keys.AEROSPIKE_PORT.toLowerCase(), 3000);
            put(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE.toLowerCase(), "test");
            put("firefly_data_model", "linked");
        }});
    }
}

