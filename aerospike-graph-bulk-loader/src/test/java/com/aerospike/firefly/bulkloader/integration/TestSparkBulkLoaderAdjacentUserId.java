package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.BeforeClass;

import java.nio.file.Path;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;

public class TestSparkBulkLoaderAdjacentUserId extends TestSparkBulkLoaderPacked {
    static private final String DEFAULT_CONFIG = "src/test/resources/conf/adjacentuserid/config.properties";
    static private final String KEEP_ID_AS_PROPERTY_CONFIG = "src/test/resources/conf/adjacentuserid/keep-provided-id-as-property.properties";
    static private final String DEFAULT_CONFIG_ARTIFICIAL_SUPERNODE = "src/test/resources/conf/adjacentuserid/config-artificial-supernode.properties";
    static private final String KEEP_ID_AS_PROPERTY_CONFIG_ARTIFICIAL_SUPERNODE = "src/test/resources/conf/adjacentuserid/keep-provided-id-as-property-artificial-supernode.properties";
    static private final String PREFLIGHT_CHECK_EDGE = "src/test/resources/conf/adjacentuserid/preflight-check-edge.properties";
    static private final String PREFLIGHT_CHECK_VERTEX = "src/test/resources/conf/adjacentuserid/preflight-check-vertex.properties";
    static private final String BAD_ENTRIES = "src/test/resources/conf/adjacentuserid/bad-entries.properties";
    static private final String NO_ID_EDGES = "src/test/resources/conf/adjacentuserid/no-id-edges.properties";
    static private final String NO_ID_EDGES_KEEP_AS_PROPERTY_OFF = "src/test/resources/conf/adjacentuserid/no-id-edges-keep-as-property-off.properties";
    static private final String DUPLICATE_VERTEX_ID = "src/test/resources/conf/adjacentuserid/duplicate-vertex-id.properties";
    static private final String DUPLICATE_EDGE_ID = "src/test/resources/conf/adjacentuserid/duplicate-edge-id.properties";
    static private final String S3_FILESYSTEM = "src/test/resources/conf/adjacentuserid/filesystem-s3.properties";
    static private final String GCS_FILESYSTEM = "src/test/resources/conf/adjacentuserid/filesystem-gcs.properties";
    static private final String FAILING_CLIENT = "src/test/resources/conf/adjacentuserid/failing-client.properties";
    static private final String DETACHED_EDGES = "src/test/resources/conf/adjacentuserid/detached-edges.properties";

    @BeforeClass
    public static void beforeAll() {
        final Configuration config = getConfig(Path.of(DEFAULT_CONFIG));
        config.setProperty(ConfigurationHelper.Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY, "false");
        final FireflyGraph graph = FireflyGraph.open(config);
        graph.getBaseGraph().dropDatabase(graph, true);
        graph.close();
    }

    @Override
    protected Configuration getTestConfig() {
        return getConfig(Path.of(DEFAULT_CONFIG));
    }

    @Override
    protected String getDefaultConfig() {
        return DEFAULT_CONFIG;
    }

    @Override
    protected String getKeepIdAsPropertyTrueConfig() {
        return KEEP_ID_AS_PROPERTY_CONFIG;
    }

    @Override
    protected String getDefaultConfigArtificialSupernode() {
        return DEFAULT_CONFIG_ARTIFICIAL_SUPERNODE;
    }

    @Override
    protected String getKeepIdAsPropertyTrueConfigArtificialSupernode() {
        return KEEP_ID_AS_PROPERTY_CONFIG_ARTIFICIAL_SUPERNODE;
    }

    @Override
    protected String getPreflightCheckEdge() {
        return PREFLIGHT_CHECK_EDGE;
    }

    @Override
    protected String getBadEntries() {
        return BAD_ENTRIES;
    }

    @Override
    protected String getPreflightCheckVertex() {
        return PREFLIGHT_CHECK_VERTEX;
    }

    @Override
    protected String getNoIdEdges() {
        return NO_ID_EDGES;
    }

    @Override
    protected String getNoIdEdgesKeepIdAsPropertyOff() {
        return NO_ID_EDGES_KEEP_AS_PROPERTY_OFF;
    }

    @Override
    protected String getDuplicateVertexId() {
        return DUPLICATE_VERTEX_ID;
    }

    @Override
    protected String getDuplicateEdgeId() {
        return DUPLICATE_EDGE_ID;
    }

    @Override
    protected String getS3FileSystem() {
        return S3_FILESYSTEM;
    }

    @Override
    protected String getGcsFileSystem() {
        return GCS_FILESYSTEM;
    }

    @Override
    protected String getFailingClient() {
        return FAILING_CLIENT;
    }

    @Override
    protected String getHasBadEdges() {
        return DETACHED_EDGES;
    }
}
