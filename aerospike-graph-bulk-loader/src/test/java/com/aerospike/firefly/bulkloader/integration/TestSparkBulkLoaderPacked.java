package com.aerospike.firefly.bulkloader.integration;

import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;

public class TestSparkBulkLoaderPacked extends TestSparkBulkLoaderBase {
    static private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config.properties";
    static private final String KEEP_ID_AS_PROPERTY_CONFIG = "src/test/resources/conf/packed/keep-provided-id-as-property.properties";
    static private final String DEFAULT_CONFIG_ARTIFICIAL_SUPERNODE = "src/test/resources/conf/packed/config-artificial-supernode.properties";
    static private final String KEEP_ID_AS_PROPERTY_CONFIG_ARTIFICIAL_SUPERNODE = "src/test/resources/conf/packed/keep-provided-id-as-property-artificial-supernode.properties";
    static private final String PREFLIGHT_CHECK_EDGE = "src/test/resources/conf/packed/preflight-check-edge.properties";
    static private final String PREFLIGHT_CHECK_VERTEX = "src/test/resources/conf/packed/preflight-check-vertex.properties";
    static private final String BAD_ENTRIES = "src/test/resources/conf/packed/bad-entries.properties";
    static private final String NO_ID_EDGES = "src/test/resources/conf/packed/no-id-edges.properties";
    static private final String NO_ID_EDGES_KEEP_AS_PROPERTY_OFF = "src/test/resources/conf/packed/no-id-edges-keep-as-property-off.properties";
    static private final String DUPLICATE_VERTEX_ID = "src/test/resources/conf/packed/duplicate-vertex-id.properties";
    static private final String DUPLICATE_EDGE_ID = "src/test/resources/conf/packed/duplicate-edge-id.properties";
    static private final String S3_FILESYSTEM = "src/test/resources/conf/packed/filesystem-s3.properties";
    static private final String GCS_FILESYSTEM = "src/test/resources/conf/packed/filesystem-gcs.properties";
    static private final String FAILING_CLIENT = "src/test/resources/conf/packed/failing-client.properties";
    static private final String DETACHED_EDGES = "src/test/resources/conf/packed/detached-edges.properties";
    static private final String SAMPLE_SUPERNODE = "src/test/resources/conf/packed/config-sampling-supernodes.properties";
    static private final String SAMPLE_SUPERNODE_TOO_HIGH = "src/test/resources/conf/packed/config-sampling-supernodes-too-high.properties";
    static private final String SAMPLE_SUPERNODE_TOO_LOW = "src/test/resources/conf/packed/config-sampling-supernodes-too-low.properties";
    static private final String DATA_MODEL = "packed";

    @Test
    public void testDataModelInitializedCorrectly() {
        Assert.assertEquals(DATA_MODEL, graph.getDataModel());
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

    @Override
    protected String getSamplingSupernode() {
        return SAMPLE_SUPERNODE;
    }

    @Override
    protected String getSamplingSupernodeTooHigh() {
        return SAMPLE_SUPERNODE_TOO_HIGH;
    }

    @Override
    protected String getSamplingSupernodeTooLow() {
        return SAMPLE_SUPERNODE_TOO_LOW;
    }
}
