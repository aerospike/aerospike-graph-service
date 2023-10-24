package com.aerospike.firefly.schema;


import java.util.List;

public class GraphSchema {
    public Long replicationFactor;
    public Long maxEdgeCacheSize;
    public Boolean vertexLabelSindex;
    public Long edgePackSize;
    public List<EdgeSchema> edgeSchema;
    public List<VertexSchema> vertexSchema;

    public GraphSchema() {
        // Default values.
        this.vertexLabelSindex = false;
        this.maxEdgeCacheSize = 8000L;
        this.edgePackSize = 10L;
    }
}
