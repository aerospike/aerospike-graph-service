package com.aerospike.firefly.schema;

import java.util.List;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GraphSchema {
    public Number replicationFactor;
    public Number maxEdgeCacheSize;
    public Boolean vertexLabelSindex;
    public Number edgePackSize;
    public List<EdgeSchema> edgeSchema;
    public List<VertexSchema> vertexSchema;

    public GraphSchema() {
        // Default values.
        this.vertexLabelSindex = false;
        this.maxEdgeCacheSize = 8000L;
        this.edgePackSize = 10L;
    }
}
