package com.aerospike.firefly.bulkloader.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.Tokens;

public class VertexIdHandler extends IdHandler {
    public VertexIdHandler(final FireflyGraph graph, final long bufferSize) {
        super(graph, bufferSize, Tokens.VERTEX_ID_COUNTER);
    }
}
