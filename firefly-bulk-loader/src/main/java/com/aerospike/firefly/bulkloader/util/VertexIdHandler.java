package com.aerospike.firefly.bulkloader.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.Tokens;

public class VertexIdHandler extends IdHandler {
    public VertexIdHandler(final FireflyGraph graph, final long bufferSize, final boolean useProvidedId) {
        super(graph, Tokens.VERTEX_ID_COUNTER, bufferSize, useProvidedId);
    }
}
