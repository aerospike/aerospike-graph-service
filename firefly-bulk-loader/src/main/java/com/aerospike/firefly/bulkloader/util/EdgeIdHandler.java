package com.aerospike.firefly.bulkloader.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.Tokens;

public class EdgeIdHandler extends IdHandler {
    public EdgeIdHandler(final FireflyGraph graph, final long bufferSize) {
        super(graph, bufferSize, Tokens.EDGE_ID_COUNTER);
    }
}
