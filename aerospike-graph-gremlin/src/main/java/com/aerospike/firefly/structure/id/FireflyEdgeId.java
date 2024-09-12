package com.aerospike.firefly.structure.id;

import java.nio.ByteBuffer;

public interface FireflyEdgeId extends FireflyId {

    ByteBuffer getEdgeIdBytes();

    Long getPackingId();

    Long getUniqueId();
}
