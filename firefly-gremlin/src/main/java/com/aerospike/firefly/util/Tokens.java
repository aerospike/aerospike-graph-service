package com.aerospike.firefly.util;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class Tokens {
    private Tokens() {
    }

    public static final String VERTEX_ID_COUNTER = "_vxidctr";
    public static final String EDGE_ID_COUNTER = "_eidctr";
    public static final String VERTEX_PROPERTY_ID_COUNTER = "_vxpidctr";
    public static final String UNIMPLEMENTED = "unimplemented";

    public static final String MEMORY_ERROR_MESSAGE = "Aerospike server side memory error detected. " +
            "Check index memory usage and/or increase server memory in Aerospike configuration.";

}
