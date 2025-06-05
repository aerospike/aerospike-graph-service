package com.aerospike.firefly.util;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class Tokens {
    private Tokens() {
    }

    public static final String VERTEX_ID_COUNTER = "_vxidctr";
    public static final String EDGE_UNIQUE_ID_COUNTER = "_euidctr";
    public static final String EDGE_PACKING_ID_COUNTER = "_epidctr";
    public static final String VERTEX_PROPERTY_ID_COUNTER = "_vxpidctr";
    public static final String UNIMPLEMENTED = "unimplemented";

    public static final String VERTEX_LABEL_SCHEMA = "_vxlsch";
    public static final String VERTEX_PROPERTY_SCHEMA = "_vxpsch";
    public static final String VERTEX_PROPERTY_PROPERTY_SCHEMA = "_vppsch";
    public static final String EDGE_LABEL_SCHEMA = "_elsch";
    public static final String EDGE_PROPERTY_SCHEMA = "_epsch";
}
