package com.aerospike.firefly.bulkloader.util;

public class ExceptionMessages {
    public static final String DATABASE_NOT_EMPTY = "Cannot bulk load into a database that contains existing Vertices or Edges.";
    public static final String JOB_ALREADY_RUNNING = "Cannot start a bulk load while another bulk load is in progress.";
    public static final String BAD_EDGE_COUNT_EXCEEDED = "Bad Edge count exceeded allowed threshold.";
    public static final String BAD_EDGE_COUNT_HOT_KEY = "The bulk loader has encountered the maximum allowed number of errors during the current operation. Please use \"g.call(\"aerospike.graphloader.admin.bulk-load.errors\")\" for specifics on the encountered issues, adjust your data or configuration as necessary, and attempt the operation again.";
    public static final String DUPLICATE_VERTEX_ID_COUNT_EXCEEDED = "Duplicate Vertex ID count exceeded allowed threshold.";
    public static final String BAD_ENTRY_COUNT_EXCEEDED = "Bad CSV row entry count exceeded allowed threshold.";
}
