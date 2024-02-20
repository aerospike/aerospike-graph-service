package com.aerospike.firefly.bulkloader.util;

public class ExceptionMessages {
    public static final String DATABASE_NOT_EMPTY = "Cannot bulk load into a database that contains existing Vertices or Edges.";
    public static final String JOB_ALREADY_RUNNING = "Cannot start a bulk load while another bulk load is in progress";
}
