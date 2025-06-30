package com.aerospike.firefly.process.call.bulkload.utils;

public class BulkLoadStatusTokens {
    public static String LOAD_STEP = "step";
    public static String PARTITIONS_COMPLETED_PERCENTAGE = "complete-partitions-percentage";
    public static String ELEMENTS_WRITTEN = "elements-written";
    public static String PROGRESS_COMPLETE = "complete";

    public static String BULK_LOAD_STATUS_KEY = "status";
    public static String BULK_LOAD_STATUS_IN_PROGRESS = "in progress";
    public static String BULK_LOAD_STATUS_SUCCESS = "success";
    public static String BULK_LOAD_STATUS_ERROR = "error";
    
    public static String BULK_LOAD_EXCEPTION_MESSAGE = "message";
    public static String BULK_LOAD_EXCEPTION_STACKTRACE = "stacktrace";

    private BulkLoadStatusTokens() {

    }
}
