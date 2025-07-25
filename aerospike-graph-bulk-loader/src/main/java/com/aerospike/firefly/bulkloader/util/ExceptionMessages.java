package com.aerospike.firefly.bulkloader.util;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.CLEAR_EXISTING_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RESUME;

public class ExceptionMessages {
    public static final String DATABASE_NOT_EMPTY = "Cannot bulk load into a database that contains existing Vertices or Edges. Consider using `incremental` mode.";
    public static final String JOB_ALREADY_RUNNING = "Cannot start a bulk load while another bulk load is in progress.";
    public static final String BAD_EDGE_COUNT_EXCEEDED = "Bad Edge count exceeded allowed threshold.";
    public static final String BAD_EDGE_COUNT_HOT_KEY = "The bulk loader has encountered the maximum allowed number of errors during the current operation. Please use \"g.call(\"aerospike.graphloader.admin.bulk-load.errors\")\" for specifics on the encountered issues, adjust your data or configuration as necessary, and attempt the operation again.";
    public static final String DUPLICATE_VERTEX_ID_COUNT_EXCEEDED = "Duplicate Vertex ID count exceeded allowed threshold.";
    public static final String BAD_ENTRY_COUNT_EXCEEDED = "Bad CSV row entry count exceeded allowed threshold.";
    public static final String INCREMENTAL_AND_CLEAR_EXISTING_DATA = "Cannot have both '" + INCREMENTAL_LOAD + "' and '" + CLEAR_EXISTING_DATA + "' flags set.";
    public static final String RESUME_AND_CLEAR_EXISTING_DATA = "Cannot have both '" + RESUME + "' and '" + CLEAR_EXISTING_DATA + "' flags set.";
    public static final String INCREMENTAL_LOAD_EMPTY_DATABASE = "Cannot perform incremental load on an empty database.";
    public static final String CLEAR_EXISTING_DATA_EMPTY_DATABASE = "Cannot clear existing data when database is empty and no recovery information is present.";
    public static final String RESUME_WITHOUT_RECOVERY_INFO = "Cannot use '" + RESUME + "' flag without recovery information.";
    public static final String INCREMENTAL_AND_RECOVERY_INFO_NO_RESUME_FLAG = "Bulk load resume information is present. Cannot resume load without '" + RESUME + "' flag.";
    public static final String RECOVERY_INFO_NO_CLEAR_EXISTING_DATA_FLAG_OR_RESUME = "Bulk load resume information is present. " +
            "Cannot resume load without '" + RESUME + "' flag. Alternatively, to drop the database and " +
            "run a fresh load, set the '" + CLEAR_EXISTING_DATA + "' flag.";
    public static final String CANNOT_RECOVER_INCREMENTAL_LOAD_WITHOUT_INCREMENTAL_FLAG =
            "To resume an incremental load, both the '" + INCREMENTAL_LOAD + "' and '" + RESUME + "' flags must be set.";
    public static final String CANNOT_RECOVER_NON_INCREMENTAL_LOAD_WITH_INCREMENTAL_FLAG =
            "To resume a non-incremental load, the '" + INCREMENTAL_LOAD + "' flag must not be set. " +
            "Use only the '" + RESUME + "' flag to resume a non-incremental load.";
}
