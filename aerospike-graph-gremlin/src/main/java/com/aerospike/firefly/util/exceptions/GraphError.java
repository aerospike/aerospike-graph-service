package com.aerospike.firefly.util.exceptions;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.util.config.ConfigurationHelper;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static com.aerospike.client.ResultCode.ASYNC_QUEUE_FULL;
import static com.aerospike.client.ResultCode.BATCH_FAILED;
import static com.aerospike.client.ResultCode.CLIENT_ERROR;
import static com.aerospike.client.ResultCode.INVALID_NODE_ERROR;
import static com.aerospike.client.ResultCode.MAX_ERROR_RATE;
import static com.aerospike.client.ResultCode.MAX_RETRIES_EXCEEDED;
import static com.aerospike.client.ResultCode.NO_MORE_CONNECTIONS;
import static com.aerospike.client.ResultCode.NO_RESPONSE;
import static com.aerospike.client.ResultCode.PARSE_ERROR;
import static com.aerospike.client.ResultCode.QUERY_TERMINATED;
import static com.aerospike.client.ResultCode.SCAN_TERMINATED;
import static com.aerospike.client.ResultCode.SERIALIZE_ERROR;
import static com.aerospike.client.ResultCode.SERVER_NOT_AVAILABLE;
import static com.aerospike.firefly.structure.FireflyElement.TTL_PROPERTY_KEY;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.MAX_CONNECTIONS_PER_NODE;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.CLEAR_ON_VERSION_INCOMPATIBILITY;

/**
 * Client codes are absolute value of ResultCode + 1000
 * <p>
 * Server codes remain as-is, retaining their value in the 0-1000 range
 * <p>
 * Custom AGS codes will start from 1100
 */
public enum GraphError {
    GRAPH_CLIENT_ERROR(clientResultCodeToEnum(CLIENT_ERROR)),
    GRAPH_PARSE_ERROR(clientResultCodeToEnum(PARSE_ERROR)),
    GRAPH_INVALID_NODE_ERROR(clientResultCodeToEnum(INVALID_NODE_ERROR)),
    GRAPH_SCAN_TERMINATED(clientResultCodeToEnum(SCAN_TERMINATED)),
    GRAPH_QUERY_TERMINATED(clientResultCodeToEnum(QUERY_TERMINATED)),
    GRAPH_NO_MORE_CONNECTIONS(clientResultCodeToEnum(NO_MORE_CONNECTIONS)),
    GRAPH_SERVER_NOT_AVAILABLE(clientResultCodeToEnum(SERVER_NOT_AVAILABLE)),
    GRAPH_ASYNC_QUEUE_FULL(clientResultCodeToEnum(ASYNC_QUEUE_FULL)),
    GRAPH_SERIALIZE_ERROR(clientResultCodeToEnum(SERIALIZE_ERROR)),
    GRAPH_MAX_RETRIES_EXCEEDED(clientResultCodeToEnum(MAX_RETRIES_EXCEEDED)),
    GRAPH_MAX_ERROR_RATE(clientResultCodeToEnum(MAX_ERROR_RATE)),
    GRAPH_NO_RESPONSE(clientResultCodeToEnum(NO_RESPONSE)),
    GRAPH_BATCH_FAILED(clientResultCodeToEnum(BATCH_FAILED)),

    AUTH_NOT_INITIALIZED(1100),
    AUTH_TOKEN_EXPIRED(1101),
    AUTH_USER_CONTEXT_INVALID(1102),
    AUTH_DISABLED_CREDENTIALS(1103),
    AUTH_USER_PARAM_NOT_FOUND(1104),
    AUTH_USER_INVALID_ROLE(1105),
    AUTH_USER_WRITE_REQUIRED(1106),
    AUTH_USER_ADMIN_REQUIRED(1107),
    AUTH_USER_READ_REQUIRED(1108),
    TTL_NOT_ENABLED(1109),
    DEFAULT_TTL_EXISTS(1110),
    NO_ACTIVE_NODES(1111),
    DROP_INDEX_UNAUTHORIZED(1112),
    DATA_MODEL_VERSION_MISMATCH(1113),
    CACHE_ADJACENT_ENABLED_COMPOSITE_ID_DISABLED(1114),
    SINDEX_RECENTLY_DROPPED(1115),
    TTL_ILLEGAL_ARGUMENT(1116),
    THREAD_LIMIT_EXCEEDED(1117),
    SET_CARDINALITY_NOT_SUPPORTED(1118),
    MRT_NOT_SUPPORTED(1119),
    NSUP_DISABLED(1120),
	
    ELEMENT_NOT_FOUND(ResultCode.KEY_NOT_FOUND_ERROR),
    RECORD_SIZE_EXCEEDED(ResultCode.RECORD_TOO_BIG),
    OUT_OF_MEMORY(ResultCode.SERVER_MEM_ERROR);

    public final int code;

    GraphError(final int code) {
        this.code = code;
    }

    static int clientResultCodeToEnum(final int clientResultCode) {
        return Math.abs(clientResultCode) + 1000;
    }

    static final HashMap<Integer, String> ERROR_MESSAGES = new HashMap<>();
    static {
        // Graph
        ERROR_MESSAGES.put(NSUP_DISABLED.code, String.format("'%s' set to 0 (disabled) and '%s' disabled. These are required for TTL to work," +
                " and this will MergeE support in Aerospike Graph Service. To enable MergeE," +
                " please set '%s' to a non-zero value.", AerospikeConnection.InfoOps.NSUP_PERIOD, AerospikeConnection.InfoOps.ALLOW_TTL_WITHOUT_NSUP, AerospikeConnection.InfoOps.NSUP_PERIOD));
        ERROR_MESSAGES.put(CACHE_ADJACENT_ENABLED_COMPOSITE_ID_DISABLED.code, String.format(
                "Cached adjacent ID strategy (%s) cannot be used when composite ID strategy (%s) is disabled.",
                ConfigurationHelper.Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY,
                ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_STRATEGY));
        ERROR_MESSAGES.put(AUTH_NOT_INITIALIZED.code, "Authentication is not initialized.");
        ERROR_MESSAGES.put(AUTH_TOKEN_EXPIRED.code, "Token has expired.");
        ERROR_MESSAGES.put(AUTH_USER_CONTEXT_INVALID.code, "User context is invalid.");
        ERROR_MESSAGES.put(AUTH_DISABLED_CREDENTIALS.code, "Authentication is disabled but credentials were provided.");
        ERROR_MESSAGES.put(AUTH_USER_PARAM_NOT_FOUND.code, "User not valid; no user found in parameters.");
        ERROR_MESSAGES.put(AUTH_USER_INVALID_ROLE.code, "User does not have a valid role.");
        ERROR_MESSAGES.put(AUTH_USER_WRITE_REQUIRED.code, "User does not have write access.");
        ERROR_MESSAGES.put(AUTH_USER_ADMIN_REQUIRED.code, "User does not have admin access.");
        ERROR_MESSAGES.put(AUTH_USER_READ_REQUIRED.code, "User does not have read access.");
        ERROR_MESSAGES.put(TTL_NOT_ENABLED.code, "TTL must be enabled to set '" + TTL_PROPERTY_KEY + "'.");
        ERROR_MESSAGES.put(DEFAULT_TTL_EXISTS.code, "Graph cannot run in a namespace that has a 'default-ttl'. " +
                "Aerospike has a non-zero 'default-ttl' in one or more nodes. " +
                "Please disable it for all nodes in namespace.");
        ERROR_MESSAGES.put(NO_ACTIVE_NODES.code, "No active server nodes found in cluster.");
        ERROR_MESSAGES.put(DROP_INDEX_UNAUTHORIZED.code, "Failed to drop index due to role violation. Please check the permissions of the role assigned.");
        ERROR_MESSAGES.put(DATA_MODEL_VERSION_MISMATCH.code, "The on-disk data model version '%s' is not compatible with the AGS version '%s' being used. To fix this, either use Aerospike Graph '%s', use a new namespace, or start with flag `" + CLEAR_ON_VERSION_INCOMPATIBILITY + "` which will delete the old data and allow using the new model.");
        ERROR_MESSAGES.put(SINDEX_RECENTLY_DROPPED.code, "This query is temporarily unavailable due to the index it utilizes%s being recently dropped. Please wait %s seconds and try again.");
        ERROR_MESSAGES.put(TTL_ILLEGAL_ARGUMENT.code, "Invalid value for TTL provided. Provided input [%s] of type %s must be numeric instead.");
        ERROR_MESSAGES.put(THREAD_LIMIT_EXCEEDED.code, "AGS has reached the server’s current query-thread limit. " +
                "Please raise the query-threads-limit setting or reduce concurrent queries and try again.");
        ERROR_MESSAGES.put(SET_CARDINALITY_NOT_SUPPORTED.code, "Cardinality.set is not supported in Aerospike Graph. Use Cardinality.list or Cardinality.single.");
        ERROR_MESSAGES.put(MRT_NOT_SUPPORTED.code, "Transactions require Aerospike database version 8 or newer with strong consistency mode enabled. Please verify that all nodes in the cluster are running a compatible version of Aerospike.");

        // Server
        ERROR_MESSAGES.put(ELEMENT_NOT_FOUND.code, "Element was dropped and no longer exists.");
        ERROR_MESSAGES.put(RECORD_SIZE_EXCEEDED.code, "Max record size exceeded. This is typically caused by too many " +
                "Properties / Edges added to an Element. Consider breaking this Element into more Elements.");
        ERROR_MESSAGES.put(OUT_OF_MEMORY.code, "Aerospike server side memory error detected. " +
                "Check index memory usage and/or increase server memory in Aerospike configuration.");

        // Client
        ERROR_MESSAGES.put(GRAPH_NO_MORE_CONNECTIONS.code, "There are no more available connections. Consider increasing the maximum allowable amount via the '" +
                MAX_CONNECTIONS_PER_NODE + "' configuration key or contact support if problem persists.");

        // TODO GRAPH-1307: Add more error messages
    }

    static String getMessage(final AerospikeException e) {
        int errorCode = e.getResultCode();
        final StringBuilder errorMessage = new StringBuilder();
        if (errorCode < 0) {
            errorCode = clientResultCodeToEnum(errorCode);
        }
        errorMessage.append("Error code ");
        errorMessage.append(errorCode);
        errorMessage.append(": ");
        if (ERROR_MESSAGES.containsKey(errorCode)) {
            errorMessage.append(ERROR_MESSAGES.get(errorCode));
        } else {
            if (e.getResultCode() < 0) {
                errorMessage.append("Unexpected Aerospike Graph Service client error. ");
            } else {
                errorMessage.append("Unexpected Aerospike Graph Service server error. ");
            }
            errorMessage.append("Please refer to troubleshooting or contact support if problem persists. ");
            errorMessage.append(e.getMessage());
        }
        return errorMessage.toString();
    }

    public static String getMessage(final GraphError error) {
        final String errorMessage = ERROR_MESSAGES.get(error.code);
        if (errorMessage == null) {
            // This should never happen.
            throw new IllegalArgumentException("Designated graph errors should always have a message");
        }
        return errorMessage;
    }

    // A dumb hack because we lose exception context and don't know if it is checked or unchecked, so need to use this.
    public static void sneakyThrow(final ExecutionException e) {
        final Throwable t = e.getCause();
        if (t == null) sneakyThrowInternal(e); // Shouldn't happen, but if it does we want the context.
        sneakyThrowInternal(t);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrowInternal(final Throwable t) throws T {
        throw (T) t; // unchecked throw
    }
}
