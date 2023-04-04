package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.bulkloader.util.FireflyBulkLoaderException;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.Callable;

public class VertexWriteThread implements Callable<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertexWriteThread.class);
    private final boolean ignoreFailedProperties;
    private final boolean ignoreElementCreationFailed;
    private final String nullValue;
    private final FireflyGraph graph;
    private final GenericRowWithSchema row;
    private static final int RETRY_LIMIT = 100;
    private final int partitionId;

    public VertexWriteThread(final boolean ignoreFailedProperties,
                             final boolean ignoreElementCreationFailed,
                             final String nullValue,
                             final FireflyGraph graph,
                             final GenericRowWithSchema row,
                             final int partitionId) {
        this.ignoreFailedProperties = ignoreFailedProperties;
        this.ignoreElementCreationFailed = ignoreElementCreationFailed;
        this.nullValue = nullValue;
        this.graph = graph;
        this.row = row;
        this.partitionId = partitionId;
    }

    /**
     * Function to write vertex to Aerospike.
     *
     * @return true if error occurred while writing vertex, false otherwise.
     */
    @Override
    public Boolean call() {
        Thread.currentThread().setName("Write-vertex-thread-for-partitionId-" + this.partitionId);
        try {
            final SparkFireflyVertex sparkVertex =
                    SparkFireflyVertex.createVertex(this.row, this.ignoreFailedProperties, this.nullValue);
            int tryCount = 0;
            while (true) {
                try {
                    this.graph.writeVertex(sparkVertex.getFireflyId(this.graph.getBaseGraph()),
                            sparkVertex.getLabel(), sparkVertex.getProperties());
                    return false;
                } catch (final AerospikeException e) {
                    if (e.getResultCode() == ResultCode.RECORD_TOO_BIG) {
                        // No point in retrying this kind of error.
                        LOGGER.error("Record too big for vertex with id '{}', label '{}', properties '{}'. Row value: '{}'.",
                                sparkVertex.getFireflyId(this.graph.getBaseGraph()), sparkVertex.getLabel(), sparkVertex.getProperties(), Arrays.toString(row.values()));
                        // If ignoreElementCreationFailed is true and an error occurred, we should ignore the error (return false).
                        // If ignoreElementCreationFailed is false and an error occurred, we should return that an error occurred (return true).
                        return !this.ignoreElementCreationFailed;
                    }
                    if (++tryCount > RETRY_LIMIT) {
                        LOGGER.error("Failed to write vertex with ID {} after {} attempts. Vertex properties: {}. Row value: {}.",
                                sparkVertex.getId(), tryCount, sparkVertex.getProperties(), Arrays.toString(row.values()), e);
                        // If ignoreElementCreationFailed is true and an error occurred, we should ignore the error (return false).
                        // If ignoreElementCreationFailed is false and an error occurred, we should return that an error occurred (return true).
                        return !this.ignoreElementCreationFailed;
                    } else {
                        LOGGER.warn("Failed to write vertex with ID: " + sparkVertex.getId() +
                                ". Attempting to write vertex again. Attempt count: " + tryCount + ".", e);
                        SparkBulkLoader.exponentialBackoff(tryCount);
                    }
                }
            }
        } catch (final FireflyBulkLoaderException e) {
            LOGGER.error("Failed to write vertex. Row value: {}.", Arrays.toString(row.values()), e);
            // If ignoreElementCreationFailed is true and an error occurred, we should ignore the error (return false).
            // If ignoreElementCreationFailed is false and an error occurred, we should return that an error occurred (return true).
            return !this.ignoreElementCreationFailed;
        }
    }
}
