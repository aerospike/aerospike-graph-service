package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.bulkloader.SparkBulkLoaderMain;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.Callable;

public class VertexWriteThread implements Callable<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertexWriteThread.class);
    private final String nullValue;
    private final FireflyGraph graph;
    private final GenericRowWithSchema fireflyRow;
    private final GenericRowWithSchema metadataRow;
    private static final int RETRY_LIMIT = 100;
    private final int partitionId;

    public VertexWriteThread(final String nullValue,
                             final FireflyGraph graph,
                             final GenericRowWithSchema fireflyRow,
                             final int partitionId,
                             final GenericRowWithSchema metadataRow) {
        this.nullValue = nullValue;
        this.graph = graph;
        this.fireflyRow = fireflyRow;
        this.partitionId = partitionId;
        this.metadataRow = metadataRow;

    }

    /**
     * Function to write vertex to Aerospike.
     *
     * @return true if error occurred while writing vertex, false otherwise.
     */
    @Override
    public Boolean call() {
        Thread.currentThread().setName("Write-vertex-thread-for-partitionId-" + this.partitionId);
        final SparkFireflyVertex sparkVertex =
                SparkFireflyVertex.createVertex(this.fireflyRow, this.nullValue);
        int tryCount = 0;
        boolean firstWrite = true;
        while (true) {
            try {
                this.graph.bulkWriteVertex(sparkVertex.getFireflyId(this.graph.getBaseGraph()),
                        sparkVertex.getLabel(), sparkVertex.getProperties(), firstWrite);
                return false;
            } catch (final AerospikeException e) {
                if (e.getResultCode() == ResultCode.RECORD_TOO_BIG) {
                    // No point in retrying this kind of error.
                    LOGGER.error("Record too big for vertex with id '{}', label '{}', properties '{}', FireflyRow value: '{}', MetadataRow value: '{}'",
                            sparkVertex.getFireflyId(this.graph.getBaseGraph()), sparkVertex.getLabel(), sparkVertex.getProperties(), Arrays.toString(fireflyRow.values()),  Arrays.toString(metadataRow.values()));
                    // Return true to signal error.
                    return true;
                }
                if (e.getResultCode() == ResultCode.KEY_EXISTS_ERROR) {
                    LOGGER.error("Failed to write vertex due to the a vertex with the provided id '{}' already existing. FireflyRow value: '{}', MetadataRow value: '{}'",
                            sparkVertex.getFireflyId(this.graph.getBaseGraph()), Arrays.toString(fireflyRow.values()),  Arrays.toString(metadataRow.values()));
                    throw e;
                }
                if (++tryCount > RETRY_LIMIT) {
                    LOGGER.error("Failed to write vertex with ID {} after {} attempts. Vertex properties: {}. FireflyRow value: {}, MetadataRow value: '{}'",
                            sparkVertex.getId(), tryCount, sparkVertex.getProperties(), Arrays.toString(fireflyRow.values()), Arrays.toString(metadataRow.values()), e);
                    // Return true to signal error.
                    return true;
                } else {
                    LOGGER.warn("Failed to write vertex with ID: " + sparkVertex.getId() +
                            ". Attempting to write vertex again. Attempt count: " + tryCount + ".", e);
                    SparkBulkLoaderMain.exponentialBackoff(tryCount);
                    firstWrite = false;
                }
            }
        }
    }
}
