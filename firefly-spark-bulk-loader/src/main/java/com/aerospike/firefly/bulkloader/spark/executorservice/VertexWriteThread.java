package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.bulkloader.util.FireflyBulkLoaderException;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class VertexWriteThread implements Runnable {
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

    @Override
    public void run() {
        Thread.currentThread().setName("Write-vertex-thread-for-partitionId-" + this.partitionId);
        try {
            final SparkFireflyVertex sparkVertex =
                    SparkFireflyVertex.createVertex(this.row, this.ignoreFailedProperties, this.nullValue);
            int tryCount = 0;
            boolean succeeded = false;
            while (!succeeded) {
                try {
                    this.graph.writeVertex(sparkVertex.getFireflyId(this.graph.getBaseGraph()),
                            sparkVertex.getLabel(), sparkVertex.getProperties());
                    succeeded = true;
                } catch (final AerospikeException e) {
                    if (++tryCount > RETRY_LIMIT) {
                        LOGGER.error("Failed to write vertex with ID " + sparkVertex.getId() + " after "
                                + tryCount + " attempts.", e);
                        if (!this.ignoreElementCreationFailed) {
                            throw e;
                        } else {
                            break;
                        }
                    } else {
                        LOGGER.warn("Failed to write vertex with ID: " + sparkVertex.getId() +
                                ". Attempting to write vertex again. Attempt count: " + tryCount + ".", e);
                        SparkBulkLoader.exponentialBackoff(tryCount);
                    }
                }
            }
        } catch (final FireflyBulkLoaderException e) {
            LOGGER.error("Failed to load vertex for row: " + Arrays.toString(this.row.values()), e);
            if (!this.ignoreElementCreationFailed) {
                throw e;
            }
        }
    }
}
