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
    private boolean ignoreFailedProperties;
    private boolean ignoreElementCreationFailed;
    private String nullValue;
    private FireflyGraph graph;
    private GenericRowWithSchema row;
    private static final int RETRY_LIMIT = 100;

    public VertexWriteThread(boolean ignoreFailedProperties, boolean ignoreElementCreationFailed, String nullValue, FireflyGraph graph, GenericRowWithSchema row) {
        this.ignoreFailedProperties = ignoreFailedProperties;
        this.ignoreElementCreationFailed = ignoreElementCreationFailed;
        this.nullValue = nullValue;
        this.graph = graph;
        this.row = row;
    }

    @Override
    public void run() {
        try {
            final SparkFireflyVertex sparkVertex =
                    SparkFireflyVertex.createVertex(row, ignoreFailedProperties, nullValue);
            int tryCount = 0;
            boolean succeeded = false;
            while (!succeeded) {
                try {
                    graph.writeVertex(sparkVertex.getFireflyId(graph.getBaseGraph().VERTEX_AERO_SET),
                            sparkVertex.getLabel(), sparkVertex.getProperties());
                    succeeded = true;
                } catch (final AerospikeException e) {
                    if (++tryCount > RETRY_LIMIT) {
                        LOGGER.error("Failed to write vertex with ID " + sparkVertex.getId() + " after "
                                + tryCount + " attempts.", e);
                        if (!ignoreElementCreationFailed) {
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
            LOGGER.error("Failed to load vertex for row: " + Arrays.toString(row.values()), e);
            if (!ignoreElementCreationFailed) {
                throw e;
            }
        }
    }
}
