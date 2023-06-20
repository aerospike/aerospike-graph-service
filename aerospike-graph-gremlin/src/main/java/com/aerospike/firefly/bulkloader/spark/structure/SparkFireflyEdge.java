package com.aerospike.firefly.bulkloader.spark.structure;

import com.aerospike.firefly.bulkloader.exception.FireflyBulkLoaderException;
import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SparkFireflyEdge extends SparkFireflyElement {
    public static final String FROM_VERTEX_HEADER = "~from";
    public static final String TO_VERTEX_HEADER = "~to";
    private static final Logger LOG = LoggerFactory.getLogger(SparkFireflyEdge.class);
    private static final String DEFAULT_LABEL = "edge";
    private final Object fromVertexId;
    private final Object toVertexId;

    private SparkFireflyEdge(final byte[] edgeId, final String label, final Object fromVertexId, final Object toVertexId,
                             final List<Map.Entry<String, Object>> properties) {
        super(edgeId, label, properties);
        this.fromVertexId = fromVertexId;
        this.toVertexId = toVertexId;
    }

    public static SparkFireflyEdge createEdge(final GenericRowWithSchema row,
                                              final boolean keepProvidedId,
                                              final String providedIdPropertyName,
                                              final String nullValue,
                                              final FireflyGraph graph,
                                              final boolean forVerification) {
        final String[] headers = row.schema().fieldNames();
        String id = null;
        String label = null;
        String fromVertexId = null;
        String toVertexId = null;
        final List<Map.Entry<String, Object>> properties = Collections.synchronizedList(new ArrayList<>());
        for (final String header : headers) {
            if (row.getAs(header) == null) {
                continue;
            }
            if (header.equalsIgnoreCase(ID_HEADER)) {
                id = row.getAs(header);
                continue;
            }
            if (header.equalsIgnoreCase(LABEL_HEADER)) {
                label = row.getAs(header);
                continue;
            }
            if (header.equalsIgnoreCase(FROM_VERTEX_HEADER)) {
                fromVertexId = row.getAs(header);
                continue;
            }
            if (header.equalsIgnoreCase(TO_VERTEX_HEADER)) {
                toVertexId = row.getAs(header);
                continue;
            }
            try {
                final Map.Entry<String, Object> property = generateProperty(header, row.getAs(header), nullValue);
                properties.add(property);
            } catch (final RuntimeException e) {
                LOG.error("Failed to generate Edge property for header '" + header + "' from value: " + row.getAs(header));
                throw new FireflyBulkLoaderException(e);
            }
        }
        if (fromVertexId == null || toVertexId == null) {
            throw new FireflyBulkLoaderException("Could not generate edge due to ~from or ~to being blank.");
        }
        if (label == null) {
            label = DEFAULT_LABEL;
        }
        final byte[] edgeId;
        if (forVerification) {
            edgeId = null;
        } else {
            edgeId = graph.edgeIdManager.getNextId(graph);
        }
        if (keepProvidedId && id != null) {
            properties.add(generateProperty(providedIdPropertyName, id, nullValue));
        }
        return new SparkFireflyEdge(edgeId, label, PropertyValueParser.parseId(fromVertexId), PropertyValueParser.parseId(toVertexId), properties);
    }

    @Override
    public FireflyId getFireflyId(final AerospikeConnection db) {
        if (this.id == null) {
            // This should never happen since we don't invoke this method for verification, and that would be the only
            // time this is null.
            throw new UnsupportedOperationException("Can't get SparkFireflyEdge in verification mode.");
        }
        return new FireflyPhatEdgeId(ByteBuffer.wrap((byte[]) this.id), db.PHAT_EDGE_SIZE, db.EDGE_AERO_SET);
    }

    public Object getInVertexId() {
        return this.toVertexId;
    }

    public Object getOutVertexId() {
        return this.fromVertexId;
    }
}
