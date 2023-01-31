package com.aerospike.firefly.spark.bulkloader.structure;

import com.aerospike.firefly.spark.bulkloader.util.FireflyBulkLoaderException;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkFireflyEdge extends SparkFireflyElement {
    private static final Logger LOG = LoggerFactory.getLogger(SparkFireflyEdge.class);
    private static final String DEFAULT_LABEL = "edge";
    public static final String FROM_VERTEX_HEADER = "~from";
    public static final String TO_VERTEX_HEADER = "~to";

    private final long fromVertexId;
    private final long toVertexId;

    private SparkFireflyEdge(final long edgeId, final String label, final long fromVertexId, final long toVertexId,
                             final List<Map.Entry<String, Object>> properties) {
        super(edgeId, label, properties);
        this.fromVertexId = fromVertexId;
        this.toVertexId = toVertexId;
    }

    public static SparkFireflyEdge createEdge(final GenericRowWithSchema row,
                                              final boolean ignoreParseFailedProperties,
                                              final boolean useProvidedId,
                                              final boolean keepProvidedId,
                                              final String providedIdPropertyName,
                                              final String nullValue,
                                              final FireflyGraph graph) {
        final String[] headers = row.schema().fieldNames();
        String id = null;
        String label = null;
        String fromVertexId = null;
        String toVertexId = null;
        final List<Map.Entry<String, Object>> properties = new ArrayList<>();
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
                LOG.warn("Failed to generate property for header '" + header + "' from value: " + row.getAs(header), e);
                if (!ignoreParseFailedProperties) {
                    throw new FireflyBulkLoaderException(e);
                }
            }
        }
        if ((id == null && useProvidedId) || fromVertexId == null || toVertexId == null) {
            throw new FireflyBulkLoaderException("Could not generate edge due to a required value being blank.");
        }
        if (label == null) {
            label = DEFAULT_LABEL;
        }
        final long edgeId;
        if (useProvidedId) {
            edgeId = Long.parseLong(id);
        } else {
            edgeId = graph.edgeIdManager.getNextId(graph);
            if (keepProvidedId && id != null) {
                properties.add(generateProperty(providedIdPropertyName, id, nullValue));
            }
        }
        return new SparkFireflyEdge(edgeId, label, Long.parseLong(fromVertexId), Long.parseLong(toVertexId),
                properties);
    }

    @Override
    public FireflyId getFireflyId(final String setName) {
        return FireflyIdPoly.fromObject(this.id, setName);
    }

    public long getInVertexId() {
        return this.toVertexId;
    }

    public long getOutVertexId() {
        return this.fromVertexId;
    }
}
