package com.aerospike.firefly.bulkloader.spark.structure;

import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.call.bulkload.utils.exception.BadCsvEntryException;
import com.aerospike.firefly.structure.id.FireflyId;
import com.amazonaws.thirdparty.jackson.core.JsonProcessingException;
import com.amazonaws.thirdparty.jackson.databind.JsonMappingException;
import com.amazonaws.thirdparty.jackson.databind.ObjectMapper;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_CACHE_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_CACHE_HEADER;

public class SparkFireflyVertex extends SparkFireflyElement {
    private static final Logger LOG = LoggerFactory.getLogger(SparkFireflyVertex.class);
    private static final String DEFAULT_LABEL = "vertex";
    private final Map<String, List<List<Object>>> toEdgeCache;
    private final Map<String, List<List<Object>>> fromEdgeCache;

    private SparkFireflyVertex(final Object id,
                               final String label,
                               final List<Map.Entry<String, Object>> properties,
                               final Map<String, List<List<Object>>> toEdgeCache,
                               final Map<String, List<List<Object>>> fromEdgeCache) {
        super(id, label, properties);
        this.toEdgeCache = toEdgeCache;
        this.fromEdgeCache = fromEdgeCache;
    }

    public static SparkFireflyVertex createVertex(final GenericRowWithSchema row,
                                                  final String nullValue) {
        final String[] headers = row.schema().fieldNames();
        String id = null;
        String label = null;
        Map<String, List<List<Object>>> toEdgeCache = new HashMap<>();
        Map<String, List<List<Object>>> fromEdgeCache = new HashMap<>();
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
            if (header.equalsIgnoreCase(TO_VERTEX_CACHE_HEADER)) {
                try {
                    final String mapString = row.getAs(header);
                    if (mapString != null && !mapString.isEmpty()) {
                        toEdgeCache = new ObjectMapper().readValue(mapString, HashMap.class);
                    }
                } catch (final JsonProcessingException e) {
                    LOG.error("Failed to generate Vertex property for header '" + header + "' from value: " + row.getAs(header));
                    throw new BadCsvEntryException(e);
                }
                continue;
            }
            if (header.equalsIgnoreCase(FROM_VERTEX_CACHE_HEADER)) {
                try {
                    final String mapString = row.getAs(header);
                    if (mapString != null && !mapString.isEmpty()) {
                        fromEdgeCache = new ObjectMapper().readValue(mapString, HashMap.class);
                    }
                } catch (final JsonProcessingException e) {
                    LOG.error("Failed to generate Vertex property for header '" + header + "' from value: " + row.getAs(header));
                    throw new BadCsvEntryException(e);
                }
                continue;
            }

            try {
                final String value = row.getAs(header);
                final Map.Entry<String, Object> property = generateProperty(header, value, nullValue);
                properties.add(property);
            } catch (final RuntimeException e) {
                LOG.error("Failed to generate Vertex property for header '" + header + "' from value: " + row.getAs(header));
                throw new BadCsvEntryException(e);
            }
        }
        if (id == null) {
            throw new BadCsvEntryException("Could not generate vertex due to ~id being blank.");
        }
        if (label == null) {
            label = DEFAULT_LABEL;
        }
        return new SparkFireflyVertex(PropertyValueParser.parseId(id), label, properties, toEdgeCache, fromEdgeCache);
    }

    @Override
    public FireflyId getFireflyId(final AerospikeConnection db) {
        return db.getIdFactory().createVertexId(this.id);
    }

    public Map<String, List<FireflyId>> getToEdgeCache(final AerospikeConnection db) {
        final Map<String, List<FireflyId>> edgeIds = new HashMap<>();
        for (final String label : this.toEdgeCache.keySet()) {
            final List<List<Object>> idsList = this.toEdgeCache.get(label);
            final List<FireflyId> fireflyIds = new ArrayList<>();
            for (final List<Object> ids : idsList) {
                fireflyIds.add(db.getIdFactory().createCompositeEdgeId(
                        db.getIdFactory().createEdgeId(EdgeOperations.decodeEdgeIDFromString((String) ids.get(0))),
                        db.getIdFactory().createVertexId(ids.get(1))));
            }
            edgeIds.put(label, fireflyIds);
        }
        return edgeIds;
    }

    public Map<String, List<FireflyId>> getFromEdgeCache(final AerospikeConnection db) {
        final Map<String, List<FireflyId>> edgeIds = new HashMap<>();
        for (final String label : this.fromEdgeCache.keySet()) {
            final List<List<Object>> idsList = this.fromEdgeCache.get(label);
            final List<FireflyId> fireflyIds = new ArrayList<>();
            for (final List<Object> ids : idsList) {
                fireflyIds.add(db.getIdFactory().createCompositeEdgeId(
                        db.getIdFactory().createEdgeId(EdgeOperations.decodeEdgeIDFromString((String) ids.get(0))),
                        db.getIdFactory().createVertexId(ids.get(1))));
            }
            edgeIds.put(label, fireflyIds);
        }
        return edgeIds;
    }
}
