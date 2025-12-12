package com.aerospike.firefly.bulkloader.spark.structure;

import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.call.bulkload.utils.exception.BadCsvEntryException;
import com.aerospike.firefly.process.call.bulkload.utils.exception.InvalidCsvHeaderException;
import com.aerospike.firefly.structure.id.FireflyId;
import com.google.cloud.hadoop.repackaged.gcs.com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_CACHE_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_CACHE_HEADER;

public class SparkFireflyVertex extends SparkFireflyElement {
    private static final Logger LOG = LoggerFactory.getLogger(SparkFireflyVertex.class);
    private static final String DEFAULT_LABEL = "vertex";
    private static final Set<String> VALID_CARDINALITIES = Set.of(SINGLE_CARDINALITY, LIST_CARDINALITY, SET_CARDINALITY);
    private final Map<String, List<List<Object>>> toEdgeCache;
    private final Map<String, List<List<Object>>> fromEdgeCache;
    private final Map<String, VertexProperty.Cardinality> cardinalityMap;

    private SparkFireflyVertex(final Object id,
                               final String label,
                               final List<Map.Entry<String, Object>> properties,
                               final Map<String, List<List<Object>>> toEdgeCache,
                               final Map<String, List<List<Object>>> fromEdgeCache,
                               final Map<String, VertexProperty.Cardinality> cardinalityMap) {
        super(id, label, properties);
        this.toEdgeCache = toEdgeCache;
        this.fromEdgeCache = fromEdgeCache;
        this.cardinalityMap = cardinalityMap;
    }

    public Map<String, VertexProperty.Cardinality> getCardinalities() {
        return this.cardinalityMap;
    }

    public static SparkFireflyVertex createVertex(final GenericRowWithSchema row,
                                                  final String nullValue) {
        final String[] headers = row.schema().fieldNames();
        String id = null;
        String label = null;
        Map<String, List<List<Object>>> toEdgeCache = new HashMap<>();
        Map<String, List<List<Object>>> fromEdgeCache = new HashMap<>();
        final List<Map.Entry<String, Object>> properties = Collections.synchronizedList(new ArrayList<>());
        final Gson gson = new Gson();
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
                        toEdgeCache = gson.fromJson(mapString, new TypeToken<Map<String, List<List<Object>>>>() {
                        }.getType());
                    }
                } catch (final Exception e) {
                    LOG.error("Failed to generate Vertex property for header '" + header + "' from value: " + row.getAs(header));
                    throw new BadCsvEntryException(e);
                }
                continue;
            }
            if (header.equalsIgnoreCase(FROM_VERTEX_CACHE_HEADER)) {
                try {
                    final String mapString = row.getAs(header);
                    if (mapString != null && !mapString.isEmpty()) {
                        fromEdgeCache = gson.fromJson(mapString, new TypeToken<Map<String, List<List<Object>>>>() {
                        }.getType());
                    }
                } catch (final Exception e) {
                    LOG.error("Failed to generate Vertex property for header '" + header + "' from value: " + row.getAs(header));
                    throw new BadCsvEntryException(e);
                }
                continue;
            }

            try {
                final String value = row.getAs(header);
                final Map.Entry<String, Object> property = generateProperty(header, value, nullValue, VALID_CARDINALITIES);
                if (property.getValue() instanceof Collection) {
                    final Collection values = (Collection) property.getValue();
                    for (final Object val : values) {
                        properties.add(new AbstractMap.SimpleEntry<>(property.getKey(), val));
                    }
                } else {
                    properties.add(property);
                }
            } catch (final InvalidCsvHeaderException iche) {
                LOG.error("Failed to generate Vertex property for header '" + header + "' from value: " + row.getAs(header));
                throw iche;
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
        final Map<String, VertexProperty.Cardinality> cardinalityMap = generateCardinalityMap(headers);
        return new SparkFireflyVertex(PropertyValueParser.parseId(id), label, properties, toEdgeCache, fromEdgeCache, cardinalityMap);
    }

    private static Map<String, VertexProperty.Cardinality> generateCardinalityMap(final String[] headers) {
        final Map<String, VertexProperty.Cardinality> cardinalityMap = new HashMap<>();
        for (final String header : headers) {
            final Map<String, String> propertyInfo = getPropertyInfoFromHeader(header);
            final String cardinality = propertyInfo.get(PROPERTY_INFO_CARDINALITY);
            final String name = propertyInfo.get(PROPERTY_INFO_NAME);
            if (SINGLE_CARDINALITY.equals(cardinality)) {
                cardinalityMap.put(name, VertexProperty.Cardinality.single);
            } else if (LIST_CARDINALITY.equals(cardinality)) {
                cardinalityMap.put(name, VertexProperty.Cardinality.list);
            } else if (SET_CARDINALITY.equals(cardinality)) {
                cardinalityMap.put(name, VertexProperty.Cardinality.set);
            }
        }
        return cardinalityMap;
    }

    @Override
    public FireflyId getFireflyId(final AerospikeConnection db) {
        return db.getIdFactory().createVertexId(this.id);
    }

    public Optional<Map<String, List<FireflyId>>> getToEdgeCache(final AerospikeConnection db) {
        if (this.toEdgeCache.isEmpty()) {
            return Optional.empty();
        }
        final Map<String, List<FireflyId>> edgeIds = new HashMap<>();
        for (final String label : this.toEdgeCache.keySet()) {
            final List<List<Object>> idsList = this.toEdgeCache.get(label);
            final List<FireflyId> fireflyIds = new ArrayList<>();
            for (final List<Object> ids : idsList) {
                fireflyIds.add(db.getIdFactory().createCompositeEdgeId(
                        db.getIdFactory().createEdgeId(EdgeOperations.decodeEdgeIDFromString((String) ids.get(1))),
                        db.getIdFactory().createVertexId(ids.get(0))));
            }
            edgeIds.put(label, fireflyIds);
        }
        return Optional.of(edgeIds);
    }

    public Optional<Map<String, List<FireflyId>>> getFromEdgeCache(final AerospikeConnection db) {
        final Map<String, List<FireflyId>> edgeIds = new HashMap<>();
        for (final String label : this.fromEdgeCache.keySet()) {
            final List<List<Object>> idsList = this.fromEdgeCache.get(label);
            final List<FireflyId> fireflyIds = new ArrayList<>();
            for (final List<Object> ids : idsList) {
                fireflyIds.add(db.getIdFactory().createCompositeEdgeId(
                        db.getIdFactory().createEdgeId(EdgeOperations.decodeEdgeIDFromString((String) ids.get(1))),
                        db.getIdFactory().createVertexId(ids.get(0))));
            }
            edgeIds.put(label, fireflyIds);
        }
        return Optional.of(edgeIds);
    }

    @Override
    protected boolean isVertexProperty() {
        return true;
    }
}
