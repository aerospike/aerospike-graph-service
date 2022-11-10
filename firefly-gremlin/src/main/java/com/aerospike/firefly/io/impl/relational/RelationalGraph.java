package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.linked.LinkedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedVertex;
import com.aerospike.firefly.io.utils.GenerationCheck;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.aerospike.firefly.io.impl.relational.RelationalVertex.getPropertyIdMap;
import static com.aerospike.firefly.io.impl.relational.RelationalVertex.getPropertyValueIdMaps;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.GRAPH_VARIABLES_RECORD;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class RelationalGraph extends FireflyGraph {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalGraph.class);
    public static final String FIREFLY_CONFIGURATION_VARIABLE_NAME = "FIREFLY_CONFIGURATION";

    /**
     * Constructor for RelationalGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public RelationalGraph(AerospikeConnection db, final Configuration conf) {
        super(db, conf);
    }


    /**
     * Function to write edge to Aerospike.
     *
     * @param edgeId     Edge id.
     * @param label      Edge label.
     * @param properties Edge properties.
     * @param inVertex   In vertex of edge.
     * @param outVertex  Out vertex of edge.
     * @return Edge.
     */
    @Override
    public FireflyEdge writeEdge(final FireflyId edgeId,
                                 final String label,
                                 final List<Map.Entry<String, Object>> properties,
                                 final FireflyVertex inVertex,
                                 final FireflyVertex outVertex) {
        // Write edge to vertex, if edge write fails, null check on edge record will protect from inconsistent data.

        // Add edge to inVertex and outVertex.
        if (!this.getBaseGraph().EDGE_CACHE_DISABLED_GLOBALLY) { //disable RMW pattern and rely on index for edge lookups
            inVertex.writeEdge(Direction.IN, edgeId, label);
            outVertex.writeEdge(Direction.OUT, edgeId, label);
        }

        // Write edge to Aerospike and return FireflyEdge.
        return RelationalEdge.writeEdge(this, edgeId, label, properties, inVertex, outVertex);
    }

    @Override
    public void bulkWriteEdge(final long edgeId, final String label, final List<Map.Entry<String, Object>> properties,
                              final long inVertexId, final long outVertexId) {
        LOG.debug("Writing edge {} [({})-({})->({})] {}.", edgeId, outVertexId, label, inVertexId, properties);

        final Map<String, Object> data = new HashMap<>();
        final Map<String, Object> typeHints = new HashMap<>();
        properties.forEach(prop -> {
            final String key = prop.getKey();
            final Object value = prop.getValue();
            FireflyHelper.validatePropertyValue(value);

            if (value != null)
                typeHints.put(key, this.db.getSupportedType(value.getClass()));
            else
                typeHints.put(key, null);

            if (properties.stream().filter(p -> p.getKey().equals(key)).count() > 1) {
                typeHints.put(key, this.db.getSupportedType(List.class));
                if (data.containsKey(key)) {
                    ((List<Object>) (data.get(key))).add(value);
                } else {
                    List<Object> temp = new ArrayList<>();
                    temp.add(value);
                    data.put(key, temp);
                }
            } else {
                data.put(key, value);
            }

        });
        final Bin labelBin = new Bin(AerospikeConnection.LABEL, Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(inVertexId));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(outVertexId));
        final Bin valueBin = new Bin(this.db.EDGE_AERO_SET, Value.get(data));
        final Bin typeHintBin = new Bin(this.db.TYPE_HINTS, Value.get(typeHints));
        FireflyRecord.writeElement(this.db, this.db.EDGE_AERO_SET, FireflyIdFactory.createId(edgeId), -1, labelBin,
                inVbin, outVBin, valueBin, typeHintBin);
    }

    public void bulkWriteEdgeToVertices(final long inVertexId, final long outVertexId,
                                        final long edgeId, final String edgeLabel) {
        final FireflyId fireflyEdgeId = FireflyIdFactory.createId(edgeId);
        GenerationCheck.writeGenerationCheck(() -> protectedWriteEdgeToVertices(
                FireflyIdFactory.createId(inVertexId), Direction.IN, fireflyEdgeId, edgeLabel));
        GenerationCheck.writeGenerationCheck(() -> protectedWriteEdgeToVertices(
                FireflyIdFactory.createId(outVertexId), Direction.OUT, fireflyEdgeId, edgeLabel));
    }

    private void protectedWriteEdgeToVertices(final FireflyId vertexId, final Direction direction,
                                              final FireflyId edgeId, final String edgeLabel) {

        // Get direction and counter keys. Direction must be IN or OUT.
        final String directionKey = direction == Direction.IN ? db.IN_EDGES : db.OUT_EDGES;
        final String counterKey = direction == Direction.IN ? db.IN_EDGE_COUNTER : db.OUT_EDGE_COUNTER;

        // Get existing Firefly record for the vertex.
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.VERTEX_AERO_SET, vertexId);

        // Initialize edge counter, cache disable flag, edge label map, and generation.
        long edgeCounter = 0;
        boolean cacheDisabled = false;
        Map<String, List<Long>> labelEdges = new HashMap<>();
        int generation = -1;

        // If the Firefly record is not null, grab existing edge data from it.
        if (fireflyRecord != null && fireflyRecord.record != null) {
            labelEdges = (Map<String, List<Long>>) Optional.ofNullable(fireflyRecord.record().getMap(directionKey)).orElse(new HashMap<>());
            edgeCounter = fireflyRecord.record().getLong(counterKey);
            cacheDisabled = fireflyRecord.record().getBoolean(db.CACHE_DISABLED);
            generation = fireflyRecord.record().generation;
        }

        final Bin[] bins;
        edgeCounter++;
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        if (cacheDisabled) {
            // Cache is already disabled.
            bins = new Bin[]{edgeCounterBin};
        } else if (edgeCounter >= db.ID_CACHE_SIZE) {
            // Cache is now disabled due to growing too big.
            final Bin cacheDisabledBin = new Bin(db.CACHE_DISABLED, Value.get(true));
            bins = new Bin[]{edgeCounterBin, cacheDisabledBin};
        } else {
            // Add the edge to the cache in the vertex if the cache has not grown too big.
            final List<Long> edges = labelEdges.getOrDefault(edgeLabel, new ArrayList<>());
            edges.add((Long) edgeId.getStorageId());

            // Add edges to edge label map.
            labelEdges.put(edgeLabel, edges);

            // Write edge label map back to vertex.
            final Bin edgeDataBin = new Bin(directionKey, Value.get(labelEdges));
            final Bin cacheDisabledBin = new Bin(db.CACHE_DISABLED, Value.get(false));
            bins = new Bin[]{edgeDataBin, edgeCounterBin, cacheDisabledBin};
        }

        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, generation, bins);
    }

    protected abstract int getTypeHint();

    /**
     * Function to write vertex to Aerospike.
     *
     * @param idValue    Id of vertex.
     * @param label      Label of vertex.
     * @param properties List of vertex properties.
     * @return Vertex.
     */
    @Override
    public FireflyVertex writeVertex(final FireflyId idValue,
                                     final String label,
                                     final List<Map.Entry<String, Object>> properties) {
        return RelationalVertex.writeVertex(this, idValue, label, properties, getTypeHint());
    }

    @Override
    public void bulkWriteVertex(final long vertexId, final String label,
                                final List<Map.Entry<String, Object>> properties, final Map<String, List<Long>> outEdges,
                                final Map<String, List<Long>> inEdges, final boolean cacheDisabled) {
        final int vertexTypeHint = getTypeHint();
        final Map<String, ?> vertexPropertyIds;
        final Map<String, Object> vertexPropertyValueMap;
        switch (vertexTypeHint) {
            case LinkedVertex.VERTEX_TYPE_HINT:
                vertexPropertyIds = getPropertyIdMap(this, properties, FireflyIdFactory.createId(vertexId), true);
                vertexPropertyValueMap = null;
                break;
            case StarPackedVertex.VERTEX_TYPE_HINT:
                throw new NotImplementedException("Not currently supported vertex hype hint: " + vertexTypeHint);
            case PackedVertex.VERTEX_TYPE_HINT:
                final RelationalVertex.PropertyValueIdMaps propertyValueIdMaps = getPropertyValueIdMaps(this, properties);
                vertexPropertyIds = propertyValueIdMaps.idMap;
                vertexPropertyValueMap = propertyValueIdMaps.valueMap;
                break;
            default:
                // Should never happen.
                throw new RuntimeException("Unknown vertex type hint: " + vertexTypeHint);
        }

        // Create vertex bins for vertex label, property ids, and property counter.
        final Bin labelBin = new Bin(AerospikeConnection.LABEL, Value.get(label));
        final Bin vertexPropertyIdsBin = new Bin(this.db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexPropertyIds));
        final Bin vertexPropertyCounterBin =
                new Bin(this.db.VP_COUNTER, Value.get(Long.valueOf(vertexPropertyIds.size())));
        final Bin typeHint = new Bin(this.db.RELATIONAL_VERTEX_TYPE_HINT, Value.get(vertexTypeHint));

        // Load edges.
        // TODO: There are potential data mismatches here compared to regular loading in this model for the sake of
        //       performance. The optimizations are listed as follows and will need to be addressed here and/or at the
        //       bulk loader if the assumptions or outcomes are incompatible with newer versions of this model.
        //  ASSUMPTION: Partial IN/OUT_EDGES are not used when CACHE_DISABLED is TRUE. Currently, once the
        //              cache is disabled due to the number of edges exceeding ID_CACHE_SIZE, no new edges are added to
        //              the bins and they are not used but the existing partial edges persist.
        //  OUTCOME: An empty map is recorded to the bin instead of a partial one in cases where CACHE_DISABLED is TRUE.
        //  ASSUMPTION: IN/OUT_EDGE_COUNTER is not used except to trigger disabling of the cache. Once it exceeds
        //              ID_CACHE_SIZE it serves no further purpose but continues to accurately reflect the edge count.
        //  OUTCOME: If CACHE_DISABLED is TRUE, set IN/OUT_EDGE_COUNTER to ID_CACHE_SIZE since the count is not used.

        // Create vertex bins for out edges.
        final Value outEdgeCountValue;
        if (cacheDisabled) {
            outEdgeCountValue = Value.get(db.ID_CACHE_SIZE);
        } else {
            long outEdgeCount = 0;
            for (final Map.Entry<String, List<Long>> labelToIds : outEdges.entrySet()) {
                outEdgeCount += labelToIds.getValue().size();
            }
            outEdgeCountValue = Value.get(outEdgeCount);
        }
        final Bin outEdgeDataBin = new Bin(this.db.OUT_EDGES, Value.get(outEdges));
        final Bin outEdgeCounterBin = new Bin(this.db.OUT_EDGE_COUNTER, outEdgeCountValue);

        // Create vertex bins for in edges.
        final Value inEdgeCountValue;
        if (cacheDisabled) {
            inEdgeCountValue = Value.get(db.ID_CACHE_SIZE);
        } else {
            long inEdgeCount = 0;
            for (final Map.Entry<String, List<Long>> labelToIds : inEdges.entrySet()) {
                inEdgeCount += labelToIds.getValue().size();
            }
            inEdgeCountValue = Value.get(inEdgeCount);
        }
        final Bin inEdgeDataBin = new Bin(this.db.IN_EDGES, Value.get(inEdges));
        final Bin inEdgeCounterBin = new Bin(this.db.IN_EDGE_COUNTER, inEdgeCountValue);

        // Create vertex bin for cache state.
        final Bin cacheDisabledBin = new Bin(db.CACHE_DISABLED, Value.get(cacheDisabled));

        // Write vertex bins to Aerospike.
        if (vertexPropertyValueMap != null) {
            final Bin vertexPropertyValuesBin =
                    new Bin(this.db.VERTEX_PROPERTY_NAME_TO_VALUE, Value.get(vertexPropertyValueMap));
            final Map<String, Long> vertexPropertyTypeHintMap = new HashMap<>();
            for (final Map.Entry<String, ?> entry : vertexPropertyValueMap.entrySet()) {
                vertexPropertyTypeHintMap.put(entry.getKey(), db.getSupportedType(entry.getValue().getClass()));
            }
            final Bin vertexPropertyValuesTypeHintsBin =
                    new Bin(db.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, Value.get(vertexPropertyTypeHintMap));
            FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, FireflyIdFactory.createId(vertexId), -1, labelBin,
                    vertexPropertyIdsBin, vertexPropertyValuesBin, vertexPropertyCounterBin,
                    vertexPropertyValuesTypeHintsBin, typeHint, outEdgeDataBin, outEdgeCounterBin, cacheDisabledBin,
                    inEdgeDataBin, inEdgeCounterBin);
        } else {
            FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, FireflyIdFactory.createId(vertexId), -1, labelBin,
                    vertexPropertyIdsBin, vertexPropertyCounterBin, typeHint, outEdgeDataBin, outEdgeCounterBin,
                    cacheDisabledBin, inEdgeDataBin, inEdgeCounterBin);
        }
    }

    /**
     * Function to read edge from Aerospike.
     *
     * @param edgeId Edge id.
     * @return Edge.
     */
    @Override
    public FireflyEdge readEdge(final FireflyId edgeId) {
        return RelationalEdge.readEdge(this, edgeId);
    }

    /**
     * Function to read edges from Aerospike.
     *
     * @param edgeIds Edge ids.
     * @return Edge.
     */
    @Override
    public List<FireflyEdge> readEdges(final List<FireflyId> edgeIds) {
        return RelationalEdge.readEdges(this, edgeIds);
    }

    /**
     * Function to create edge from a record.
     *
     * @param keyRecord Record to use.
     * @return Edge.
     */
    @Override
    public FireflyEdge edgeFromRecord(final KeyRecord keyRecord) {
        return RelationalEdge.fromRecord(this, keyRecord);
    }

    /**
     * Function to read vertex from Aerospike.
     *
     * @param idValue Id of vertex.
     * @return Vertex.
     */
    @Override
    public FireflyVertex readVertex(final FireflyId idValue) {
        return RelationalVertex.readVertex(this, idValue);
    }

    @Override
    public List<FireflyVertex> readVertices(final List<FireflyId> idValues) {
        return RelationalVertex.readVertices(this, idValues);
    }

    /**
     * Function to create vertex from a record.
     *
     * @param keyRecord Record to use.
     * @return Vertex.
     */
    @Override
    public FireflyVertex vertexFromRecord(final KeyRecord keyRecord) {
        return RelationalVertex.fromRecord(this, keyRecord);
    }

    /**
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET
     * remove k from the ELEMENT_PROPERTIES map
     *
     * @param element Element to remove property from
     * @param key     property key to remove
     */
    @Override
    public void removeProperty(final FireflyElement element, final String key) {
        GenerationCheck.writeGenerationCheck(() -> db.removeTypeHintedValueFromMap(
                db.getElementPropertySet(element.getClass()),
                element.id,
                db.getElementPropertySet(element.getClass()), key));
    }

    /**
     * Determine if a vertex exists.
     *
     * @param idValue vertex id to check.
     * @return true if vertex exists, false otherwise.
     */
    @Override
    public boolean vertexExists(final FireflyId idValue) {
        LOG.debug("Checking if vertex {} exists.", idValue);
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.VERTEX_AERO_SET, idValue);
        return db.exists(key);
    }

    /**
     * Determine if an edge exists.
     *
     * @param idValue edge id to check.
     * @return true if edge exists, false otherwise.
     */
    @Override
    public boolean edgeExists(final FireflyId idValue) {
        LOG.debug("Checking if edge {} exists.", idValue);
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.EDGE_AERO_SET, idValue);
        return db.exists(key);
    }

    /**
     * Return a Graph variable value by name
     *
     * @param key Graph variable key
     * @param <V> type
     * @return Graph variable value
     */
    @Override
    public <V> V readGraphVariable(final String key) {
        if (Objects.equals(key, FIREFLY_CONFIGURATION_VARIABLE_NAME)) {
            return (V) this.configuration();
        }
        return db.readTypeHintedValueFromMap(db.GRAPH_VARIABLES_SET, FireflyIdFactory.createId(GRAPH_VARIABLES_RECORD), db.GRAPH_VARIABLES_MAP, key);
    }

    /**
     * Return a Set of all the Graph variable names
     *
     * @return Set of Graph variable keys
     */
    @Override
    public Set<String> readGraphVariableKeys() {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.GRAPH_VARIABLES_SET, FireflyIdFactory.createId(GRAPH_VARIABLES_RECORD));
        if (fireflyRecord == null) return new HashSet<>();
        final Map<String, ?> m = (Map<String, ?>) fireflyRecord.record.getMap(db.GRAPH_VARIABLES_MAP);
        return m.keySet();
    }

    /**
     * Write a Graph variable
     *
     * @param key   Graph variable key
     * @param value Graph variable value to write
     * @param <V>   Graph variable value type
     */
    @Override
    public <V> void writeGraphVariable(final String key, final V value) {
        db.writeTypeHintedValueToMap(db.GRAPH_VARIABLES_SET, FireflyIdFactory.createId(GRAPH_VARIABLES_RECORD), db.GRAPH_VARIABLES_MAP, key, value);
    }

    /**
     * Remove a Graph variable
     *
     * @param key Graph variable key to remove
     */
    @Override
    public void removeGraphVariable(final String key) {
        GenerationCheck.writeGenerationCheck(() -> db.removeTypeHintedValueFromMap(
                db.GRAPH_VARIABLES_SET,
                FireflyIdFactory.createId(GRAPH_VARIABLES_RECORD),
                db.GRAPH_VARIABLES_MAP,
                key));
    }

    /**
     * Lookup Edges with a particular property value by index
     *
     * @param key   Property Key
     * @param value Property Value being searched for
     * @return an Iterator of Edges
     */
    @Override
    public Iterator<FireflyEdge> queryEdgePropertyStringMatchIndex(String key, Object value) {
        if (!String.class.isAssignableFrom(value.getClass())) {
            throw new RuntimeException(String.format("%s not a string", value.getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.STRING_E_KV_INDEX,
                Filter.contains(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (String) value));
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr ->
                readEdge(FireflyIdFactory.createId(kr.key.userKey.getObject())));
        return IteratorUtils.filter(edges, edge -> edge.property(key).value().equals(value));
    }

    /**
     * Lookup Edge by numeric match on property value
     *
     * @param key       Property key to match
     * @param predicate type of match
     * @return Iterator of FireflyEdge results
     */
    @Override
    public Iterator<FireflyEdge> queryEdgePropertyNumericMatchIndex(String key, P<?> predicate) {
        final Object value = predicate.getValue();
        Filter filter;
        if (Number.class.isAssignableFrom(value.getClass())) {
            if (Integer.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (Long.valueOf((Integer) value)));
            else if (Long.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (Long) value);
            else
                throw new RuntimeException(String.format("%s not a supported numeric match type", value.getClass()));
        } else {
            throw new RuntimeException(String.format("%s not assignable to Number", value.getClass()));
        }

        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.NUMERIC_E_KV_INDEX, filter);
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr ->
                readEdge(FireflyIdFactory.createId(kr.key.userKey.getObject())));
        return IteratorUtils.filter(edges, edge -> edge.properties(key).hasNext());
    }

    /**
     * Lookup Edge by numeric range match on property value
     *
     * @param key       Property key to match
     * @param predicate type of match (lt or gt) with value embedded
     * @return Iterator of FireflyEdge results
     */
    @Override
    public Iterator<FireflyEdge> queryEdgePropertyNumericRangeIndex(String key, P<?> predicate) {
        Filter filter;
        if (Number.class.isAssignableFrom(predicate.getValue().getClass())) {
            final long val = Long.class.isAssignableFrom(predicate.getValue().getClass()) ?
                    (long) predicate.getValue() : Long.valueOf((Integer) predicate.getValue());
            if (predicate.getBiPredicate().equals(Compare.lt))
                filter = Filter.range(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, Long.MIN_VALUE, val);
            else if (predicate.getBiPredicate().equals(Compare.gt))
                filter = Filter.range(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, val, Long.MAX_VALUE);
            else throw new RuntimeException(String.format("%s not a supported predicate", predicate));
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.NUMERIC_E_KV_INDEX, filter);
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr ->
                readEdge(FireflyIdFactory.createId(kr.key.userKey.getObject())));
        return IteratorUtils.filter(edges, edge -> edge.properties(key).hasNext());
    }

    /**
     * Lookup Vertex by string match on property value
     *
     * @param value value to match
     * @return Iterator of FireflyVertex results
     */
    @Override
    public Iterator<FireflyVertex> queryVertexLabelStringIndex(Object value) {
        final Iterator<KeyRecord> iter = db.queryIndex(db.VERTEX_AERO_SET, db.V_LABEL_INDEX,
                Filter.contains(AerospikeConnection.LABEL, IndexCollectionType.DEFAULT, (String) value));
        return IteratorUtils.map(iter, this::vertexFromRecord);
    }

    /**
     * Lookup Edge by string match on label value
     *
     * @param value label value to match
     * @return Iterator of FireflyEdge results
     */
    @Override
    public Iterator<FireflyEdge> queryEdgeLabelStringIndex(Object value) {
        final Iterator<KeyRecord> iter = db.queryIndex(db.getElementPropertySet(FireflyEdge.class), db.E_LABEL_INDEX,
                Filter.contains(AerospikeConnection.LABEL, IndexCollectionType.DEFAULT, (String) value));
        return IteratorUtils.map(iter, this::edgeFromRecord);
    }

    protected Iterator<Long> scanAllVertices() {
        //@todo performance
        LOG.trace("Scanning {} ids.", db.VERTEX_AERO_SET);
        final Iterator<Map.Entry<Key, Record>> i = db.scanAllKeysInSet(db.VERTEX_AERO_SET, null);
        return IteratorUtils.map(i, r -> (Long) FireflyIdFactory.createId(r.getKey().userKey.getObject()).getStorageId());
    }

    @Override
    public <V> Property<V> writeProperty(final FireflyElement element, final String key, final V value) {
        return RelationalProperty.writeProperty(this, element, key, value);
    }

    @Override
    public <V> Map<String, Property<V>> readProperties(final FireflyElement element) {
        return RelationalProperty.readProperties(this, element);
    }

    @Override
    public <V> Property<V> readProperty(final FireflyElement element, final String key) {
        return RelationalProperty.readProperty(this, element, key);
    }

    @Override
    public long getVertexCount() {
        return AerospikeConnection.InfoOps.getSetSize(db.VERTEX_AERO_SET, db.getNamespace(), db.getClient());
    }

    @Override
    public long getEdgeCount() {
        return AerospikeConnection.InfoOps.getSetSize(db.EDGE_AERO_SET, db.getNamespace(), db.getClient());
    }
}
