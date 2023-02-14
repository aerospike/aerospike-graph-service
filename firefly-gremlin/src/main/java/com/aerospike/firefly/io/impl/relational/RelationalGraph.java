package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.GRAPH_VARIABLES_RECORD;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
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
        outVertex.writeEdge(Direction.OUT, getIdFactory().createCompositeEdgeId(edgeId, inVertex.id), label);
        inVertex.writeEdge(Direction.IN, getIdFactory().createCompositeEdgeId(edgeId, outVertex.id), label);

        // Write edge to Aerospike and return FireflyEdge.
        return RelationalEdge.writeEdge(this, edgeId, label, properties, inVertex, outVertex);
    }

    @Override
    public void bulkWriteEdge(final long edgeId, final String label, final List<Map.Entry<String, Object>> properties,
                              final long inVertexId, final long outVertexId) {
        LOG.debug("Writing edge {} [({})-({})->({})] {}.", edgeId, outVertexId, label, inVertexId, properties);

        final Map<String, Object> data = new TreeMap<>();
        final Map<String, Object> typeHints = new TreeMap<>();
        properties.forEach(prop -> {
            final String key = prop.getKey();
            final Object value = prop.getValue();
            FireflyHelper.validatePropertyValue(value);

            if (value == null) {
                data.remove(key);
                typeHints.remove(key);
            } else {
                typeHints.put(key, db.getSupportedType(value.getClass()));
                data.put(key, value);
            }
        });

        final Bin labelBin = new Bin(AerospikeConnection.LABEL, Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(FireflyIdPoly.fromObject(inVertexId, db.VERTEX_AERO_SET).getKeyHashBase64()));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(FireflyIdPoly.fromObject(outVertexId, db.VERTEX_AERO_SET).getKeyHashBase64()));
        final Bin valueBin = new Bin(this.db.PROPERTIES, Value.get(data, MapOrder.KEY_ORDERED));
        final Bin typeHintBin = new Bin(this.db.TYPE_HINTS, Value.get(typeHints, MapOrder.KEY_ORDERED));
        FireflyRecord.writeElement(this.db, this.db.EDGE_AERO_SET, getIdFactory().createId(edgeId, FireflyEdge.class), -1, labelBin,
                inVbin, outVBin, valueBin, typeHintBin);
    }

    /**
     * Function to bulk write edges to a vertex's edge cache
     *
     * @param vertexId  Vertex label.
     * @param direction Direction of the edges.
     * @param edgeIds   List of edge IDs.
     * @param edgeLabel Label of all edges in edge ID list.
     * @return False if the edge cache of the vertex is disabled.
     */
    public void bulkWriteEdgesToVertexCache(final FireflyId vertexId, final Direction direction,
                                            final List<Value> edgeIds, final String edgeLabel) {
        // Get the key.
        final Key key = FireflyRecord.getKey(db, this.db.VERTEX_AERO_SET, vertexId);

        // Get direction and counter keys. Direction must be IN or OUT.
        final String directionBinName = direction == Direction.IN ? this.db.IN_EDGES : this.db.OUT_EDGES;
        final String counterBinName = direction == Direction.IN ? this.db.IN_EDGE_COUNTER : this.db.OUT_EDGE_COUNTER;

        // Simple bin to increment the edge cache counter.
        final Bin incrementEdgeCountBin = new Bin(counterBinName, edgeIds.size());

        // Create the operations.
        final Operation incrementEdgeCount = Operation.add(incrementEdgeCountBin);
        final Operation getEdgeCount = Operation.get(counterBinName);
        final Operation getCacheState = Operation.get(this.db.EDGE_CACHE_DISABLED);
        final Operation appendEdgeId = ListOperation.appendItems(
                directionBinName,
                edgeIds,
                CTX.mapKeyCreate(Value.get(edgeLabel), MapOrder.KEY_ORDERED)
        );

        final FireflyCache cache = db.transactionCache.get();
        if (cache != null) {
            cache.invalidate(key);
        }

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        final Record results = this.db.operate(writePolicy, key, incrementEdgeCount, getEdgeCount,
                getCacheState, appendEdgeId);
        final boolean isCacheDisabled = results.getBoolean(this.db.EDGE_CACHE_DISABLED);
        final long edgeCount = results.getLong(counterBinName);

        if (isCacheDisabled) {
            // Cache was already disabled so wipe the write we just did to prevent memory leak.
            final Bin emptyEdgeCacheBin = new Bin(directionBinName, Value.get(new TreeMap<>(), MapOrder.KEY_ORDERED));
            final Operation wipeCache = Operation.put(emptyEdgeCacheBin);
            this.db.operate(writePolicy, key, wipeCache);
        } else if (edgeCount > this.db.ID_CACHE_SIZE) {
            // Disable the edge cache for this vertex and clear the cache.
            final Bin disabledCacheBin = new Bin(this.db.EDGE_CACHE_DISABLED, true);
            final Operation disableCache = Operation.put(disabledCacheBin);
            final Bin emptyEdgeCacheBin = new Bin(directionBinName, Value.get(new TreeMap<>(), MapOrder.KEY_ORDERED));
            final Operation wipeCache = Operation.put(emptyEdgeCacheBin);
            this.db.operate(writePolicy, key, disableCache, wipeCache);
        }
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
     * Function to remove edge record via id without reading the edge back.
     * NOTE: This function does not remove the edge from adjacent vertices. This must be done separately.
     *
     * @param edgeId Id of edge to remove.
     */
    @Override
    public void removeEdgeById(final FireflyId edgeId) {
        // Remove edge.
        LOG.debug("Removing edge {}.", edgeId);

        db.delete(FireflyRecord.getKey(db, db.EDGE_AERO_SET, edgeId));
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
     * Function to create vertex from a KeyRecord.
     *
     * @param keyRecord Record to use.
     * @return Vertex.
     */
    @Override
    public FireflyVertex vertexFromRecord(final KeyRecord keyRecord) {
        return RelationalVertex.fromRecord(this, keyRecord);
    }

    /**
     * Function to create vertex from a Key-Record Map.Entry pair.
     *
     * @param keyRecord Record to use.
     * @return Vertex.
     */
    @Override
    public FireflyVertex vertexFromRecord(final Map.Entry<Key, Record> keyRecord) {
        return RelationalVertex.fromRecord(this, new KeyRecord(keyRecord.getKey(), keyRecord.getValue()));
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
        db.removeTypeHintedValueFromMap(
                db.setFromElementType(element.getClass()),
                element.id,
                db.PROPERTIES,
                key,
                db.TYPE_HINTS);
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
        final Key key = FireflyRecord.getKey(db, db.VERTEX_AERO_SET, idValue);
        return db.exists(key);
    }

    /**
     * Determine verticies in a list exist.
     *
     * @param idValue vertex id to check.
     * @return true if vertex exists, false otherwise.
     */
    @Override
    public boolean[] vertexExists(final List<FireflyId> idValue) {
        LOG.debug("Checking if vertex {} exists.", idValue);
        return db.exists(idValue.stream().map(id -> FireflyRecord.getKey(db, db.VERTEX_AERO_SET, id)).toArray(Key[]::new));
    }

    /**
     * Determine if an edge exists.
     *
     * @param idValue edge id to check.
     * @return true if edge exists, false otherwise.
     */
    @Override
    public boolean[] edgeExists(final List<FireflyId> idValue) {
        LOG.debug("Checking if edge {} exists.", idValue);
        return db.exists(idValue.stream().map(id -> FireflyRecord.getKey(db, db.EDGE_AERO_SET, id)).toArray(Key[]::new));
    }

    @Override
    public boolean edgeExists(final FireflyId idValue) {
        LOG.debug("Checking if edge {} exists.", idValue);
        final Key key = FireflyRecord.getKey(db, db.EDGE_AERO_SET, idValue);
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
        return db.readTypeHintedValueFromMap(
                db.GRAPH_VARIABLES_SET,
                FireflyIdPoly.fromObject(GRAPH_VARIABLES_RECORD, db.GRAPH_VARIABLES_SET),
                db.GRAPH_VARIABLES_MAP,
                key,
                db.TYPE_HINTS);
    }

    /**
     * Return a Set of all the Graph variable names
     *
     * @return Set of Graph variable keys
     */
    @Override
    public Set<String> readGraphVariableKeys() {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db,
                db.GRAPH_VARIABLES_SET,
                FireflyIdPoly.fromObject(GRAPH_VARIABLES_RECORD, db.GRAPH_VARIABLES_SET));
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
        db.writeTypeHintedGraphVariable(db.GRAPH_VARIABLES_SET,
                FireflyIdPoly.fromObject(GRAPH_VARIABLES_RECORD, db.GRAPH_VARIABLES_SET),
                db.GRAPH_VARIABLES_MAP,
                key,
                value,
                db.TYPE_HINTS);
    }

    /**
     * Remove a Graph variable
     *
     * @param key Graph variable key to remove
     */
    @Override
    public void removeGraphVariable(final String key) {
        db.removeTypeHintedValueFromMap(
                db.GRAPH_VARIABLES_SET,
                FireflyIdPoly.fromObject(GRAPH_VARIABLES_RECORD, db.GRAPH_VARIABLES_SET),
                db.GRAPH_VARIABLES_MAP,
                key,
                db.TYPE_HINTS);
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
                Filter.contains(db.PROPERTIES, IndexCollectionType.MAPVALUES, (String) value));
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr ->
                readEdge(getIdFactory().createId(kr.key.userKey.getObject(), FireflyEdge.class)));
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
                filter = Filter.contains(db.PROPERTIES, IndexCollectionType.MAPVALUES, (Long.valueOf((Integer) value)));
            else if (Long.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(db.PROPERTIES, IndexCollectionType.MAPVALUES, (Long) value);
            else
                throw new RuntimeException(String.format("%s not a supported numeric match type", value.getClass()));
        } else {
            throw new RuntimeException(String.format("%s not assignable to Number", value.getClass()));
        }

        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.NUMERIC_E_KV_INDEX, filter);
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr ->
                readEdge(getIdFactory().createId(kr.key.userKey.getObject(), FireflyEdge.class)));
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
                filter = Filter.range(db.PROPERTIES, IndexCollectionType.MAPVALUES, Long.MIN_VALUE, val);
            else if (predicate.getBiPredicate().equals(Compare.gt))
                filter = Filter.range(db.PROPERTIES, IndexCollectionType.MAPVALUES, val, Long.MAX_VALUE);
            else throw new RuntimeException(String.format("%s not a supported predicate", predicate));
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.NUMERIC_E_KV_INDEX, filter);
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr ->
                readEdge(getIdFactory().createId(kr.key.userKey.getObject(), FireflyEdge.class)));
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
        final Iterator<KeyRecord> iter = db.queryIndex(db.setFromElementType(FireflyEdge.class), db.E_LABEL_INDEX,
                Filter.contains(AerospikeConnection.LABEL, IndexCollectionType.DEFAULT, (String) value));
        return IteratorUtils.map(iter, this::edgeFromRecord);
    }

    protected Iterator<FireflyId> scanAllVertices() {
        //@todo performance
        LOG.trace("Scanning {} ids.", db.VERTEX_AERO_SET);
        final Iterator<Map.Entry<Key, Record>> i = db.scanAllKeysInSet(db.VERTEX_AERO_SET, null);
        return IteratorUtils.map(i, r -> getIdFactory().createId(r.getKey().userKey.getObject(), FireflyVertex.class));
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
