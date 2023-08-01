package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.bulkloader.exception.FireflyLoadingException;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.ReadContext;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.AerospikeConnection.getSupportedType;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.structure.FireflyVertex.SUPERNODE_KEY;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public abstract class RelationalGraph extends FireflyGraph {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalGraph.class);

    /**
     * Constructor for RelationalGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public RelationalGraph(AerospikeConnection db, final Configuration conf, final Settings gremlinServerSettings) {
        super(db, conf, gremlinServerSettings);
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
        final boolean inVertexCacheWrite = inVertex.writeEdge(Direction.IN, getIdFactory().createCompositeEdgeId(edgeId, outVertex.id), label);
        final boolean outVertexCacheWrite = outVertex.writeEdge(Direction.OUT, getIdFactory().createCompositeEdgeId(edgeId, inVertex.id), label);

        // Write edge to Aerospike and return FireflyEdge.
        return RelationalEdge.writeEdge(this, edgeId, label, properties, inVertex, outVertex, inVertexCacheWrite, outVertexCacheWrite);
    }

    @Override
    public void bulkWriteEdge(final byte[] edgeId, final String label, final List<Map.Entry<String, Object>> properties,
                              final Object inVertexId, final Object outVertexId, final boolean inVSupernode,
                              final boolean outVSupernode) {
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
                typeHints.put(key, getSupportedType(value));
                data.put(key, value);
            }
        });

        final List<Operation> operations = new ArrayList<>();
        // CREATE and UPDATE are both okay since this is idempotent.
        final MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        final Operation writeLabel = MapOperation.put(mapPolicy,db.LABEL_BIN,
                Value.get(edgeId), Value.get(label));
        operations.add(writeLabel);

        final Operation writeInV = MapOperation.put(mapPolicy, Direction.IN.name(),
                Value.get(edgeId), Value.get(FireflyIdPoly.fromObject(inVertexId, db.VERTEX_AERO_SET).getKeyHashBase64()));
        operations.add(writeInV);
        final Operation writeOutV = MapOperation.put(mapPolicy, Direction.OUT.name(),
                Value.get(edgeId), Value.get(FireflyIdPoly.fromObject(outVertexId, db.VERTEX_AERO_SET).getKeyHashBase64()));
        operations.add(writeOutV);

        // Write to supernodes bin if vertex cache overflowed.
        if (inVSupernode) {
            final Operation writeInVSupernode = MapOperation.put(mapPolicy, db.SUPERNODES_IN_BIN,
                    Value.get(edgeId), Value.get(FireflyIdPoly.fromObject(inVertexId, db.VERTEX_AERO_SET).getKeyHashBase64()));
            operations.add(writeInVSupernode);
        }
        if (outVSupernode) {
            final Operation writeOutVSupernode = MapOperation.put(mapPolicy, db.SUPERNODES_OUT_BIN,
                    Value.get(edgeId), Value.get(FireflyIdPoly.fromObject(outVertexId, db.VERTEX_AERO_SET).getKeyHashBase64()));
            operations.add(writeOutVSupernode);
        }

        final Operation writeProperties = MapOperation.put(mapPolicy, db.PROPERTIES_BIN,
                Value.get(edgeId), Value.get(data, MapOrder.KEY_ORDERED));
        operations.add(writeProperties);
        final Operation writeTypeHints = MapOperation.put(mapPolicy, db.TYPE_HINTS_BIN,
                Value.get(edgeId), Value.get(typeHints, MapOrder.KEY_ORDERED));
        operations.add(writeTypeHints);

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        writePolicy.maxRetries = db.AEROSPIKE_WRITE_MAX_RETRY;
        final Key key = getKey(db, db.EDGE_AERO_SET, getIdFactory().createId(edgeId, FireflyEdge.class));
        try {
            db.operate(writePolicy, key, operations.toArray(new Operation[0]));
        } catch (final AerospikeException ae) {
            throw new FireflyLoadingException(ae);
        }
        fireflySummaryUpdater.addEdgeWriteToQueue(label, properties.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
    }

    /**
     * Function to bulk write edges to a vertex's edge cache
     *
     * @param vertexId  Vertex label.
     * @param direction Direction of the edges.
     * @param edgeIds   List of edge IDs.
     * @param edgeLabel Label of all edges in edge ID list.
     */
    public void bulkWriteEdgesToVertexCache(final FireflyId vertexId, final Direction direction,
                                            final List<Value> edgeIds, final String edgeLabel) {
        // Get the key.
        final Key key = FireflyRecord.getKey(db, this.db.VERTEX_AERO_SET, vertexId);

        // Get direction and counter keys. Direction must be IN or OUT.
        final String directionBinName = direction == Direction.IN ? this.db.IN_EDGES_BIN : this.db.OUT_EDGES_BIN;
        final String counterBinName = direction == Direction.IN ? this.db.IN_EDGE_COUNTER_BIN : this.db.OUT_EDGE_COUNTER_BIN;

        // Simple bin to increment the edge cache counter.
        final Bin incrementEdgeCountBin = new Bin(counterBinName, edgeIds.size());

        // Create the operations.
        final Operation incrementEdgeCount = Operation.add(incrementEdgeCountBin);
        final ListPolicy preventDuplicates = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL | ListWriteFlags.PARTIAL);
        final Operation appendEdgeId = ListOperation.appendItems(
                preventDuplicates,
                directionBinName,
                edgeIds,
                CTX.mapKeyCreate(Value.get(edgeLabel), MapOrder.KEY_ORDERED)
        );

        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        try {
            this.db.operate(writePolicy, key, incrementEdgeCount, appendEdgeId);
        } catch (final AerospikeException ae) {
            throw new FireflyLoadingException(ae);
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
        // If the supernode property flag is set on the vertex write, remove it from the write steam and assign it.
        final Map.Entry supernodeFlag = properties.stream().filter(e -> e.getKey().equals(SUPERNODE_KEY)).findFirst().orElse(null);
        if (entry != null) {
            properties.remove(entry);
        }
        final boolean isEdgeCacheOverflowed = !this.db.GLOBAL_EDGE_CACHE_ENABLED_FLAG ||
                this.db.ON_RECORD_ID_LIMIT <= 0 || supernodeFlag != null;
        return RelationalVertex.writeVertex(this, idValue, label, properties, getTypeHint(), true, isEdgeCacheOverflowed);
    }

    @Override
    public void bulkWriteVertex(final FireflyId idValue,
                                   final String label,
                                   final List<Map.Entry<String, Object>> properties,
                                   final boolean supernode) {
        try {
            // We do not use ~supernode flag to allow forcing a vertex to a supernode when bulk loading since it impacts our
            // bulk loader flow and also we already have to check for this regardless inside the bulk loader.
            RelationalVertex.writeVertex(this, idValue, label, properties, getTypeHint(), false, supernode);
        } catch (final AerospikeException ae) {
            throw new FireflyLoadingException(ae);
        }
    }

    /**
     * Function to read edges from Aerospike.
     *
     * @param edgeIds Edge ids.
     * @return Edge.
     */
    @Override
    public List<FireflyEdge> readEdges(final List<HasContainer> hasContainers, final List<FireflyId> edgeIds) {
        if (!hasContainers.isEmpty()) {
            throw new RuntimeException("Pushdown is not currently supported for Edges.");
        }
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

        RelationalEdge.removeEdgeById(this, edgeId);
    }

    /**
     * Function to read vertex from Aerospike.
     *
     * @param idValue Id of vertex.
     * @return Vertex.
     */
    @Override
    public FireflyVertex readVertex(final FireflyId idValue) {
        final List<FireflyVertex> vertices = readVertices(List.of(), List.of(idValue));
        if (vertices.isEmpty()) {
            return null;
        } else {
            return vertices.get(0);
        }
    }

    @Override
    public List<FireflyVertex> readVertices(final List<HasContainer> hasContainers, final List<FireflyId> idValues) {
        return RelationalVertex.readVertices(this, hasContainers, idValues);
    }

    /**
     * Function to create vertex from a KeyRecord.
     *
     * @param keyRecord KeyRecord to use.
     * @return Vertex.
     */
    @Override
    public FireflyVertex vertexFromRecord(final KeyRecord keyRecord) {
        return RelationalVertex.fromRecord(this, keyRecord);
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
                FireflyIdPoly.fromObject(db.GRAPH_VARIABLES_REC_KEY, db.GRAPH_VARIABLES_SET),
                db.GRAPH_VARIABLES_BIN,
                key,
                db.TYPE_HINTS_BIN);
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
                FireflyIdPoly.fromObject(db.GRAPH_VARIABLES_REC_KEY, db.GRAPH_VARIABLES_SET));
        if (fireflyRecord == null) return new HashSet<>();
        final Map<String, ?> m = (Map<String, ?>) fireflyRecord.record().getMap(db.GRAPH_VARIABLES_BIN);
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
                FireflyIdPoly.fromObject(db.GRAPH_VARIABLES_REC_KEY, db.GRAPH_VARIABLES_SET),
                db.GRAPH_VARIABLES_BIN,
                key,
                value,
                db.TYPE_HINTS_BIN);
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
                FireflyIdPoly.fromObject(db.GRAPH_VARIABLES_REC_KEY, db.GRAPH_VARIABLES_SET),
                db.GRAPH_VARIABLES_BIN,
                key,
                db.TYPE_HINTS_BIN);
    }

    protected Iterator<FireflyId> scanAllVertices() {
        LOG.trace("Scanning {} ids.", db.VERTEX_AERO_SET);
        final Iterator<KeyRecord> i = db.scanAllKeysInSet(ReadContext.create(db.VERTEX_AERO_SET), null);
        return FireflyCloseableIteratorUtils.map(i, r -> getIdFactory().createId(r.key.userKey.getObject(), FireflyVertex.class));
    }

    @Override
    public long getVertexCount(final Expression expression) {
        return FireflyCloseableIteratorUtils.count(db.scanAllKeysInSet(ReadContext.create(db.VERTEX_AERO_SET), expression, false));
    }

    @Override
    public long getEdgeCount() {
        return FireflyCloseableIteratorUtils.count(this.db.readElementIds(FireflyEdge.class));
    }
}
