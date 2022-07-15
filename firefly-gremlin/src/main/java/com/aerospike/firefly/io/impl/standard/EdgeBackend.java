package com.aerospike.firefly.io.impl.standard;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.Backend;
import com.aerospike.firefly.io.AbstractBackend;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class EdgeBackend extends AbstractBackend implements Backend.Edge {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeConnection.class);

    public EdgeBackend(AerospikeConnection db) {
        super(db);
    }

    /**
     * add an edge to a vertex
     *
     * @param vertex
     * @param edgeId
     * @param label
     * @param direction
     */
    @Override
    public void addEdgeToVertex(FireflyVertex vertex, FireflyId edgeId, String label, Direction direction) {
        LOG.debug("Adding edge {} to vertex {}.", edgeId.value(), vertex);
        final String directionKey = direction == Direction.IN ? db.IN_EDGES : db.OUT_EDGES;
        final String counterKey = direction == Direction.IN ? db.IN_EDGE_COUNTER : db.OUT_EDGE_COUNTER;

        final FireflyRecord fireflyRecord = db.vertexBackend.getVertexRecord(vertex.id);
        long edgeCounter = 0;
        boolean cacheDisabled = false;
        Map<String, List<Long>> labelEdges = new HashMap<>();
        if (fireflyRecord != null && fireflyRecord.record() != null) {
            labelEdges = (Map<String, List<Long>>) Optional.ofNullable(fireflyRecord.record().getMap(directionKey)).orElse(new HashMap<>());
            edgeCounter = fireflyRecord.record().getLong(counterKey);
            cacheDisabled = fireflyRecord.record().getBoolean(db.CACHE_DISABLED);
        }

        final List<Long> edges = labelEdges.getOrDefault(label, new ArrayList<>());
        if (edgeCounter < db.ID_CACHE_SIZE)
            edges.add(NumericIdManager.convert(edgeId.value()));
        else
            cacheDisabled = true;
        edgeCounter++;

        labelEdges.put(label, edges);
        final Bin edgeDataBin = new Bin(directionKey, Value.get(labelEdges));
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        final Bin cacheDisabledBin = new Bin(db.CACHE_DISABLED, Value.get(cacheDisabled));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertex.id, edgeDataBin, edgeCounterBin, cacheDisabledBin);
    }

    /**
     * remove an Edge from its associated Vertex
     *
     * @param graph     Graph refrence
     * @param vertex    Vertex to remove edge from
     * @param edge      Edge to remove
     * @param direction Direction of edge
     */
    @Override
    public void removeEdgeFromVertex(final FireflyGraph graph, final FireflyVertex vertex, final FireflyEdge edge, final Direction direction) {
        LOG.debug("Removing edge {} to vertex {}.", edge.id(), vertex);
        if (vertex == null)
            throw new NoSuchElementException(); //@todo transactions for edge removal
        FireflyRecord r = db.vertexBackend.getVertexRecord(vertex.id);
        final String directionKey = direction == Direction.IN ? db.IN_EDGES : db.OUT_EDGES;
        final String counterKey = direction == Direction.IN ? db.IN_EDGE_COUNTER : db.OUT_EDGE_COUNTER;
        if (r == null)
            return;
        Map<String, List<Long>> labelEdges = (Map<String, List<Long>>) r.record.getMap(directionKey);
        if (labelEdges == null) {
            labelEdges = new HashMap<>();
        }
        long edgeCounter = r.record.getLong(counterKey);
        if (edgeCounter > 0)
            edgeCounter--;
        if (edgeCounter == db.ID_CACHE_SIZE - 1) // if id set size within cache size, restore the cache
            labelEdges = db.vertexBackend.getXXXIdsFromVertexLabelMap(vertex, directionKey);
        List<Long> edges = labelEdges.getOrDefault(edge.label(), new ArrayList<>());
        edges.remove(NumericIdManager.convert(edge.id()));
        labelEdges.put(edge.label(), edges);
        final Bin edgeIdsBin = new Bin(directionKey, Value.get(labelEdges));
        final Bin edgeCounterBin = new Bin(counterKey, Value.get(edgeCounter));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertex.id, edgeIdsBin, edgeCounterBin);
    }

    /**
     * remove an edge by id
     *
     * @param graph  Graph reference
     * @param edgeId Id of edge to remove
     */
    @Override
    public void removeEdge(final FireflyGraph graph, final FireflyId edgeId) {
        LOG.debug("Removing edge {}.", edgeId.value());
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.EDGE_AERO_SET, edgeId.toNumericId());
        FireflyEdge e = readEdge(graph, edgeId);
        if (e == null) //@todo transactions for edge removal
            return;
//            throw new NoSuchElementException();
        removeEdgeFromVertex(graph, (FireflyVertex) e.inVertex(), e, Direction.IN);
        removeEdgeFromVertex(graph, (FireflyVertex) e.outVertex(), e, Direction.OUT);
        db.delete(key);
    }

    /**
     * Read an Edge by id
     *
     * @param graph  Graph handle
     * @param edgeId Edge id to read
     * @return Edge
     */
    @Override
    public FireflyEdge readEdge(final FireflyGraph graph, final FireflyId edgeId) {
        LOG.debug("Reading edge {}.", edgeId.value().toString());
        final FireflyRecord edgeRecord = FireflyRecord.read(db, db.EDGE_AERO_SET, edgeId.toNumericId());
        if (edgeRecord == null) {
            return null;
        }
        return new FireflyEdge(FireflyId.loadFromAerospike(db, FireflyEdge.class, edgeRecord),
                edgeRecord.record.getString(db.LABEL),
                FireflyId.of(FireflyVertex.class, edgeRecord.record.getLong(Direction.OUT.name())),
                FireflyId.of(FireflyVertex.class, edgeRecord.record.getLong(Direction.IN.name())),
                graph);
    }

    /**
     * Construct a FireflyEdge from a Record
     *
     * @param graph      FireflyGraph
     * @param edgeRecord FireflyRecord
     * @return FireflyEdge
     */
    @Override
    public FireflyEdge edgeFromRecord(FireflyGraph graph, FireflyRecord edgeRecord) {
        LOG.debug("Creating edge from record {}.", edgeRecord.key().userKey.toString());
        return new FireflyEdge(FireflyId.loadFromAerospike(db, FireflyEdge.class, FireflyRecord.fromRecord(db, edgeRecord.key(), edgeRecord.record())),
                edgeRecord.record().getString(db.LABEL),
                FireflyId.of(FireflyVertex.class, edgeRecord.record().getLong(Direction.OUT.name())),
                FireflyId.of(FireflyVertex.class, edgeRecord.record().getLong(Direction.IN.name())),
                graph);
    }

    /**
     * Write an edge
     * The edge will form 1 record in the EDGE_AERO_SET set
     * properties are written to the property record associated with this edge ID in the property set
     *
     * @param graph     handle to Graph
     * @param edgeId    Id of Edge to write
     * @param label     label for Edge to write
     * @param inVertex  in Vertex for new Edge
     * @param outVertex out Vertex for new Edge
     * @param keyValues Edge properties
     */
    @Override
    public void writeEdge(final FireflyGraph graph,
                          final FireflyId edgeId,
                          final String label,
                          final FireflyVertex outVertex,
                          final FireflyVertex inVertex,
                          final Object[] keyValues) {
        LOG.debug("Writing edge {} [{}-({})->{}].", edgeId.value(), outVertex.id(), label, inVertex.id());
        final Bin labelBin = new Bin(db.LABEL, Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(db.idToStorageType(inVertex.id())));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(db.idToStorageType(outVertex.id())));

        FireflyRecord.writeElement(db, db.EDGE_AERO_SET, edgeId, labelBin, inVbin, outVBin);
        addEdgeToVertex(outVertex, edgeId, label, Direction.OUT);
        addEdgeToVertex(inVertex, edgeId, label, Direction.IN);

        final Iterator<Object> propIter = IteratorUtils.asIterator(keyValues);
        while (propIter.hasNext()) {
            final Object propKey = propIter.next();
            final Object propVal = propIter.next();
            db.elementBackend.writeProperty(edgeId, FireflyEdge.class, (String) propKey, propVal);
        }
    }

    /**
     * Write a fully qualified edge including caching in IN/OUT vertices and edge properties.
     *
     * @param graph      handle to Graph
     * @param edgeId     Id of Edge to write
     * @param label      label for Edge to write
     * @param outVertex  out Vertex for new Edge
     * @param inVertex   in Vertex for new Edge
     * @param properties Edge properties
     */
    @Override
    public void writeFullyQualifiedEdge(final FireflyGraph graph,
                                        final FireflyId edgeId,
                                        final String label,
                                        final FireflyVertex outVertex,
                                        final FireflyVertex inVertex,
                                        final List<Map.Entry<String, Object>> properties) {
        LOG.debug("Writing fully qualified edge {} [({})-({})->({})] {}.", edgeId.value(), outVertex.id(), label, inVertex.id(), properties);

        addEdgeToVertex(outVertex, edgeId, label, Direction.OUT);
        addEdgeToVertex(inVertex, edgeId, label, Direction.IN);

        final Map<String, Object> data = new HashMap<>();
        final Map<String, Object> typeHints = new HashMap<>();
        properties.forEach(prop -> {
            final String key = prop.getKey();
            final Object value = prop.getValue();
            FireflyHelper.validatePropertyValue(value);

            if (value != null)
                typeHints.put(key, db.getSupportedType(value.getClass()));
            else
                typeHints.put(key, null);

            if (properties.stream().filter(p -> p.getKey().equals(key)).count() > 1) {
                typeHints.put(key, db.getSupportedType(List.class));
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
        final Bin labelBin = new Bin(db.LABEL, Value.get(label));
        final Bin inVbin = new Bin(Direction.IN.name(), Value.get(db.idToStorageType(inVertex.id())));
        final Bin outVBin = new Bin(Direction.OUT.name(), Value.get(db.idToStorageType(outVertex.id())));
        final Bin valueBin = new Bin(db.EDGE_AERO_SET, Value.get(data));
        final Bin typeHintBin = new Bin(db.TYPE_HINTS, Value.get(typeHints));
        FireflyRecord.writeElement(db, db.EDGE_AERO_SET, edgeId, labelBin, inVbin, outVBin, valueBin, typeHintBin);
    }

    /**
     * Determine if an Edge exists
     *
     * @param edgeId edge id to check
     * @return Boolean edge exists
     */
    @Override
    public boolean edgeExists(final FireflyId edgeId) {
        LOG.debug("Checking if edge {} exists.", edgeId.value());
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.EDGE_AERO_SET, edgeId.toNumericId());
        return db.exists(key);
    }

    /**
     * Get a "fast count" of the number of elements in the Edge set using Aerospike info
     *
     * @return number of Edges
     */
    @Override
    public long getEdgeCount() {
        return AerospikeConnection.InfoOps.getSetSize(db.EDGE_AERO_SET, db.getNamespace(), db.getClient());
    }
}
