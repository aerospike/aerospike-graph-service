package com.aerospike.firefly.io.impl.standard;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.Backend;
import com.aerospike.firefly.io.AbstractBackend;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.OUT_EDGE_COUNTER;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class VertexBackend extends AbstractBackend implements Backend.Vertex {
    private static final Logger LOG = LoggerFactory.getLogger(AerospikeConnection.class);

    public VertexBackend(AerospikeConnection db) {
        super(db);
    }

    /**
     * Get a "fast count" of the number of elements in the Vertex set using Aerospike info
     *
     * @return number of Vertices
     */
    @Override
    public long getVertexCount() {
        return AerospikeConnection.InfoOps.getSetSize(db.VERTEX_AERO_SET, db.getNamespace(), db.getClient());
    }

    /**
     * Read a record from VERTEX_AERO_SET and return a constructed FireflyVertex
     *
     * @param graph    Graph handle
     * @param vertexId id of Vertex to read
     * @return Vertex to return
     */

    @Override
    public FireflyVertex readVertex(final FireflyGraph graph, final FireflyId vertexId) {
        final FireflyRecord fireflyRecord = getVertexRecord(vertexId);
        if (fireflyRecord == null) {
            return null;
        }
        return vertexFromRecord(graph, fireflyRecord);
    }

    /**
     * write a labeled Vertex record
     *
     * @param graph    handle to Graph instance
     * @param vertexId vertex id to write
     * @param label    vertex label to write
     */
    @Override
    public void writeVertex(final FireflyGraph graph, final FireflyId vertexId, final String label) {
        LOG.debug("Writing Vertex {}.", vertexId.value().toString());
        final Bin labelBin = new Bin(db.LABEL, Value.get(label));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, labelBin);
    }

    /**
     * remove a Vertex Record
     *
     * @param graph    refrence to Graph
     * @param vertexId id of Vertex to remove
     */
    @Override
    public void removeVertex(final FireflyGraph graph, final FireflyId vertexId) {
        LOG.debug("Removing Vertex {}.", vertexId.value().toString());
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.VERTEX_AERO_SET, vertexId.toNumericId());
        db.delete(key);
    }

    @Override
    public FireflyVertex vertexFromRecord(FireflyGraph graph, FireflyRecord fireflyRecord) {
        return new FireflyVertex(FireflyId.loadFromAerospike(db, FireflyVertex.class, fireflyRecord), fireflyRecord.record.getString(db.LABEL), graph);
    }


    /**
     * Write a fully qualified Vertex (includes label, id, and vertex properties).
     *
     * @param graph    handle to Graph instance
     * @param vertexId vertex id to write
     * @param label    vertex label to write
     */
    @Override
    public void writeFullyQualifiedVertex(final FireflyGraph graph, final FireflyId vertexId, final String label, List<Map.Entry<String, Object>> properties) {
        LOG.debug("Writing fully qualified Vertex {}.", vertexId.value().toString());
        final Map<String, List<Long>> vertexPropertyLabelIdMap = new HashMap<>();
        final List<Long> vertexPropertyIdCache = new ArrayList<>();
        properties.forEach(vp -> {
            FireflyId vertexPropertyId = FireflyId.createFromManager(graph, FireflyVertexProperty.class);
            final Bin vpkBin = new Bin(db.VERTEX_PROPERTY_NAME, vp.getKey());
            final Bin pviBin = new Bin(db.PARENT_VERTEX_ID, db.idToStorageType(vertexId.value()));
            db.writeTypeHintedValueToMap(db.VERTEX_PROPERTY_AERO_SET, vertexPropertyId, db.KEY_VALUE, vp.getKey(), vp.getValue(), vpkBin, pviBin);
            final List<Long> vertexPropertyIds = vertexPropertyLabelIdMap.getOrDefault(vp.getKey(), new ArrayList<>());
            vertexPropertyIds.add(NumericIdManager.convert(vertexPropertyId.value()));
            vertexPropertyLabelIdMap.put(vp.getKey(), vertexPropertyIds);

            if (vertexPropertyIdCache.size() < db.ID_CACHE_SIZE)
                vertexPropertyIdCache.add(NumericIdManager.convert(vertexPropertyId.value()));
        });

        final Bin labelBin = new Bin(db.LABEL, Value.get(label));
        final Bin vertexPropertyIdsBin = new Bin(db.VERTEX_PROPERTY_NAME_TO_ID, Value.get(vertexPropertyLabelIdMap));
        final Bin vertexPropertyCounterBin = new Bin(db.VP_COUNTER, Value.get(Long.valueOf(vertexPropertyIdCache.size())));
        FireflyRecord.writeElement(db, db.VERTEX_AERO_SET, vertexId, labelBin, vertexPropertyIdsBin, vertexPropertyCounterBin);
    }

    @Override
    public Iterator<Object> getEdgeIdsFromVertex(FireflyVertex vertex, Direction direction) {
        if (direction.equals(Direction.IN))
            return getInEdgeIdsFromVertex(vertex);
        else if (direction.equals(Direction.OUT))
            return getOutEdgeIdsFromVertex(vertex);
        else
            return IteratorUtils.concat(getInEdgeIdsFromVertex(vertex), getOutEdgeIdsFromVertex(vertex));
    }


    /**
     * Get the in-edge ids for a vertex
     * If the counter is less than the cache size, use the cache
     * else, query by scan
     *
     * @param vertex Vertex to read in edge ids from
     * @return Iterator of raw Ids
     */
    public Iterator<Object> getInEdgeIdsFromVertex(final FireflyVertex vertex) {
        LOG.debug("Getting in edge ids from Vertex {}.", vertex.id().toString());
        FireflyRecord r = getVertexRecord(vertex.id);
        long edge_count = r.record.getLong(db.IN_EDGE_COUNTER);
        boolean cacheDisabled = r.record.getBoolean(db.CACHE_DISABLED);
        if (edge_count < db.ID_CACHE_SIZE && !cacheDisabled)
            return getXXXIdsFromVertexByCache(vertex, db.IN_EDGES);
        else
            return getInEdgeIdsFromVertexByScan(vertex);
    }

    /**
     * Get the out-edge ids for a vertex
     * If the counter is less than the cache size, use the cache
     * else, query by scan
     *
     * @param vertex Vertex to read out edge ids from
     * @return Iterator of raw Ids
     */
    public Iterator<Object> getOutEdgeIdsFromVertex(final FireflyVertex vertex) {
        LOG.debug("Getting out edge ids from Vertex {}.", vertex.id().toString());
        FireflyRecord r = getVertexRecord(vertex.id);
        long edgeCount = r.record.getLong(OUT_EDGE_COUNTER);
        boolean cacheDisabled = r.record.getBoolean(db.CACHE_DISABLED);
        if (edgeCount < db.ID_CACHE_SIZE && !cacheDisabled)
            return getXXXIdsFromVertexByCache(vertex, db.OUT_EDGES);
        else
            return getOutEdgeIdsFromVertexByScan(vertex);
    }

    /**
     * Scan for and return the out direction ids associated with a vertex
     *
     * @param vertex Vertex to read out edge ids from
     * @return Iterator of raw Ids
     */
    public Iterator<Object> getOutEdgeIdsFromVertexByScan(final FireflyVertex vertex) {
        LOG.debug("Getting out edge ids from Vertex {} via scan.", vertex.id().toString());
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(Direction.OUT.name()),
                        Exp.val((Long) db.idToStorageType(vertex.id())))
        );
        return db.scanFilteredIdsInSet(db.EDGE_AERO_SET, exp);
    }

    @Override
    public Iterator<Object> getEdgeIdsFromVertexByScan(final FireflyVertex vertex, Direction direction) {
        if (direction.equals(Direction.IN))
            return getInEdgeIdsFromVertexByScan(vertex);
        else if (direction.equals(Direction.OUT))
            return getOutEdgeIdsFromVertexByScan(vertex);
        else
            return IteratorUtils.concat(getInEdgeIdsFromVertexByScan(vertex), getOutEdgeIdsFromVertexByScan(vertex));
    }

    /**
     * read an Id cache from a vertex, return it as a map of label to ids with label
     *
     * @param vertex  Vertex to read data from
     * @param mapName Bin name
     * @return Map of data
     */
    @Override
    public Map<String, List<Long>> getXXXIdsFromVertexLabelMap(final FireflyVertex vertex, String mapName) {
        LOG.debug("Getting out XXX ids from Vertex {} using label map {}.", vertex.id().toString(), mapName);
        FireflyRecord r = getVertexRecord(vertex.id);
        Map<String, List<Long>> labelIds = (Map<String, List<Long>>) r.record.getMap(mapName);
        if (labelIds == null) {
            labelIds = new HashMap<>();
        }
        return labelIds;
    }

    /**
     * read an Id cache from a vertex
     *
     * @param vertex  Vertex to read data from
     * @param mapName Bin name
     * @return Iterator of ids
     */
    @Override
    public Iterator<Object> getXXXIdsFromVertexByCache(final FireflyVertex vertex, String mapName) {
        LOG.debug("Getting out XXX ids from Vertex {} using cache {}.", vertex.id().toString(), mapName);
        return IteratorUtils.map(
                IteratorUtils.flatMap(getXXXIdsFromVertexLabelMap(vertex, mapName).entrySet().iterator(),
                        mapEntry -> mapEntry.getValue().iterator()),
                it -> it);
    }

    /**
     * Issue a scan query for all In direction edges associated with a vertex
     *
     * @param vertex vertex to read in edge ids from
     * @return Iterator of raw ids
     */
    public Iterator<Object> getInEdgeIdsFromVertexByScan(final FireflyVertex vertex) {
        LOG.trace("Getting in edge ids from Vertex by scan {}.", vertex.id().toString());
        final Expression exp = Exp.build(
                Exp.eq(
                        Exp.intBin(Direction.IN.name()),
                        Exp.val((Long) db.idToStorageType(vertex.id())))
        );
        return db.scanFilteredIdsInSet(db.EDGE_AERO_SET, exp);
    }

    /**
     * Determine of a Vertex exists
     *
     * @param vertexId Id of Vertex to check
     * @return Boolean vertex exists
     */
    @Override
    public boolean vertexExists(final FireflyId vertexId) {
        LOG.debug("Checking if vertex {} exists.", vertexId.value());
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.VERTEX_AERO_SET, vertexId.toNumericId());
        return db.exists(key);
    }

    /**
     * Get the backing FireflyRecord for a FireflyVertex by FireflyId
     *
     * @param id Id of Vertex
     * @return FireflyRecord
     */
    @Override
    public FireflyRecord getVertexRecord(FireflyId id) {
        LOG.trace("Getting vertex record v[{}].", id.value());
        return FireflyRecord.read(db, db.VERTEX_AERO_SET, id.toNumericId());
    }

}
