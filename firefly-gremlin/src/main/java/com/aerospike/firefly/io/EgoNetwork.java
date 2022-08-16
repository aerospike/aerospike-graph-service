package com.aerospike.firefly.io;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
class EgoNetwork {
    public final FireflyId egoId;
    public final List<KeyRecord> vertexRecords;
    public final List<KeyRecord> edgeRecords;
    public final List<KeyRecord> propertyRecords;
    private final AerospikeConnection db;
    private final FireflyGraph graph;

    private EgoNetwork(final FireflyId egoId, FireflyGraph graph) {
        this.egoId = egoId;
        this.graph = graph;
        this.db = graph.getBaseGraph();
        vertexRecords = new ArrayList<>();
        edgeRecords = new ArrayList<>();
        propertyRecords = new ArrayList<>();
    }

    private void read() {
        FireflyId id = FireflyId.of(FireflyVertex.class, egoId);
        FireflyVertex startVertex = db.vertexBackend.readVertex(graph, id);
        List<Object> outEdgeIds = IteratorUtils.list(db.vertexBackend.getEdgeIdsFromVertex(startVertex, Direction.OUT));
        List<Object> inEdgeIds = IteratorUtils.list(db.vertexBackend.getEdgeIdsFromVertex(startVertex, Direction.IN));

        List<Key> outEdgeKeys = outEdgeIds.stream().map(edgeId ->
                new Key(db.getNamespace(), db.EDGE_AERO_SET, (Long) edgeId)).collect(Collectors.toList());
        List<Key> inEdgeKeys = inEdgeIds.stream().map(edgeId ->
                new Key(db.getNamespace(), db.EDGE_AERO_SET, (Long) edgeId)).collect(Collectors.toList());
        List<KeyRecord> results = new ArrayList<>();

        Record[] outEdgeRecords = db.read(outEdgeKeys.toArray(new Key[]{}));
        IntStream.range(0, outEdgeRecords.length).forEach(i -> {
            results.add(new KeyRecord(outEdgeKeys.get(i), outEdgeRecords[i]));
        });
        Record[] inEdgeRecords = db.read(inEdgeKeys.toArray(new Key[]{}));
        IntStream.range(0, inEdgeRecords.length).forEach(i -> {
            results.add(new KeyRecord(inEdgeKeys.get(i), inEdgeRecords[i]));
        });
        results.addAll(db.vertexRecordsFromEdgeRecords(outEdgeRecords, Direction.OUT));
        results.addAll(db.vertexRecordsFromEdgeRecords(inEdgeRecords, Direction.IN));
//            db.vertexBackend.getXXXIdsFromVertexByCache()

    }

    public static EgoNetwork create(final FireflyId egoId) {
//            return new EgoNetwork(egoId, db);
        return null;
    }

    public List<Object> vertexNeighborhood() {
        return vertexRecords.stream().map(kr -> kr.key.userKey).collect(Collectors.toList());
    }

    public Iterator<KeyRecord> records() {
        return IteratorUtils.concat(vertexRecords.iterator(), edgeRecords.iterator(), propertyRecords.iterator());
    }
}
