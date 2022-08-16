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
    public final FireflyVertex ego;
    public final List<KeyRecord> vertexRecords;
    public final List<KeyRecord> edgeRecords;
    public final List<KeyRecord> propertyRecords;
    private final AerospikeConnection db;
    private final FireflyGraph graph;

    private EgoNetwork(final FireflyId egoId, FireflyGraph graph) {
        this.ego = graph.readVertex(egoId);
        this.graph = graph;
        this.db = graph.getBaseGraph();
        this.vertexRecords = new ArrayList<>();
        this.edgeRecords = new ArrayList<>();
        this.propertyRecords = new ArrayList<>();
    }

    private void read() {
        List<Long> outEdgeIds = IteratorUtils.list(ego.getEdgeIdsFromVertex(Direction.OUT));
        List<Long> inEdgeIds = IteratorUtils.list(ego.getEdgeIdsFromVertex(Direction.IN));

        List<Key> outEdgeKeys = outEdgeIds.stream().map(edgeId ->
                new Key(db.getNamespace(), db.EDGE_AERO_SET, edgeId)).collect(Collectors.toList());
        List<Key> inEdgeKeys = inEdgeIds.stream().map(edgeId ->
                new Key(db.getNamespace(), db.EDGE_AERO_SET, edgeId)).collect(Collectors.toList());


        Record[] outEdgeRecords = db.read(outEdgeKeys.toArray(new Key[]{}));
        IntStream.range(0, outEdgeRecords.length).forEach(i -> {
            this.edgeRecords.add(new KeyRecord(outEdgeKeys.get(i), outEdgeRecords[i]));
        });
        Record[] inEdgeRecords = db.read(inEdgeKeys.toArray(new Key[]{}));
        IntStream.range(0, inEdgeRecords.length).forEach(i -> {
            this.edgeRecords.add(new KeyRecord(inEdgeKeys.get(i), inEdgeRecords[i]));
        });

        this.vertexRecords.addAll(db.vertexRecordsFromEdgeRecords(outEdgeRecords, Direction.OUT));
        this.vertexRecords.addAll(db.vertexRecordsFromEdgeRecords(inEdgeRecords, Direction.IN));
    }

    public static EgoNetwork create(final FireflyId egoId, final FireflyGraph graph) {
        return new EgoNetwork(egoId, graph);
    }

    public List<Object> vertexNeighborhood() {
        return vertexRecords.stream().map(kr -> kr.key.userKey).collect(Collectors.toList());
    }

    public Iterator<KeyRecord> records() {
        return IteratorUtils.concat(vertexRecords.iterator(), edgeRecords.iterator(), propertyRecords.iterator());
    }
}
