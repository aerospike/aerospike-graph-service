package com.aerospike.firefly.io;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

/**
 * Represents a "ego" vertex, all its neighbors, and all associated records
 */
public class EgoNetwork {
    public final FireflyVertex ego;
    public final Set<KeyRecord> vertexRecords;
    public final Set<KeyRecord> edgeRecords;
    public final Set<KeyRecord> propertyRecords;
    private final AerospikeConnection db;
    private final FireflyGraph graph;

    private EgoNetwork(final FireflyId egoId, FireflyGraph graph) {
        this.ego = graph.readVertex(egoId);
        this.graph = graph;
        this.db = graph.getBaseGraph();
        this.vertexRecords = new HashSet<>();
        this.edgeRecords = new HashSet<>();
        this.propertyRecords = new HashSet<>();
    }

    private EgoNetwork read() {
        List<FireflyId> outEdgeIds = ego.getEdgeIdsFromVertex(Direction.OUT);
        List<FireflyId> inEdgeIds = ego.getEdgeIdsFromVertex(Direction.IN);

        // Almost all instances of (Long) casting to read FireflyIds has been removed, this is one of the last ones.
        List<Key> outEdgeKeys = outEdgeIds.stream().map(edgeId ->
                new Key(db.getNamespace(), edgeId.getKeyHash(), db.EDGE_AERO_SET, null)).collect(Collectors.toList());
        List<Key> inEdgeKeys = inEdgeIds.stream().map(edgeId ->
                new Key(db.getNamespace(), edgeId.getKeyHash(), db.EDGE_AERO_SET, null)).collect(Collectors.toList());


        Record[] outEdgeRecords = db.read(outEdgeKeys.toArray(new Key[]{}));
        IntStream.range(0, outEdgeRecords.length).forEach(i -> {
            KeyRecord it = new KeyRecord(outEdgeKeys.get(i), outEdgeRecords[i]);
            this.edgeRecords.add(it);
        });
        Record[] inEdgeRecords = db.read(inEdgeKeys.toArray(new Key[]{}));
        IntStream.range(0, inEdgeRecords.length).forEach(i -> {
            KeyRecord it = new KeyRecord(inEdgeKeys.get(i), inEdgeRecords[i]);
            this.edgeRecords.add(it);
        });

        this.vertexRecords.addAll(db.vertexRecordsFromEdgeRecords(outEdgeRecords, Direction.OUT));
        this.vertexRecords.addAll(db.vertexRecordsFromEdgeRecords(inEdgeRecords, Direction.IN));
        return this;
    }

    /**
     * Generate an ego network by reading data from Aerospike
     *
     * @param egoId The central vertex to start from
     * @param graph FireflyGraph instance
     * @return EgoNetwork
     */
    public static EgoNetwork create(final FireflyId egoId, final FireflyGraph graph) {
        return new EgoNetwork(egoId, graph).read();
    }

    /**
     * return all the Id's of Verticies in the ego network
     *
     * @return
     */
    public List<Object> vertexNeighborhood() {
        return vertexRecords.stream().map(kr -> kr.key.userKey).collect(Collectors.toList());
    }

    /**
     * return an Iterator of all Records in the ego network
     *
     * @return Iterator of all Records in the ego network
     */
    public Iterator<KeyRecord> records() {
        return IteratorUtils.concat(vertexRecords.iterator(), edgeRecords.iterator(), propertyRecords.iterator());
    }
}
