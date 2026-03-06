package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyEdgeRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyEdgeFactory;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Iterator that builds {@link FireflyEdge} objects directly from scanned phat edge
 * {@link KeyRecord}s, avoiding a redundant second read from the database.
 */
public class FireflyPhatEdgeScanIterator implements CloseableIterator<Edge> {
    private final Iterator<KeyRecord> keyRecords;
    private final FireflyGraph graph;
    private final AerospikeConnection db;
    private Iterator<FireflyEdge> currentEdges = Collections.emptyIterator();

    public FireflyPhatEdgeScanIterator(final Iterator<KeyRecord> keyRecordIterator,
                                       final FireflyGraph graph) {
        this.keyRecords = keyRecordIterator;
        this.graph = graph;
        this.db = graph.getBaseGraph();
    }

    @Override
    public boolean hasNext() {
        while (!currentEdges.hasNext()) {
            if (!keyRecords.hasNext()) {
                return false;
            }
            materializeNextRecord();
        }
        return true;
    }

    @Override
    public Edge next() {
        if (!hasNext()) {
            throw FastNoSuchElementException.instance();
        }
        return currentEdges.next();
    }

    @Override
    public void close() {
        CloseableIterator.closeIterator(keyRecords);
    }

    private void materializeNextRecord() {
        final KeyRecord keyRecord = keyRecords.next();
        final FireflyEdgeRecord edgeRecord = new FireflyEdgeRecord(keyRecord.record, db);
        final Map<ByteBuffer, ?> edgeDataMap =
                (Map<ByteBuffer, ?>) keyRecord.record.getMap(db.getConfig().edgeDataBin);

        final List<FireflyEdge> edges = new ArrayList<>(edgeDataMap.size());
        for (final ByteBuffer edgeIdBytes : edgeDataMap.keySet()) {
            final FireflyEdgeId edgeId = db.getIdFactory().createEdgeId(edgeIdBytes);
            final FireflyEdge edge = FireflyEdgeFactory.create(edgeId, edgeRecord, graph);
            if (edge != null) {
                edges.add(edge);
            }
        }
        currentEdges = edges.iterator();
    }
}
