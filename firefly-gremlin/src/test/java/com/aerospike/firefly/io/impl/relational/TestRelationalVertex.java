package com.aerospike.firefly.io.impl.relational;

import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
@Ignore("TODO GRAPH-412")
public class TestRelationalVertex extends AbstractFireflySuite {
    @Override
    protected boolean clearData() {
        return false;
    }

    @Before
    public void beforeEachTest() {
        // Use this to clear data here because we haven't solidified our deletes yet.
        graph.getBaseGraph().dropDatabase();
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
    }

    @Test
    public void scanAndIndexHaveEquivalentResultsBoth() {
        RelationalVertex aRelationalVertex = (RelationalVertex) graph.traversal().V().next();
        List<FireflyId> idsByIndex = FireflyCloseableIteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByIndex(Direction.BOTH));
        List<FireflyId> idsByScan = FireflyCloseableIteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByScan(Direction.BOTH));
        List<Long> longIdsByIndex = idsByIndex.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        List<Long> longIdsByScan = idsByScan.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        Collections.sort(longIdsByIndex);
        Collections.sort(longIdsByScan);
        assertEquals(longIdsByScan.size(), longIdsByIndex.size());
        assertEquals(longIdsByScan, longIdsByIndex);
    }

    @Test
    public void scanAndIndexHaveEquivalentResultsIN() {
        RelationalVertex aRelationalVertex = (RelationalVertex) graph.traversal().V().next();
        List<FireflyId> idsByIndex = FireflyCloseableIteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByIndex(Direction.IN));
        List<FireflyId> idsByScan = FireflyCloseableIteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByScan(Direction.IN));
        List<Long> longIdsByIndex = idsByIndex.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        List<Long> longIdsByScan = idsByScan.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        Collections.sort(longIdsByIndex);
        Collections.sort(longIdsByScan);
        assertEquals(longIdsByScan.size(), longIdsByIndex.size());
        assertEquals(longIdsByScan, longIdsByIndex);
    }

    @Test
    public void scanAndIndexHaveEquivalentResultsOUT() {
        RelationalVertex aRelationalVertex = (RelationalVertex) graph.traversal().V().next();
        List<FireflyId> idsByIndex = FireflyCloseableIteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByIndex(Direction.OUT));
        List<FireflyId> idsByScan = FireflyCloseableIteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByScan(Direction.OUT));
        List<Long> longIdsByIndex = idsByIndex.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        List<Long> longIdsByScan = idsByScan.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        Collections.sort(longIdsByIndex);
        Collections.sort(longIdsByScan);
        assertEquals(longIdsByScan.size(), longIdsByIndex.size());
        assertEquals(longIdsByScan, longIdsByIndex);
    }
}
