package com.aerospike.firefly.io.impl.relational;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
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
        List<Long> idsByIndex = IteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByIndex(Direction.BOTH));
        List<Long> idsByScan = IteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByScan(Direction.BOTH));
        Collections.sort(idsByIndex);
        Collections.sort(idsByScan);
        assertEquals(idsByScan.size(), idsByIndex.size());
        assertEquals(idsByScan, idsByIndex);
    }
    @Test
    public void scanAndIndexHaveEquivalentResultsIN() {
        RelationalVertex aRelationalVertex = (RelationalVertex) graph.traversal().V().next();
        List<Long> idsByIndex = IteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByIndex(Direction.IN));
        List<Long> idsByScan = IteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByScan(Direction.IN));
        Collections.sort(idsByIndex);
        Collections.sort(idsByScan);
        assertEquals(idsByScan.size(), idsByIndex.size());
        assertEquals(idsByScan, idsByIndex);
    }
    @Test
    public void scanAndIndexHaveEquivalentResultsOUT() {
        RelationalVertex aRelationalVertex = (RelationalVertex) graph.traversal().V().next();
        List<Long> idsByIndex = IteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByIndex(Direction.OUT));
        List<Long> idsByScan = IteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByScan(Direction.OUT));
        Collections.sort(idsByIndex);
        Collections.sort(idsByScan);
        assertEquals(idsByScan.size(), idsByIndex.size());
        assertEquals(idsByScan, idsByIndex);
    }
}
