package com.aerospike.firefly.io.impl.relational;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestRelationalVertex {
    static private final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);

    static {
        // TODO: Is this test even valuable now? Scans no longer work properly on Edge records that are adjacency
        //       indexed and an adjacency index query only returns overflow Edges that couldn't fit in the Edge caches.
        //       All we can really test now is a scan versus a non-overflowing Edge cache for equality.
        CONFIG.setProperty(ConfigurationHelper.Keys.ADJACENCY_INDEX_ENABLED_FLAG.toLowerCase(), true);
        CONFIG.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT.toLowerCase(), 10000);
    }

    static private final FireflyGraph GRAPH = FireflyGraph.open(CONFIG);

    @AfterClass
    static public void afterAll() {
        GRAPH.getBaseGraph().clearNamespace();
        GRAPH.close();
    }

    @Before
    public void beforeEach() {
        GRAPH.getBaseGraph().dropDatabase(GRAPH, false);
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), GRAPH);
    }

    @Test
    public void scanAndCacheHaveEquivalentResultsBoth() {
        RelationalVertex aRelationalVertex = (RelationalVertex) GRAPH.traversal().V().next();
        List<FireflyId> idsByIndex = aRelationalVertex.getEdgeIdsFromVertex(Direction.BOTH);
        List<FireflyId> idsByScan = FireflyCloseableIteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByScan(Direction.BOTH));
        List<Long> longIdsByIndex = idsByIndex.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        List<Long> longIdsByScan = idsByScan.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        Collections.sort(longIdsByIndex);
        Collections.sort(longIdsByScan);
        assertEquals(longIdsByScan.size(), longIdsByIndex.size());
        assertEquals(longIdsByScan, longIdsByIndex);
    }

    @Test
    public void scanAndCacheHaveEquivalentResultsIN() {
        RelationalVertex aRelationalVertex = (RelationalVertex) GRAPH.traversal().V().next();
        List<FireflyId> idsByIndex = aRelationalVertex.getEdgeIdsFromVertex(Direction.IN);
        List<FireflyId> idsByScan = FireflyCloseableIteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByScan(Direction.IN));
        List<Long> longIdsByIndex = idsByIndex.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        List<Long> longIdsByScan = idsByScan.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        Collections.sort(longIdsByIndex);
        Collections.sort(longIdsByScan);
        assertEquals(longIdsByScan.size(), longIdsByIndex.size());
        assertEquals(longIdsByScan, longIdsByIndex);
    }

    @Test
    public void scanAndCacheHaveEquivalentResultsOUT() {
        RelationalVertex aRelationalVertex = (RelationalVertex) GRAPH.traversal().V().next();
        List<FireflyId> idsByIndex = aRelationalVertex.getEdgeIdsFromVertex(Direction.OUT);
        List<FireflyId> idsByScan = FireflyCloseableIteratorUtils.list(aRelationalVertex.getEdgeIdsFromVertexByScan(Direction.OUT));
        List<Long> longIdsByIndex = idsByIndex.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        List<Long> longIdsByScan = idsByScan.stream().map(id -> (Long) id.getStorageId()).collect(Collectors.toList());
        Collections.sort(longIdsByIndex);
        Collections.sort(longIdsByScan);
        assertEquals(longIdsByScan.size(), longIdsByIndex.size());
        assertEquals(longIdsByScan, longIdsByIndex);
    }
}
