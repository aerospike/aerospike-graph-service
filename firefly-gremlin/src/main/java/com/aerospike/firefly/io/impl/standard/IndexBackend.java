package com.aerospike.firefly.io.impl.standard;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AbstractBackend;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.Backend;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.KEY_VALUE;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.PARENT_VERTEX_ID;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class IndexBackend extends AbstractBackend implements Backend.Index {
    public IndexBackend(AerospikeConnection db) {
        super(db);
    }

    /**
     * Lookup Edges with a particular property value by index
     *
     * @param graph FireflyGraph
     * @param key   Property Key
     * @param value Property Value being searched for
     * @return an Iterator of Edges
     */
    @Override
    public Iterator<FireflyEdge> queryEdgePropertyStringMatchIndex(FireflyGraph graph, String key, Object value) {
        if (!String.class.isAssignableFrom(value.getClass())) {
            throw new RuntimeException(String.format("%s not a string", value.getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.STRING_E_KV_INDEX,
                Filter.contains(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (String) value));
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr -> db.edgeBackend.edgeFromRecord(graph, kr.key, kr.record));
        return IteratorUtils.filter(edges, edge -> edge.property(key).value().equals(value));
    }

    @Override
    public Iterator<FireflyEdge> queryEdgePropertyNumberMatchIndex(FireflyGraph graph, String key, P<?> predicate) {
        final Object value = predicate.getValue();
        Filter filter;
        if (Number.class.isAssignableFrom(value.getClass())) {
            if (Integer.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (Long.valueOf((Integer) value)));
            else if (Long.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (Long) value);
            else
                throw new RuntimeException(String.format("%s not a supported numeric match type", value.getClass()));
        } else {
            throw new RuntimeException(String.format("%s not assignable to Number", value.getClass()));
        }

        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.NUMERIC_E_KV_INDEX, filter);
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr -> db.edgeBackend.edgeFromRecord(graph, kr.key, kr.record));
        return IteratorUtils.filter(edges, edge -> edge.properties(key).hasNext());
    }

    @Override
    public Iterator<FireflyEdge> queryEdgePropertyNumberRangeIndex(FireflyGraph graph, String key, P<?> predicate) {
        Filter filter;
        if (Number.class.isAssignableFrom(predicate.getValue().getClass())) {
            final long val = Long.class.isAssignableFrom(predicate.getValue().getClass()) ?
                    (long) predicate.getValue() : Long.valueOf((Integer) predicate.getValue());
            if (predicate.getBiPredicate().equals(Compare.lt))
                filter = Filter.range(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, Long.MIN_VALUE, val);
            else if (predicate.getBiPredicate().equals(Compare.gt))
                filter = Filter.range(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, val, Long.MAX_VALUE);
            else throw new RuntimeException(String.format("%s not a supported predicate", predicate));
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.NUMERIC_E_KV_INDEX, filter);
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr -> db.edgeBackend.edgeFromRecord(graph, kr.key, kr.record));
        return IteratorUtils.filter(edges, edge -> edge.properties(key).hasNext());
    }


    @Override
    public Iterator<? extends Vertex> queryVertexLabelStringIndex(FireflyGraph graph, Object value) {
        final Iterator<KeyRecord> iter = db.queryIndex(db.VERTEX_AERO_SET, db.V_LABEL_INDEX, Filter.contains(db.LABEL, IndexCollectionType.DEFAULT, (String) value));
        return IteratorUtils.map(iter, kr ->
                db.vertexBackend.vertexFromRecord(graph, FireflyRecord.fromRecord(db, kr.key, kr.record)));
    }

    @Override
    public Iterator<? extends Edge> queryEdgeLabelStringIndex(FireflyGraph graph, Object value) {
        final Iterator<KeyRecord> iter = db.queryIndex(db.getElementPropertySet(FireflyEdge.class), db.E_LABEL_INDEX,
                Filter.contains(db.LABEL, IndexCollectionType.DEFAULT, (String) value));
        return IteratorUtils.map(iter, kr ->
                db.edgeBackend.edgeFromRecord(graph, kr.key, kr.record));
    }

    /**
     * Lookup VertexProperties with a particular Value by index
     *
     * @param graph FireflyGraph
     * @param key   Property Key
     * @param value Property Value being searched for
     * @return Iterator of VertexProperties
     */
    @Override
    public Iterator<FireflyVertexProperty> queryVertexPropertyStringIndex(FireflyGraph graph, String key, Object value) {
        final Iterator<KeyRecord> rsi = db.queryIndex(db.VERTEX_PROPERTY_AERO_SET, db.STRING_VP_KV_INDEX,
                Filter.contains(KEY_VALUE, IndexCollectionType.MAPVALUES, (String) value));
        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                db.vpBackend.vertexPropertyFromRecord(graph, FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyId.of(FireflyVertex.class, kr.record.getLong(db.PARENT_VERTEX_ID))));
        return IteratorUtils.filter(vps, vp -> vp.key().equals(key));
    }

    @Override
    public Iterator<FireflyVertexProperty> queryVertexPropertyNumberMatchIndex(FireflyGraph graph, String key, P<?> predicate) {
        final Object value = predicate.getValue();
        Filter filter;
        if (Number.class.isAssignableFrom(value.getClass())) {
            if (Integer.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(KEY_VALUE, IndexCollectionType.MAPVALUES, (Long.valueOf((Integer) value)));
            else if (Long.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(KEY_VALUE, IndexCollectionType.MAPVALUES, (Long) value);
            else
                throw new RuntimeException(String.format("%s not a supported numeric match type", value.getClass()));
        } else {
            throw new RuntimeException(String.format("%s not assignable to Number", value.getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.VERTEX_PROPERTY_AERO_SET, db.NUMERIC_E_KV_INDEX, filter);

        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                db.vpBackend.vertexPropertyFromRecord(graph, FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyId.of(FireflyVertex.class, kr.record.getLong(db.PARENT_VERTEX_ID))));
        return IteratorUtils.filter(vps, vp -> vp.key().equals(key));
    }

    @Override
    public Iterator<FireflyVertexProperty> queryVertexPropertyNumberRangeIndex(FireflyGraph graph, String key, P<?> predicate) {
        Filter filter;
        if (Number.class.isAssignableFrom(predicate.getValue().getClass())) {
            final long val = Long.class.isAssignableFrom(predicate.getValue().getClass()) ?
                    (long) predicate.getValue() : Long.valueOf((Integer) predicate.getValue());
            if (predicate.getBiPredicate().equals(Compare.lt))
                filter = Filter.range(KEY_VALUE, IndexCollectionType.MAPVALUES, Long.MIN_VALUE, val);
            else if (predicate.getBiPredicate().equals(Compare.gt))
                filter = Filter.range(KEY_VALUE, IndexCollectionType.MAPVALUES, val, Long.MAX_VALUE);
            else throw new RuntimeException(String.format("%s not a supported predicate", predicate));
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
        }

        Iterator<KeyRecord> rsi = db.queryIndex(db.VERTEX_PROPERTY_AERO_SET, db.NUMERIC_VP_KV_INDEX, filter);
        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                (FireflyVertexProperty) db.vpBackend.vertexPropertyFromRecord(graph, FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyId.of(FireflyVertex.class, kr.record.getLong(PARENT_VERTEX_ID))));
        return IteratorUtils.filter(vps, vp -> vp.key().equals(key));
    }

}
