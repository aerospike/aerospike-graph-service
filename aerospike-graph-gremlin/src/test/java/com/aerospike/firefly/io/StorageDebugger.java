package com.aerospike.firefly.io;

import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StorageDebugger extends AbstractFireflySuite {
    private Vertex insertVertex() {
        final String NAME = "name";
        final String AGE = "age";
        Map<String, Object> properties = new HashMap<>() {{
            put(NAME, "grant");
            put(AGE, 35);
        }};

        return graph.traversal()
                .addV()
                .property(NAME, properties.get(NAME))
                .property(AGE, properties.get(AGE))
                .property(T.id, 1)
                .next();
    }

    @Test
    @Ignore
    public void testDebugRecord() {
        final PackedVertex v = (PackedVertex) insertVertex();

        final Object uid = v.id.getUserId();

        assertEquals(1, uid);
        final Map<String, Object> debug = v.debugStorage();
        MapUtils.debugPrint(System.out, "debug", debug);
        final GraphTraversalSource g = graph.traversal();
        final Property<Object> x = g.V(v).properties(FireflyElement.DEBUG_STORAGE_PROPERTY).next();
        final Map<String, Object> debug2 = (Map<String, Object>) x.value();
        assertEquals(debug, debug2);
    }

    @Override
    protected boolean clearData() {
        return false;
    }

    @Test
    public void testStorageDebugOffByDefault() {
        Configuration config = graph.configuration();

        Vertex v = insertVertex();
        assertFalse(graph.traversal().V(v).properties(FireflyElement.DEBUG_STORAGE_PROPERTY.toLowerCase()).hasNext());
        Map<String,Object> configData = IteratorUtils.stream(config.getKeys())
                .map(k -> new HashMap.SimpleEntry<>(k, config.getProperty(k)))
                .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll);
        configData.put(ConfigurationHelper.Keys.STORAGE_DEBUGGER_FLAG, "true");
        Configuration config2 =  new MapConfiguration(configData);
        final FireflyGraph graph2 = FireflyGraph.open(config2);
        assertTrue(graph2.traversal().V(v).properties(FireflyElement.DEBUG_STORAGE_PROPERTY).hasNext());
    }
}
