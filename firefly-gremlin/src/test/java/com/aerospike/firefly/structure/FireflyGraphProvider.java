package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.GraphTest;
import org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest;
import org.apache.tinkerpop.gremlin.structure.io.IoVertexTest;

import java.io.ObjectInputFilter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.Tokens.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyGraphProvider extends AbstractGraphProvider {

    private static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);

        // Adjust here to test transition from caches to scans
        config.setProperty(ConfigurationHelper.Keys.ID_CACHE_SIZE,"100000");
    }

    protected IdManager selectIdMakerFromTest(final Class<?> test, final String testMethodName) {
        if (test.equals(GraphTest.class)) {
            final Set<String> vertexTestsThatNeedLongIdManager = new HashSet<String>() {{
                add("shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentation");
                add("shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentations");
                add("shouldIterateVerticesWithNumericIdSupportUsingIntegerRepresentation");
                add("shouldIterateVerticesWithNumericIdSupportUsingIntegerRepresentations");
                add("shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentation");
                add("shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentations");
                add("shouldIterateVerticesWithNumericIdSupportUsingStringRepresentation");
                add("shouldIterateVerticesWithNumericIdSupportUsingStringRepresentations");

            }};
            final Set<String> edgeTestsThatNeedLongIdManager = new HashSet<String>() {{
                add("shouldIterateEdgesWithNumericIdSupportUsingDoubleRepresentation");
                add("shouldIterateEdgesWithNumericIdSupportUsingDoubleRepresentations");
                add("shouldIterateEdgesWithNumericIdSupportUsingIntegerRepresentation");
                add("shouldIterateEdgesWithNumericIdSupportUsingIntegerRepresentations");
                add("shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentation");
                add("shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentations");
                add("shouldIterateEdgesWithNumericIdSupportUsingStringRepresentation");
                add("shouldIterateEdgesWithNumericIdSupportUsingStringRepresentations");
            }};
            final Set<String> testsThatNeedUuidIdManager = new HashSet<String>() {{
                add("shouldIterateVerticesWithUuidIdSupportUsingStringRepresentation");
                add("shouldIterateVerticesWithUuidIdSupportUsingStringRepresentations");
                add("shouldIterateEdgesWithUuidIdSupportUsingStringRepresentation");
                add("shouldIterateEdgesWithUuidIdSupportUsingStringRepresentations");
            }};

            if (vertexTestsThatNeedLongIdManager.contains(testMethodName))
                return new NumericIdManager<FireflyVertex>(FireflyVertex.class, VERTEX_ID_COUNTER);
            else if (edgeTestsThatNeedLongIdManager.contains(testMethodName))
                return new NumericIdManager<FireflyEdge>(FireflyEdge.class, EDGE_ID_COUNTER);
            else if (testsThatNeedUuidIdManager.contains(testMethodName))
                throw new UnsupportedOperationException(UNIMPLEMENTED);
        } else if (test.equals(IoEdgeTest.class)) {
            final Set<String> edgeTestsThatNeedLongIdManager = new HashSet<String>() {{
                add("shouldReadWriteEdge[graphson-v1]");
                add("shouldReadWriteDetachedEdgeAsReference[graphson-v1]");
                add("shouldReadWriteDetachedEdge[graphson-v1]");
                add("shouldReadWriteEdge[graphson-v2]");
                add("shouldReadWriteDetachedEdgeAsReference[graphson-v2]");
                add("shouldReadWriteDetachedEdge[graphson-v2]");
            }};

            if (edgeTestsThatNeedLongIdManager.contains(testMethodName))
                return new NumericIdManager<FireflyEdge>(FireflyEdge.class, EDGE_ID_COUNTER);
        } else if (test.equals(IoVertexTest.class)) {
            final Set<String> vertexTestsThatNeedLongIdManager = new HashSet<String>() {{
                add("shouldReadWriteVertexWithBOTHEdges[graphson-v1]");
                add("shouldReadWriteVertexWithINEdges[graphson-v1]");
                add("shouldReadWriteVertexWithOUTEdges[graphson-v1]");
                add("shouldReadWriteVertexNoEdges[graphson-v1]");
                add("shouldReadWriteDetachedVertexNoEdges[graphson-v1]");
                add("shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v1]");
                add("shouldReadWriteVertexMultiPropsNoEdges[graphson-v1]");
                add("shouldReadWriteVertexWithBOTHEdges[graphson-v2]");
                add("shouldReadWriteVertexWithINEdges[graphson-v2]");
                add("shouldReadWriteVertexWithOUTEdges[graphson-v2]");
                add("shouldReadWriteVertexNoEdges[graphson-v2]");
                add("shouldReadWriteDetachedVertexNoEdges[graphson-v2]");
                add("shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v2]");
                add("shouldReadWriteVertexMultiPropsNoEdges[graphson-v2]");
            }};

            if (vertexTestsThatNeedLongIdManager.contains(testMethodName))
                return new NumericIdManager(FireflyVertex.class, VERTEX_ID_COUNTER);
        }
        //@todo review this default
        return new NumericIdManager(FireflyVertex.class, VERTEX_ID_COUNTER);

//        return FireflyGraph.DefaultIdManager.ANY;
    }


    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        HashMap<String, Object> configMap = new HashMap<String, Object>();
        config.getKeys().forEachRemaining(key -> {
            configMap.put(key, config.get(Object.class, key));
        });
        configMap.put(Graph.GRAPH, FireflyGraph.class.getName());
        return configMap;
    }

    @Override
    public void clear(Graph graph, Configuration configuration) {
        AerospikeConnection db = AerospikeConnection.connect(config);
        db.dropDatabase();
        db.close();
    }

    @Override
    public Set<Class> getImplementations() {
        return new HashSet<>() {{
            add(FireflyGraph.class);
            add(FireflyGraphVariables.class);
            add(FireflyEdge.class);
            add(FireflyVertex.class);
            add(FireflyVertexProperty.class);
        }};
    }
}
