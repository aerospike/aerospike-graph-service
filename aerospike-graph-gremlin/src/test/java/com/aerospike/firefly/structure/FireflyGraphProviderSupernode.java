package com.aerospike.firefly.structure;

import com.aerospike.firefly.process.FireflyTestListener;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ENABLE_FIREFLY_DROP_STRATEGY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyGraphProviderSupernode extends AbstractGraphProvider {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraphProviderSupernode.class);
    private static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");

        // Adjust here to test transition from caches to scans
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT.toLowerCase(), "2");
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName,
                                                    final Class<?> test,
                                                    final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {
        // Load config map with base config
        final HashMap<String, Object> configMap = new HashMap<>();
        config.getKeys().forEachRemaining(key -> configMap.put(key, config.get(Object.class, key)));

        // Add GRAPH_ID:graphName and GRAPH:FireflyGraph.
        configMap.put(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), graphName);
        configMap.put(Graph.GRAPH, FireflyGraph.class.getName());

        // Disable FireflyGraphDropStrategy for this test since it truncates the DB so event for vertex removal doesn't fire
        if (test.equals(EventStrategyProcessTest.class) && testMethodName.equals("shouldTriggerRemoveVertex")) {
            configMap.put(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase(), "false");
        }

        return configMap;
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) {
        // Connect to db using configuration
        Configuration conf = configuration;
        conf.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "false");
        if (graph != null) {
            final FireflyGraph fireflyGraph = (FireflyGraph) graph;
            if (fireflyGraph.closed.get()) {
                try (final FireflyGraph fireflyGraph2 = FireflyGraph.open(configuration)) {
                    fireflyGraph2.getBaseGraph().dropDatabase(fireflyGraph2, false);
                }
            } else {
                fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
                fireflyGraph.close();
            }
        } else {
            try (final FireflyGraph fireflyGraph = FireflyGraph.open(conf)) {
                fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            }
        }
    }

    @Override
    public Optional<TestListener> getTestListener() {
        return Optional.of(new FireflyTestListener(LOG));
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
