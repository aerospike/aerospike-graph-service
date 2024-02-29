package com.aerospike.firefly.io.query;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestGraphQuery  {

    private final Configuration testConfig;
    FireflyGraph graph;
    @Parameterized.Parameters
    public static Collection<Configuration> data() {
        final Path packed = Path.of("../conf/integration-test-settings-packed.properties");
        Configuration[] configs = IntStream.range(0, 4)
                .mapToObj(i -> ConfigurationHelper.loadFromFile(packed))
                .collect(Collectors.toList())
                .toArray(new Configuration[]{});

        configs[0].setProperty(ConfigurationHelper.Keys.QUERY_IMPL, ConfigurationHelper.Keys.QUERY_PAGED);
        configs[0].setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG, true);
        configs[0].setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES, "type,color");

        configs[1].setProperty(ConfigurationHelper.Keys.QUERY_IMPL, ConfigurationHelper.Keys.QUERY_LEGACY);
        configs[1].setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG, true);
        configs[1].setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES, "type,color");

        configs[2].setProperty(ConfigurationHelper.Keys.QUERY_IMPL, ConfigurationHelper.Keys.QUERY_PAGED);
        configs[2].setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG, false);

        configs[3].setProperty(ConfigurationHelper.Keys.QUERY_IMPL, ConfigurationHelper.Keys.QUERY_LEGACY);
        configs[3].setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG, false);

        return Arrays.asList(configs);
    }

    public TestGraphQuery(final Configuration testConfig) {
        this.testConfig = testConfig;
    }

    @Before
    public void setup(){
        graph = FireflyGraph.open(testConfig);
    }
    @After
    public void cleanup(){
        graph.getBaseGraph().dropDatabase(graph,true);
        graph.close();
    }


    @Test
    public void testQuery() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("this", "that").iterate();
        assertEquals(1, (long) g.V().has("color", "yellow").outE().count().next());
    }

}
