package com.aerospike.firefly.olap;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDistributedGraphComputer {
    private Configuration config;

    @Before
    public void beforeEach() {
        config = ConfigurationHelper.loadFromFile(Tokens.INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
    }

    @Test
    public void testBasic() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            System.out.println("!!!!!!!!!!!!Start");
            List<Vertex> output = graph.traversal().withComputer().V().has("name", "marko").out().toList();
            System.out.println("!!!!!!!!!!!!end");
            System.out.println(output);
        }
    }

    @Test
    public void testMap() {
        SparkSession spark = SparkSession.builder()
                .appName("Java Map Schema Example")
                .master("local[*]")
                .getOrCreate();

        // Define schema
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("label", DataTypes.StringType, false)
                .add("properties", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true));

        // Create data
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        Row row = RowFactory.create("vertexId", "vertexLabel", properties);

        // Create DataFrame
        Dataset<Row> df = spark.createDataFrame(java.util.Collections.singletonList(row), schema);

        // Show DataFrame
        df.show(false);

        spark.stop();
    }
}
