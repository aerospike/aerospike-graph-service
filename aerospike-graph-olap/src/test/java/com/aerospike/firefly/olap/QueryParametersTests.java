package com.aerospike.firefly.olap;

import com.aerospike.firefly.olap.config.QueryParameters;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueryParametersTests {
    private Configuration config;

    @Before
    public void beforeEach() {
        config = ConfigurationHelper.loadFromFile(Tokens.INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
    }

    @Test
    public void repartition() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            final Long output = graph.traversal()
                    .withComputer()
                    .V().out().with(QueryParameters.REPARTITION, false) // will be replaced with FireflyBatchVertexReadStepLocal
                    .out().with(QueryParameters.REPARTITION, true) // will be replaced with FireflyCountGlobalLocalStep
                    .count()
                    .next();

            assertEquals(2L, output.longValue());
        }
    }
}
