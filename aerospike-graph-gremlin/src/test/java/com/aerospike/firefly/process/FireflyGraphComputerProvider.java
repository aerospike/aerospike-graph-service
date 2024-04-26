package com.aerospike.firefly.process;

import com.aerospike.firefly.process.computer.FireflyGraphComputer;
import com.aerospike.firefly.structure.FireflyGraphProvider;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Random;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@GraphProvider.Descriptor(computer = FireflyGraphComputer.class)
public class FireflyGraphComputerProvider extends FireflyGraphProvider {

    private static final Random RANDOM = TestHelper.RANDOM;

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        return graph.traversal().withStrategies(VertexProgramStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put(VertexProgramStrategy.WORKERS, RANDOM.nextInt(Runtime.getRuntime().availableProcessors()) + 1);
            put(VertexProgramStrategy.GRAPH_COMPUTER, RANDOM.nextBoolean() ?
                    GraphComputer.class.getCanonicalName() :
                    FireflyGraphComputer.class.getCanonicalName());
        }})));
    }
}
