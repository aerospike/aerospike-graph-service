package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.structure.FireflyGraphProvider;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Random;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@GraphProvider.Descriptor(computer = LocalGraphComputer.class)
public class LocalGraphComputerProvider extends FireflyGraphProvider {

    private static final Random RANDOM = TestHelper.RANDOM;

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        //int workers = RANDOM.nextInt(Runtime.getRuntime().availableProcessors()) + 1;
        int workers = RANDOM.nextInt(3) + 1;

        return graph.traversal().withStrategies(
                VertexProgramStrategy.build()
                        .workers(workers)                          // number of parallel threads
                        .graphComputer(RANDOM.nextBoolean() ?      // verifying semantics of api
                                GraphComputer.class :
                                LocalGraphComputer.class).create());
    }
}
