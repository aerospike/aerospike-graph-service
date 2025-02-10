package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.process.computer.local.BatchMessenger;
import com.aerospike.firefly.process.computer.local.BatchTraversalVertexProgram;
import com.aerospike.firefly.process.computer.local.LocalMessageBoard;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DefaultTraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.ACTIVE_TRAVERSERS;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedExecutor.class);

    private static void logDebuggingMessage(final String message) {
        System.out.println("Task " + TaskContext.getPartitionId() + " - " + message);
    }

    static Dataset<Row> execute(final Dataset<Row> input,
                                final DistributedMemory memory,
                                final DistributedConfigHelper configHelper,
                                final Configuration vertexProgramConfig,
                                final StructType schema) {
        System.out.println("Starting with " + input.rdd().partitions().length + " partitions.");
        return input.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            logDebuggingMessage("starting with " + (iterator.hasNext() ? "non-empty" : "empty") + " partition.");

            // Open graph.
            try (FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                final VertexProgram vertexProgram = VertexProgram.createVertexProgram(graph, vertexProgramConfig);

                final PureTraversal<?, ?> pureTraversal = ((BatchTraversalVertexProgram) vertexProgram).getTraversal().clone();
                pureTraversal.get().applyStrategies();
                final Traversal traversal = pureTraversal.get();
                final Codec codec = new Codec(traversal);
                final TraversalMatrix<?, ?> traversalMatrix = new TraversalMatrix<>(traversal.asAdmin());
                final List<Row> output = new ArrayList<>();

                final Set<TraverserRequirement> traverserRequirements = traversal.asAdmin().getTraverserRequirements();
                final TraverserGenerator traverserGenerator = DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(traverserRequirements);

                // Set memory is in execute.
                memory.setInExecute(true);
                final LocalMessageBoard messageBoard = new LocalMessageBoard();

                // Create VertexProgram for worker and prset iteration start.
                final BatchTraversalVertexProgram workerVertexProgram = vertexProgram instanceof BatchTraversalVertexProgram
                        ? (BatchTraversalVertexProgram) vertexProgram
                        : new BatchTraversalVertexProgram((TraversalVertexProgram) vertexProgram);

                workerVertexProgram.workerIterationStart(memory.asImmutable());

                // Create distributed messenger.
                final BatchMessenger messenger = new BatchMessenger<>(messageBoard, workerVertexProgram.getMessageCombiner());

                // Loop through input rows, transform to vertices, and execute workerVertexProgram.
                final TraverserSet<Object> traverserSet = new TraverserSet<>();
                while (iterator.hasNext()) {
                    final Row r = iterator.next();
                    traverserSet.add(codec.decode(r, traverserGenerator, traversalMatrix).asAdmin());
                }

                logDebuggingMessage("Input TraverserSet size: " + traverserSet.size());
                if (!traverserSet.isEmpty()) {
                    logDebuggingMessage("Step: " + new ArrayList<>(traverserSet).get(0).getStepId());
                }
                workerVertexProgram.execute(
                        traverserSet,
                        messenger,
                        memory);

                // End worker iteration.
                workerVertexProgram.workerIterationEnd(memory.asImmutable());

                // Set memory is not in execute.
                memory.setInExecute(false);

                // TODO: Is this correct for all cases ?
                final TraverserSet<Traverser.Admin<?>> traversers = messageBoard.getActiveTraversers();
                traversers.forEach(t -> output.add(codec.encode(t)));

                // Return results.
                logDebuggingMessage("ending with " + output.size() + " rows.");
                return output.iterator();
            } catch (Exception e) {
                logDebuggingMessage("ERROR");
                e.printStackTrace();
                throw e;
            }
        }, RowEncoder.apply(schema));
    }
}
