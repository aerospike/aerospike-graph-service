package com.aerospike.firefly.olap.structure;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.io.aerospike.query.paged.PageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.PartitionedSindexPageFetcher;
import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.process.computer.local.BatchMessenger;
import com.aerospike.firefly.process.computer.local.BatchTraversalVertexProgram;
import com.aerospike.firefly.process.computer.local.LocalMessageBoard;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.amazonaws.services.frauddetector.model.DataType;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DefaultTraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                                final Traversal<?, ?> traversal,
                                final StructType schema) {
        System.out.println("Starting with " + input.rdd().partitions().length + " partitions.");
        return input.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            logDebuggingMessage("starting with " + (iterator.hasNext() ? "non-empty" : "empty") + " partition.");

            final Codec codec;
            final TraversalMatrix<?, ?> traversalMatrix;
            final List<Row> output;
            try {
                codec = new Codec(traversal.asAdmin().getTraverserRequirements());
                traversalMatrix = new TraversalMatrix<>(traversal.asAdmin());
                output = new ArrayList<>();
            } catch (Exception e) {
                logDebuggingMessage("ERROR");
                e.printStackTrace();
                throw e;
            }

            // Open graph.
            try (FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                final Set<TraverserRequirement> traverserRequirements = traversal.asAdmin().getTraverserRequirements();
                final TraverserGenerator traverserGenerator = DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(traverserRequirements);

                // Set memory is in execute.
                memory.setInExecute(true);
                final LocalMessageBoard messageBoard = new LocalMessageBoard();

                // Create VertexProgram for worker and prset iteration start.
                final VertexProgram vertexProgram = VertexProgram.createVertexProgram(graph, vertexProgramConfig);
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

                final List<Object> batchIds = traverserSet.stream().map(t -> ((Element) t.get()).id()).collect(Collectors.toList());
                logDebuggingMessage("Input TraverserSet: " + traverserSet);
                if (!traverserSet.isEmpty()) {
                    logDebuggingMessage("Step: " + traverserSet.stream().collect(Collectors.toList()).get(0).getStepId());
                }
                workerVertexProgram.execute(
                        traverserSet,
                        messenger,
                        memory,
                        batchIds::contains);

                // End worker iteration.
                workerVertexProgram.workerIterationEnd(memory.asImmutable());

                // Set memory is not in execute.
                memory.setInExecute(false);

                // TODO: Is this correct for all cases ?
                final TraverserSet<Traverser.Admin<?>> traversers = messageBoard.getActiveTraversers();
                logDebuggingMessage("Output traverserSet size : " + traversers.size());
                if (traversers.size() > 0) {
                    logDebuggingMessage("Step: " + traversers.stream().collect(Collectors.toList()).get(0).getStepId());
                }
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
