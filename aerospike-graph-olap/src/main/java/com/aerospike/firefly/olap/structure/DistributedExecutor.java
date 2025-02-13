package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.olap.process.BatchJob;
import com.aerospike.firefly.olap.process.TraversalProgram;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedExecutor.class);

    static Dataset<Row> execute(Dataset<Row> input,
                                final DistributedMemory memory,
                                final DistributedConfigHelper configHelper,
                                final Configuration vertexProgramConfig,
                                final StructType schema,
                                final int workerCount) {
        System.out.println("Starting with " + input.rdd().partitions().length + " partitions.");
        if (input.rdd().partitions().length < workerCount / 2) {
            System.out.println("Repartitioning to " + workerCount + " partitions.");
            input = DistributedGraphComputer.magicSwap(input.repartition(workerCount));
        }
        System.out.println("Ending with " + input.rdd().partitions().length + " partitions.");
        return input.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            TaskLogger.logDebuggingMessage("starting with " + (iterator.hasNext() ? "non-empty" : "empty") + " partition.", LOGGER);

            // Open graph.
            try (FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                graph.logInfo = TaskLogger.instance;
                final VertexProgram vertexProgram = VertexProgram.createVertexProgram(graph, vertexProgramConfig);

                final PureTraversal<?, ?> pureTraversal = ((TraversalProgram) vertexProgram).getTraversal().clone();
                pureTraversal.get().applyStrategies();
                final Traversal traversal = pureTraversal.get();
                final Codec codec = new Codec(traversal);
                final TraversalMatrix<?, ?> traversalMatrix = new TraversalMatrix<>(traversal.asAdmin());
                final List<Row> output = new ArrayList<>();

                final Set<TraverserRequirement> traverserRequirements = traversal.asAdmin().getTraverserRequirements();
                final TraverserGenerator traverserGenerator = DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(traverserRequirements);

                // Set memory is in execute.
                memory.setInExecute(true);

                // Create VertexProgram for worker and prset iteration start.
                final TraversalProgram workerVertexProgram = vertexProgram instanceof TraversalProgram
                        ? (TraversalProgram) vertexProgram
                        : new TraversalProgram((TraversalVertexProgram) vertexProgram);

                workerVertexProgram.workerIterationStart(memory.asImmutable());

                // Loop through input rows, transform to vertices, and execute workerVertexProgram.
                final TraverserSet<Object> traverserSet = new TraverserSet<>();

                int runningTotal = 0;
                while (iterator.hasNext()) {
                    while (traverserSet.size() < 5000 && iterator.hasNext()) {
                        final Row r = iterator.next();
                        traverserSet.add(codec.decode(r, traverserGenerator, traversalMatrix).asAdmin());
                    }

                    runningTotal += traverserSet.size();
                    TaskLogger.logDebuggingMessage("Input TraverserSet size: " + traverserSet.size() + "/" + runningTotal, LOGGER);
                    if (!traverserSet.isEmpty()) {
                        TaskLogger.logDebuggingMessage("Step: " + new ArrayList<>(traverserSet).get(0).getStepId(), LOGGER);
                    }

                    final BatchJob job  = new BatchJob(traverserSet);
                    workerVertexProgram.execute(job, memory);
                    traverserSet.clear();

                    // TODO: Is this correct for all cases ?
                    final TraverserSet<Traverser.Admin> traversers = job.getResults();
                    traversers.forEach(t -> output.add(codec.encode(t)));
                    job.clear();
                }

                // End worker iteration.
                workerVertexProgram.workerIterationEnd(memory.asImmutable());

                // Set memory is not in execute.
                memory.setInExecute(false);

                // Return results.
                TaskLogger.logDebuggingMessage("ending with " + output.size() + " rows.", LOGGER);
                return output.iterator();
            } catch (Exception e) {
                TaskLogger.logDebuggingMessage("ERROR", LOGGER);
                e.printStackTrace();
                throw e;
            }
        }, RowEncoder.apply(schema));
    }
}
