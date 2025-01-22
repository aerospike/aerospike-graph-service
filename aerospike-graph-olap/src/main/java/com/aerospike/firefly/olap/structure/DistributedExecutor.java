package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.SingleMessenger;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DefaultTraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.ACTIVE_TRAVERSERS;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class DistributedExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedExecutor.class);
    public static final Map<String, VertexComputeKey> COMPUTE_KEY_MAP = Map.of(
            HALTED_TRAVERSERS, VertexComputeKey.of(HALTED_TRAVERSERS, false),
            ACTIVE_TRAVERSERS, VertexComputeKey.of(ACTIVE_TRAVERSERS, true)
    );

    static Dataset<Row> execute(final Dataset<Row> input,
                                final DistributedMemory memory,
                                final DistributedConfigHelper configHelper,
                                final Configuration vertexProgramConfig,
                                final Traversal<?, ?> traversal,
                                final StructType schema) {
        System.out.println("Executing");
        System.out.println("Partitions: " + input.rdd().partitions().length);
        return input.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            final TraverserGenerator traverserGenerator = traversal.asAdmin().getTraverserGenerator();
            LOGGER.info("test");
            System.out.println("Mapping partitions");
            memory.setInExecute(true);
            try (FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                final VertexProgram workerVertexProgram = VertexProgram.createVertexProgram(graph, vertexProgramConfig);
                workerVertexProgram.workerIterationStart(memory.asImmutable());
                final List<Row> output = new ArrayList<>();
                final DistributedMessenger<TraverserSet<Vertex>> messenger = new DistributedMessenger<>();
                while (iterator.hasNext()) {

                    final DistributedVertex vertex = new DistributedVertex(iterator.next(), graph);
                    //if (memory.isInitialIteration()) {
                        // TODO Should we use matrix to generate ??
                        final TraverserSet<Vertex> traverserList = new TraverserSet<>();
                        // final Traverser<Vertex> <== Create traverser and set step id so that it isnt halted.
                        traverserList.add(traverserGenerator.generate(vertex, (GraphStep<Vertex, Vertex>) traversal.asAdmin().getStartStep(), 1L));
                        final List<TraverserSet<Vertex>> traverserSetMessage = new ArrayList<>();
                        traverserSetMessage.add(traverserList);
                        messenger.setVertexAndIncomingMessages(vertex, traverserSetMessage); // TODO: Needs traverserSet.
                    //} else {
                    //}
                    //final TraverserGenerator<Vertex> traverserGenerator = DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(traverserRequirement);
                    //traverserGenerator.generate(vertex, traversal.asAdmin().getStartStep(), 1L);
                    workerVertexProgram.execute(vertex, messenger, memory);
                    output.add(createRow(vertex));

                }
                workerVertexProgram.workerIterationEnd(memory.asImmutable());
                System.out.println("Output: " + output);
                memory.setInExecute(false);
                Iterator<?> msgs = messenger.receiveMessages();
                while (msgs.hasNext()) {
                    System.out.println("!!!!!!!!!!!Vertex : " + msgs.next());
                }
                Object foo = messenger.getVerticesWithActiveTraversersIncoming();
                return output.iterator();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }, RowEncoder.apply(schema));
    }

    private static Row createRow(final DistributedVertex vertex) {
        return RowFactory.create(
                vertex.id,
                vertex.idTypeOrdinal,
                vertex.label(),
                vertex.getScalaProperties(),
                vertex.getScalaEdges(Direction.IN),
                vertex.getScalaEdges(Direction.OUT));
    }

    public static <A, B> scala.collection.mutable.Map<A, B> toScalaMap(HashMap<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala();
    }

    public static Set<VertexComputeKey> getComputeKeys() {
        return COMPUTE_KEY_MAP.values().stream().collect(Collectors.toSet());
    }

    public static Set<String> getComputeKeyStrings() {
        return COMPUTE_KEY_MAP.keySet();
    }
}
