package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.process.computer.local.BatchMessenger;
import com.aerospike.firefly.process.computer.local.BatchTraversalVertexProgram;
import com.aerospike.firefly.process.computer.local.LocalMessageBoard;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DefaultTraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.olap.structure.DistributedElement.HALTED_COL;
import static com.aerospike.firefly.olap.structure.DistributedElement.LABEL_COL;
import static com.aerospike.firefly.olap.structure.DistributedElement.REF_COL;
import static com.aerospike.firefly.olap.structure.DistributedElement.STEP_COL;
import static com.aerospike.firefly.olap.structure.DistributedGraphComputer.getIdType;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.ACTIVE_TRAVERSERS;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
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
        System.out.println("Partitions: " + input.rdd().partitions().length);
        input.show(false);
        return input.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            final TraversalMatrix<?, ?> traversalMatrix = new TraversalMatrix<>(traversal.asAdmin());
            final List<Row> output = new ArrayList<>();

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
                    System.out.println("!!!! It: " + r.fieldIndex(HALTED_COL));
                    if ((Boolean) r.get(r.fieldIndex(HALTED_COL))) {
                        // TODO: Maybe should assert. This would be a logic error.
                        output.add(r);
                    } else {
                        final Object id = r.get(0);
                        final DistributedElement.ID_TYPE idType = DistributedElement.ID_TYPE.values()[(int) r.get(1)];
                        final ReferenceVertex vertex;
                        if (idType.equals(DistributedElement.ID_TYPE.STRING)) {
                            vertex = new ReferenceVertex(id, (String) r.get(r.fieldIndex(LABEL_COL)));
                        } else if (idType.equals(DistributedElement.ID_TYPE.INTEGER)) {
                            vertex = new ReferenceVertex(Integer.parseInt((String) id), (String) r.get(r.fieldIndex(LABEL_COL)));
                        } else if (idType.equals(DistributedElement.ID_TYPE.LONG)) {
                            vertex = new ReferenceVertex(Long.parseLong((String) id), (String) r.get(r.fieldIndex(LABEL_COL)));
                        } else {
                            throw new IllegalArgumentException("Only string int and long ids are supported in olap");
                        }
                        final String stepId = (String) r.get(r.fieldIndex(STEP_COL));
                        final Traverser traverser = traverserGenerator.generate(vertex, traversalMatrix.getStepById(stepId), 1L);
                        traverser.asAdmin().setStepId(stepId);
                        traverserSet.add(traverser.asAdmin());
                    }
                }

                final List<Object> batchIds = traverserSet.stream().map(t -> ((Element) t.get()).id()).collect(Collectors.toList());
                System.out.println("!!! Input traverserSet size : " + traverserSet.size());
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
                for (Traverser.Admin<?> traverser : traversers) {
                    if (traverser.isHalted()) {
                        System.out.println("!!! HALTED !!!");
                    }
                    if (traverser.get() instanceof FireflyVertex) {
                        output.add(createRow(new ReferenceVertex((FireflyVertex)traverser.get()), traverser.getStepId(), traverser.isHalted()));
                    } else if (traverser.get() instanceof ReferenceVertex) {
                        output.add(createRow((ReferenceVertex) traverser.get(), traverser.getStepId(), traverser.isHalted()));
                    } else {
                        throw new RuntimeException("Error " + traverser.get().getClass().getName() + " Not supported to convert to row.");
                    }
                }

                // Return results.
                System.out.println("Returning " + output);
                if (output.isEmpty()) {
                    System.out.println("empty");
                }
                return output.iterator();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }, RowEncoder.apply(schema));
    }

    private static Row createRow(final ReferenceVertex vertex, final String stepId, final boolean halted) {
        final DistributedElement.ID_TYPE idType;
        final Object id = vertex.id();
        if (id instanceof String) {
            idType = DistributedElement.ID_TYPE.STRING;
        } else if (id instanceof Integer) {
            idType = DistributedElement.ID_TYPE.INTEGER;
        } else {
            idType = DistributedElement.ID_TYPE.LONG;
        }
        return RowFactory.create(vertex.id().toString(), idType.ordinal(), vertex.label(), null, null, null, halted, true, stepId);
    }

    private Row createRow(final DistributedVertex vertex) {
        return RowFactory.create(
                vertex.id,
                vertex.idTypeOrdinal,
                vertex.label(),
                vertex.getScalaProperties(),
                vertex.getScalaEdges(Direction.IN),
                vertex.getScalaEdges(Direction.OUT));
    }


    public static Row createRow(final FireflyVertex vertex, Boolean halted) {
        //final TraverserGenerator generator = traversal.asAdmin().getTraverserGenerator();
        // TODO: Make a better format, stringifying these is going to be slow.
        scala.collection.mutable.Map<String, String> properties = JavaConverters.mapAsScalaMap(vertex.getRawVertexStringPropertyValues());
        final Map<String, List<byte[]>> inEdgesJava = vertex.getCachedIdMap(Direction.IN);
        final Map<String, Seq<byte[]>> inEdgesScala = new HashMap<>();
        for (final String label : inEdgesJava.keySet()) {
            inEdgesScala.put(label, JavaConverters.asScalaBuffer(inEdgesJava.get(label)));
        }
        final Map<String, List<byte[]>> outEdgesJava = vertex.getCachedIdMap(Direction.OUT);
        final Map<String, Seq<byte[]>> outEdgesScala = new HashMap<>();
        for (final String label : outEdgesJava.keySet()) {
            outEdgesScala.put(label, JavaConverters.asScalaBuffer(outEdgesJava.get(label)));
        }
        scala.collection.mutable.Map<String, Seq<byte[]>> inEdges = JavaConverters.mapAsScalaMap(inEdgesScala);
        scala.collection.mutable.Map<String, Seq<byte[]>> outEdges = JavaConverters.mapAsScalaMap(outEdgesScala);

        return RowFactory.create(
                vertex.id().toString(),
                getIdType(vertex.id()).ordinal(),
                vertex.label(),
                properties,
                inEdges,
                outEdges,
                halted);
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
