package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.config.DistributedConfigHelper;
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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
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
import static com.aerospike.firefly.olap.structure.DistributedElement.REF_COL;
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
        return input.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            final List<Row> output = new ArrayList<>();

            // Open graph.
            try (FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {

                // Set memory is in execute.
                memory.setInExecute(true);

                // Create VertexProgram for worker and prset iteration start.
                final VertexProgram workerVertexProgram = VertexProgram.createVertexProgram(graph, vertexProgramConfig);
                workerVertexProgram.workerIterationStart(memory.asImmutable());

                // Create distributed messenger.
                final DistributedMessenger<TraverserSet<Vertex>> messenger = new DistributedMessenger<>();

                // Loop through input rows, transform to vertices, and execute workerVertexProgram.
                final List<Vertex> incomingVertices = new ArrayList<>();
                final List<FireflyId> ffids = new ArrayList<>();
                while (iterator.hasNext()) {
                    final Row r = iterator.next();
                    if ((Boolean) r.get(r.fieldIndex(HALTED_COL))) {
                        // TODO: Maybe should assert. This would be a logic error.
                        output.add(r);
                    } else if ((Boolean) r.get(r.fieldIndex(REF_COL))) {
                        final Object id = r.get(0);
                        final DistributedElement.ID_TYPE idType = DistributedElement.ID_TYPE.values()[(int) r.get(1)];
                        final FireflyId fireflyId;
                        if (idType.equals(DistributedElement.ID_TYPE.STRING)) {
                            fireflyId = graph.getIdFactory().createVertexId(id);
                        } else if (idType.equals(DistributedElement.ID_TYPE.INTEGER)) {
                            fireflyId = graph.getIdFactory().createVertexId(Integer.parseInt((String) id));
                        } else if (idType.equals(DistributedElement.ID_TYPE.LONG)) {
                            fireflyId = graph.getIdFactory().createVertexId(Long.parseLong((String) id));
                        } else {
                            throw new IllegalArgumentException("Only string int and long ids are supported in olap");
                        }
                        ffids.add(fireflyId);
                    } else {
                        final DistributedVertex vertex = new DistributedVertex(r, graph);
                        incomingVertices.add(vertex);
                    }
                }
                if (!ffids.isEmpty()) {
                    final List<FireflyVertex> otherVertices = graph.readVertices(List.of(), ffids, null);
                    incomingVertices.addAll(otherVertices);
                }

                for (final Vertex vertex : incomingVertices) {
                    workerVertexProgram.execute(vertex, messenger, memory);
                }

                // End worker iteration.
                workerVertexProgram.workerIterationEnd(memory.asImmutable());

                // Set memory is not in execute.
                memory.setInExecute(false);

                // TODO: Is this correct for all cases ?
                final List<Tuple2<Object, TraverserSet<Vertex>>> msgs = messenger.getOutgoingMessages();
                for (Tuple2<Object, TraverserSet<Vertex>> msg : msgs) {
                    if (msg._2.element().get() instanceof ReferenceVertex) {
                        msg._2.forEach(v -> output.add(createRow((ReferenceVertex)v.get(), v.isHalted())));
                    } else if (msg._2.element().get() instanceof FireflyVertex) {
                        msg._2.forEach(v -> output.add(createRow((FireflyVertex) v.get(), v.isHalted())));
                    }
                }

                // Return results.
                return output.iterator();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }, RowEncoder.apply(schema));
    }

    private static Row createRow(final ReferenceVertex vertex, final boolean halted) {
        final DistributedElement.ID_TYPE idType;
        final Object id = vertex.id();
        if (id instanceof String) {
            idType = DistributedElement.ID_TYPE.STRING;
        } else if (id instanceof Integer) {
            idType = DistributedElement.ID_TYPE.INTEGER;
        } else {
            idType = DistributedElement.ID_TYPE.LONG;
        }
        return RowFactory.create(vertex.id().toString(), idType.ordinal(), vertex.label(), null, null, null, halted, true);
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

    private static Row createRow(final ReferenceVertex vertex) {
        return RowFactory.create();
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
