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
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class DistributedExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedExecutor.class);
    static Dataset<Row> execute(final Dataset<Row> input,
                                final DistributedMemory memory,
                                final DistributedConfigHelper configHelper,
                                final Configuration vertexProgramConfig,
                                final StructType schema) {
        System.out.println("Executing");
        System.out.println("Partitions: " + input.rdd().partitions().length);
        return input.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            LOGGER.info("test");
            System.out.println("Mapping partitions");
            try (FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                final VertexProgram workerVertexProgram = VertexProgram.createVertexProgram(graph, vertexProgramConfig);
                workerVertexProgram.workerIterationStart(memory.asImmutable());
                List<Row> output = new ArrayList<>();
                workerVertexProgram.setup(memory);
                while (iterator.hasNext()) {
                    final DistributedVertex vertex = new DistributedVertex(iterator.next());
                    output.add(createRow(vertex));
                }
                workerVertexProgram.workerIterationEnd(memory.asImmutable());
                System.out.println("Output: " + output);
                return output.iterator();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }, RowEncoder.apply(schema));
    }

    private static Row createRow(final DistributedVertex vertex) {
        return RowFactory.create(
                vertex.id().toString(),
                vertex.label(),
                toScalaMap(new HashMap<String, String>()));
                //new HashMap<>(),
                //new HashMap<>(),
                //new HashMap<>());
    }

    public static <A, B> scala.collection.mutable.Map<A, B> toScalaMap(HashMap<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala();
    }
}
