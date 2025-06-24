package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.process.FireflyProgram;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.process.computer.local.LocalGraphComputerView;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.spark.sql.SparkSession;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.computer.util.MapMemory;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;

// some queries must be executed on master only
public class DistributedMasterExecutor {
    public static DefaultComputerResult execute(final FireflyProgram vertexProgram,
                                                final FireflyGraph graph,
                                                final SparkSession spark,
                                                final DistributedAerospikeConnection db,
                                                final String jobId) {
        final Traversal.Admin traversal = vertexProgram.getTraversal().clone().get();
        traversal.applyStrategies();

        final Step startStep = traversal.getStartStep();
        if (startStep instanceof Configuring) {
            ((Configuring) startStep).configure("spark", spark);
            ((Configuring) startStep).configure("jobId", jobId);
            ((Configuring) startStep).configure("db", db);
        }

        final TraverserGenerator tg = traversal.getTraverserGenerator();
        final TraverserSet result = new TraverserSet();
        traversal.forEachRemaining(t -> result.add(tg.generate(t, (Step) traversal.getSteps().get(0), 1)));

        final MapMemory memory = new MapMemory();
        memory.set(TraversalVertexProgram.HALTED_TRAVERSERS, result);

        final GraphFilter graphFilter = new GraphFilter();
        // now only GraphComputer.ResultGraph.ORIGINAL
        final GraphComputer.ResultGraph resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(vertexProgram), Optional.empty());
        // now only GraphComputer.Persist.NOTHING
        final GraphComputer.Persist persist = GraphComputerHelper.getPersistState(Optional.ofNullable(vertexProgram), Optional.empty());

        final LocalGraphComputerView view = FireflyHelper.createGraphComputerView(graph, graphFilter, vertexProgram.getVertexComputeKeys());
        final Graph resultGraph2 = view.processResultGraphPersist(resultGraph, persist);
        return new DefaultComputerResult(resultGraph2, memory);
    }
}
