package com.aerospike.firefly.process.traversal.step.computer;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeIdStepLocal extends VertexStep<Vertex> implements PrecomputableComputerStep<Vertex> {
    private final Direction direction;
    private final Set<String> edgeLabels;

    // HasContainers to apply to the read of the composite id step to filter results.
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;
    private final List<String> requiredProperties;
    final Traversal.Admin traversal;
    final Set<String> labels;
    private static final ThreadLocal<Map<FireflyId, FireflyVertex>> cache =
            ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<List<Pair<Traverser.Admin<Vertex>, FireflyVertex>>> inputCache =
            ThreadLocal.withInitial(ArrayList::new);
    private static final ThreadLocal<List<Vertex>> outputOrderCache =
            ThreadLocal.withInitial(ArrayList::new);

    public FireflyCompositeIdStepLocal(final Traversal.Admin traversal,
                                       final Direction direction,
                                       final String[] edgeLabels,
                                       final Set<String> labels,
                                       final List<HasContainer> hasContainers,
                                       final List<String> requiredProperties) {
        super(traversal, Vertex.class, direction, edgeLabels);
        this.traversal = traversal;
        this.direction = direction;
        this.edgeLabels = new HashSet<>(Arrays.asList(edgeLabels));
        this.labels = new HashSet<>(labels);
        if (hasContainers != null) {
            final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities =
                    FireflyBatchReadHelper.getHasContainersWithCardinalityOrder((FireflyGraph) traversal.getGraph().get(), Vertex.class, hasContainers);
            // TODO GRAPH-401: This is a hack to get around the fact that we cannot filter our cache with a hasContainer.
            //  To get around this we have to filter everything post read again, so all containers pushed to firefly no
            //  matter what.
            fireflyHasContainers = hasContainerWithCardinalities.stream().map(a -> a.hasContainer).collect(Collectors.toList());
            aerospikeHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(hasContainerWithCardinalities);
        } else {
            fireflyHasContainers = List.of();
            aerospikeHasContainers = List.of();
        }
        this.requiredProperties = requiredProperties;
        for (final String label : labels) {
            this.addLabel(label);
        }
    }

    public void add(final Traverser.Admin<?> tv, final Vertex v) {
        inputCache.get().add(new Pair<>() {
            @Override
            public FireflyVertex setValue(final FireflyVertex value) {
                return null;
            }

            final Traverser.Admin<Vertex> traverser = (Traverser.Admin<Vertex>) tv;
            final FireflyVertex vertex = (FireflyVertex) v;

            @Override
            public Traverser.Admin<Vertex> getLeft() {
                return traverser;
            }

            @Override
            public FireflyVertex getRight() {
                return vertex;
            }
        });
    }

    public void precompute() {
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<?>> fireflyCompositeIdStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyVertex> fireflyVertexMap = new TreeMap<>();

        for (final Pair<Traverser.Admin<Vertex>, FireflyVertex> pair : inputCache.get()) {
            // Get next input traverser and get the FireflyVertex form of it.
            final FireflyVertex vertex = pair.getRight();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // All the work for supernode scan/index/cache handling is done in the getVertexIdsFromVertex function.
            TraversalUtil.supernodeTraversalWarning(graph, this.traversal, vertex);

            FireflyBatchReadHelper.addElementsToSet(
                    fireflyIdList, uniqueIdSet, fireflyVertexMap, vertex.getVertexIdsFromVertex(direction, edgeLabels));

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyCompositeIdStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(pair.getLeft(), fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                // Drain data to output.
                FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                        fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, cache.get(), graph::readVertices, requiredProperties);
            }
        }

        // Drain data to output.
        FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, cache.get(), graph::readVertices, requiredProperties);
    }

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<Vertex> traverser) {
        if (cache.get() == null) {
            final Iterator<Vertex> vertices = traverser.get().vertices(this.direction, super.getEdgeLabels());
            return FireflyCloseableIteratorUtils.filter(vertices, v -> HasContainer.testAll(v, fireflyHasContainers));
        } else {
            final List<Vertex> output = new ArrayList<>();
            final List<FireflyId> missingIds = new ArrayList<>();
            final FireflyVertex fireflyVertex = (FireflyVertex) ((ComputerGraph.ComputerVertex) traverser.get()).getBaseVertex();
            fireflyVertex.getVertexIdsFromVertex(direction, edgeLabels).forEachRemaining(id -> {
                if (cache.get().containsKey(id)) {
                    output.add(cache.get().get(id));
                } else {
                    missingIds.add(id);
                }
            });
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            final List<FireflyVertex> vertices = graph.readVertices(aerospikeHasContainers, missingIds, requiredProperties);
            output.addAll(vertices);
            outputOrderCache.get().addAll(output);
            return FireflyCloseableIteratorUtils.filter(output.iterator(), v -> HasContainer.testAll(v, fireflyHasContainers));
        }
    }

    public List<Vertex> get() {
        System.out.println("get: " + traversal.asAdmin().toString());
        // System.out.println("outputOrderCache.get().size(): " + outputOrderCache.get().size());
        return new ArrayList<>(outputOrderCache.get());
    }

    @Override
    public void release() {
        System.out.println("release: " + traversal.asAdmin().toString());
        cache.get().clear();
        inputCache.get().clear();
        outputOrderCache.get().clear();
    }
}


/*


Execution time: 1497 milliseconds

==>Traversal Metrics
Step                                                               Count  Traversers       Time (ms)    % Dur
=============================================================================================================
FireflyGraphStep(vertex',[''AccountId_0014P00002QZ...                   1           1           0.732     0.38
FireflyCompositeIdStep(1000)                                           13          13           7.886     4.14
NotStep([PropertiesStep([~supernode]',property)])                      10          10           0.834     0.44
  PropertiesStep([~supernode]',property)                                                        0.718
NotStep([HasStep([~label.within([FirstName', Las...                     9           9           0.112     0.06
  HasStep([~label.within([FirstName', LastName', ...                                            0.025
FireflyCompositeIdStep(1000)                                         5071        5033         142.954    75.05
NotStep([HasStep([~id.eq(''AccountId_0014P00002QZ...                 5062        5024          11.736     6.16
  HasStep([~id.eq(''AccountId_0014P00002QZljmQAD$...                                            4.646
IdStep@[b]                                                           5062        5024           2.819     1.48
ProjectStep([datapoint_label', account_id]',[[Sel...                 5062        5024          16.960     8.90
  SelectOneStep(last',a',null)                                       5024        5024           2.575
  LabelStep                                                          5024        5024           2.165
  SelectOneStep(last',b',null)                                       5024        5024           2.509
FireflyCacheGCStep:FireflyCache(4c3c3406-e8d1-4...                   5062        5024           3.023     1.59
FireflyProfileStep                                                   5062        5024           3.407     1.79
  FireflyScanTime                                                                               0.000
>TOTAL                                                                  -           -         190.468        -

==>Traversal Metrics
Step                                                               Count  Traversers       Time (ms)    % Dur
=============================================================================================================
FireflyGraphStep(vertex,['AccountId_0014P00002QZ...                   54          54          11.172     1.36
FireflyBatchEdgeReadStep(1000)                                      1136        1136          40.086     4.87
NotStep([HasStep([~label.eq(STRICT)])])@[e]                         1097        1097           5.788     0.70
  HasStep([~label.eq(STRICT)])                                                                 2.353
EdgeOtherVertexStep                                                 1097        1097         649.240    78.95
NotStep([HasStep([~label.eq(Name)])])@[v]                            952         952           7.567     0.92
  HasStep([~label.eq(Name)])                                                                   2.998
ProjectStep([s_id, e_l, e_d, v_l, v_id],[[Selec...                   952         952         106.215    12.92
  SelectOneStep(last,s,null)                                         952         952           1.039
  PropertiesStep([value],value)                                      952         952          51.996
  SelectOneStep(last,e,null)                                         952         952           0.541
  LabelStep                                                          952         952           0.654
  SelectOneStep(last,e,null)                                         952         952           0.401
  PropertiesStep([orderCount],value)                                 952         952           1.061
  SelectOneStep(last,v,null)                                         952         952           0.362
  LabelStep                                                          952         952           0.508
  SelectOneStep(last,v,null)                                         952         952           0.343
  PropertiesStep([value],value)                                      952         952          39.805
FireflyCacheGCStep:FireflyCache(4b91c791-531c-4...                   952         952           1.155     0.14
FireflyProfileStep                                                   952         952           1.109     0.13
  FireflyScanTime                                                                              0.000
                                            >TOTAL                     -           -         822.335        -

Execution time: 97 milliseconds

==>Traversal Metrics
Step                                                               Count  Traversers       Time (ms)    % Dur
=============================================================================================================
FireflyGraphStep(vertex,['AccountId_0014P00002QZ...                    1           1           0.875     2.91
FireflyCompositeIdStep(1000)                                          18          13           5.511    18.31
NotStep([PropertiesStep([~supernode],property)])                      14          11           0.693     2.30
  PropertiesStep([~supernode],property)                                                        0.634
NotStep([HasStep([~label.within([FirstName, Las...                    11           8           0.064     0.21
  HasStep([~label.within([FirstName, LastName, ...                                             0.016
FireflyCompositeIdStep(1000)                                         700         411          20.047    66.59
NotStep([HasStep([~id.eq('AccountId_0014P00002QZ...                  683         403           0.827     2.75
  HasStep([~id.eq('AccountId_0014P00002QZhQIQA1$...                                            0.345
IdStep@[b]                                                           683         403           0.234     0.78
ProjectStep([datapoint_label, account_id],[[Sel...                   683         403           1.290     4.29
  SelectOneStep(last,a,null)                                         403         403           0.201
  LabelStep                                                          403         403           0.161
  SelectOneStep(last,b,null)                                         403         403           0.187
FireflyCacheGCStep:FireflyCache(82945d2e-2dd8-4...                   683         403           0.354     1.18
FireflyProfileStep                                                   683         403           0.207     0.69
  FireflyScanTime                                                                              0.000

 >TOTAL                     -           -          30.105        -
 ==>Traversal Metrics
 Step                                                               Count  Traversers       Time (ms)    % Dur
 =============================================================================================================
 FireflyGraphStep(vertex,['AccountId_0014P00002QZ...                    5           5           3.077     6.93
 FireflyBatchEdgeReadStep(1000)                                        86          86          10.153    22.87
 NotStep([HasStep([~label.eq(STRICT)])])@[e]                           77          77           0.424     0.96
   HasStep([~label.eq(STRICT)])                                                                 0.145
 EdgeOtherVertexStep                                                   77          77          23.233    52.33
 NotStep([HasStep([~label.eq(Name)])])@[v]                             67          67           0.425     0.96
   HasStep([~label.eq(Name)])                                                                   0.154
 ProjectStep([s_id, e_l, e_d, v_l, v_id],[[Selec...                    67          67           6.767    15.24
   SelectOneStep(last,s,null)                                          67          67           0.060
   PropertiesStep([value],value)                                       67          67           3.233
   SelectOneStep(last,e,null)                                          67          67           0.033
   LabelStep                                                           67          67           0.046
   SelectOneStep(last,e,null)                                          67          67           0.025
   PropertiesStep([orderCount],value)                                  67          67           0.081
   SelectOneStep(last,v,null)                                          67          67           0.023
   LabelStep                                                           67          67           0.034
   SelectOneStep(last,v,null)                                          67          67           0.023
   PropertiesStep([value],value)                                       67          67           2.587
 FireflyCacheGCStep:FireflyCache(2d8a8714-e285-4...                    67          67           0.248     0.56
 FireflyProfileStep                                                    67          67           0.064     0.15
   FireflyScanTime                                                                              0.000
                                             >TOTAL                     -           -          44.395

Execution time: 351 milliseconds

==>Traversal Metrics
Step                                                               Count  Traversers       Time (ms)    % Dur
=============================================================================================================
FireflyGraphStep(vertex,['AccountId_0014P00002QZ...                    1           1           1.272     6.36
FireflyCompositeIdStep(1000)                                          22          22           8.872    44.35
NotStep([PropertiesStep([~supernode],property)])                      22          22           1.096     5.48
  PropertiesStep([~supernode],property)                                                        1.008
NotStep([HasStep([~label.within([FirstName, Las...                     7           7           0.099     0.50
  HasStep([~label.within([FirstName, LastName, ...                                             0.033
FireflyCompositeIdStep(1000)                                          74          74           7.544    37.71
NotStep([HasStep([~id.eq('AccountId_0014P00002QZ...                   67          67           0.264     1.32
  HasStep([~id.eq('AccountId_0014P00002QZhxkQAD$...                                            0.105
IdStep@[b]                                                            67          67           0.078     0.39
ProjectStep([datapoint_label, account_id],[[Sel...                    67          67           0.466     2.33
  SelectOneStep(last,a,null)                                          67          67           0.062
  LabelStep                                                           67          67           0.091
  SelectOneStep(last,b,null)                                          67          67           0.060
FireflyCacheGCStep:FireflyCache(479ce6ff-36ae-4...                    67          67           0.234     1.17
FireflyProfileStep                                                    67          67           0.075     0.38
  FireflyScanTime                                                                              0.000
>TOTAL

Step                                                               Count  Traversers       Time (ms)    % Dur
=============================================================================================================
FireflyGraphStep(vertex,[AccountId_0014P00002QZ...                    31          31           7.922     6.36
FireflyBatchEdgeReadStep(1000)                                       537         537          13.723    11.02
NotStep([HasStep([~label.eq(STRICT)])])@[e]                          169         169           1.710     1.37
  HasStep([~label.eq(STRICT)])                                                                 0.672
EdgeOtherVertexStep                                                  169         169          80.686    64.79
NotStep([HasStep([~label.eq(Name)])])@[v]                            169         169           1.243     1.00
  HasStep([~label.eq(Name)])                                                                   0.443
ProjectStep([s_id, e_l, e_d, v_l, v_id],[[Selec...                   169         169          18.707    15.02
  SelectOneStep(last,s,null)                                         169         169           0.204
  PropertiesStep([value],value)                                      169         169           9.289
  SelectOneStep(last,e,null)                                         169         169           0.111
  LabelStep                                                          169         169           0.129
  SelectOneStep(last,e,null)                                         169         169           0.065
  PropertiesStep([orderCount],value)                                 169         169           0.231
  SelectOneStep(last,v,null)                                         169         169           0.064
  LabelStep                                                          169         169           0.086
  SelectOneStep(last,v,null)                                         169         169           0.062
  PropertiesStep([value],value)                                      169         169           6.706
FireflyCacheGCStep:FireflyCache(00756b83-03db-4...                   169         169           0.379     0.30
FireflyProfileStep                                                   169         169           0.153     0.12
  FireflyScanTime                                                                              0.000
                                            >TOTAL                     -           -         124.527        -
 */
