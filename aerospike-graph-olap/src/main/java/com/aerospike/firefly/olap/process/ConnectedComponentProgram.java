/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.codec.ConnectedComponentCodec;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.olap.structure.MutableDetachedVertexProperty;
import com.aerospike.firefly.process.computer.VertexProgramConfig;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.aerospike.firefly.process.computer.VertexProgramConfig.TRAVERSAL_VERTEX_PROGRAM_STEP;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

public class ConnectedComponentProgram extends AlgorithmProgram {

    private static final String PROPERTY = "gremlin.connectedComponentVertexProgram.property";
    // it's not mistake, same in TinkerPop
    private static final String EDGE_TRAVERSAL = "gremlin.pageRankVertexProgram.edgeTraversal";
    private static final String SAVE_RESULTS = "gremlin.connectedComponentVertexProgram.saveResults";
    private static final String WORK_SET_SIZE = "gremlin.connectedComponentVertexProgram.workSetSize";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            MemoryComputeKey.of(HALTED_TRAVERSERS, Operator.addAll, false, false),
            MemoryComputeKey.of(START_STEP, Operator.assign, true, false)));

    private Configuration configuration;
    private int workSetSize;

    // for serialization
    private ConnectedComponentProgram() {
    }

    public ConnectedComponentProgram(final VertexProgramConfig program, final FireflyGraph graph) {
        final BaseConfiguration configuration = new BaseConfiguration();
        program.storeState(configuration);
        loadState(graph, configuration);
    }

    private void init(final FireflyGraph graph) {
        this.optionsStrategy = OptionsStrategy.create(configuration);

        this.columnName = ConnectedComponentCodec.COMPONENT_COL;
        this.graph = graph;
        setTraversal();
        this.codec = new ConnectedComponentCodec(this.graphTraversal.get(), this.property);
        this.db = new DistributedAerospikeConnection(graph, true);

        this.workSetSize = configuration.getInt(WORK_SET_SIZE, 10000);
    }

    // iterations:
    // 1 - initialize vertices, write initial component to db
    // 2 - create groups
    // 3 - merge groups and save results as vertex property

    @Override
    public void execute(final BatchJob job, final Memory memory) {

        if (1 == memory.getIteration()) {
            job.getStarts().forEach(traverser -> {
                final DetachedVertex vertex = buildDetached((Vertex) traverser.get());
                traverser.set(vertex);

                this.db.writeVertex(vertex.id().toString());
            });

            job.pass();
            return;
        }

        if (3 == memory.getIteration()) {
            // 5000 records per BatchJob, so no more than 5000 groups
            final Map<String, String> recordIds = new HashMap<>();

            final List<Object> vertexIds = job.getStarts().stream()
                    .map(traverser -> ((DetachedVertex) traverser.get()).id()).collect(Collectors.toList());
            final Map<Object, String> recordIdCache = db.readVertexGroupRecordId(vertexIds);

            job.getStarts().forEach(traverser -> {
                final DetachedVertex vertex = (DetachedVertex) traverser.get();
                final String recordId = recordIdCache.get(vertex.id());

                // not in cache yet
                if (!recordIds.containsKey(recordId)) {
                    // groupId and list of connected records
                    final Pair<String, List<String>> pair = db.readGroup(recordId);
                    if (pair.getValue1() == null || pair.getValue1().isEmpty()) {
                        recordIds.put(recordId, pair.getValue0());
                    } else {
                        // read all groups from cluster
                        final Pair<String, Set<String>> treeGroup = solveGroupTree(recordId, pair.getValue0(), pair.getValue1());
                        treeGroup.getValue1().forEach(rId -> recordIds.put(rId, treeGroup.getValue0()));
                    }
                }

                final MutableDetachedVertexProperty p = (MutableDetachedVertexProperty) vertex.property(property);
                p.setValue(recordIds.get(recordId));
            });

            if ((Boolean) optionsStrategy.getOptions().getOrDefault(SAVE_RESULTS, true)) {
                saveResultAsProperty(job);
            } else {
                job.pass();
            }
            return;
        }

        // iteration 2
        job.getStarts().forEach(traverser -> {
            final DetachedVertex vertex = (DetachedVertex) traverser.get();
            makeGroup(vertex);
        });

        job.pass();
    }

    // result is groupId and Set of recordIds
    private Pair<String, Set<String>> solveGroupTree(final String recordId, final String groupId, final List<String> connected) {
        // no more than worker count
        final Set<String> visited = new HashSet<>();
        visited.add(recordId);
        final Set<String> groupIds = new HashSet<>();
        groupIds.add(groupId);

        Set<String> connectedSet = new HashSet<>(connected);
        while (!connectedSet.isEmpty()) {
            final Set<String> next = new HashSet<>();
            visited.addAll(connectedSet);

            db.readGroups(connectedSet).forEach(pair -> {
                groupIds.add(pair.getValue0());
                if (pair.getValue1() != null) {
                    next.addAll(pair.getValue1());
                }
            });
            next.removeAll(visited);
            connectedSet = next;
        }

        final String mergedGroupId = groupIds.stream().min(String::compareTo).orElseThrow();

        // possible to run this several times concurrently, but who cares?
        db.finalizeGroups(visited, mergedGroupId);

        return Pair.with(mergedGroupId, visited);
    }

    private void makeGroup(final DetachedVertex start) {
        final String vertexId = start.id().toString();
        String recordId = UUID.randomUUID().toString();

        // is visited by other worker?
        final Pair<ConnectedComponentProgram.VertexStatus, String> status = this.db.visitVertex(vertexId, recordId, VertexStatus.COMPLETED);
        if (status.getValue0() == VertexStatus.COMPLETED) {
            return;
        } else if (status.getValue0() == VertexStatus.UNVISITED) {
            // need to create new group only if vertex is not visited
            this.db.createGroup(recordId, vertexId);
        } else {
            recordId = status.getValue1();
        }

        final Set<String> neighbourRecords = new HashSet<>();

        final Set<FireflyId> visited = new HashSet<>();
        Set<FireflyVertex> current = new HashSet<>();

        final FireflyVertex startVertex = (FireflyVertex) graph.vertices(vertexId).next();
        current.add(startVertex);
        visited.add(startVertex.id);

        while (!current.isEmpty()) {
            final Set<FireflyVertex> nextVertices = new HashSet<>();
            final Set<Vertex> neighbours = new HashSet<>();
            // vertex is completed when all neighbours are visited
            final List<String> completedVertexIds = new ArrayList<>();
            // It would be better to read each vertex only once, but then there is the issue of memory usage.
            final VertexStep edgeFilterStep = getEdgeTraversalStep();
            final Set<String> edgeLabels = edgeFilterStep.getEdgeLabels().length == 0
                    ? Collections.emptySet()
                    : new HashSet<>(Arrays.asList(edgeFilterStep.getEdgeLabels()));

            for (final FireflyVertex v : current) {
                // handle vertices one by one to limit memory usage
                final Iterator<Vertex> itty = v.getVerticesFromVertex(edgeFilterStep.getDirection(), edgeLabels);
                while (itty.hasNext()) {
                    final FireflyVertex a = (FireflyVertex) itty.next();
                    if (!visited.contains(a.id) && graph.graphComputerView.getGraphFilter().legalVertex(a)) {
                        neighbours.add(a);
                        // add only legal vertices to visited
                        visited.add(a.id);
                    }
                }
                completedVertexIds.add(v.id().toString());
                if (visited.size() >= workSetSize) {
                    break;
                }
            }

            for (final Vertex v : neighbours) {
                final String id = v.id().toString();
                // todo: consider bulk visit
                final String vRecordId = this.db.visitVertex(id, recordId, VertexStatus.VISITED).getValue1();
                if (vRecordId != null) {
                    neighbourRecords.add(vRecordId);
                } else {
                    nextVertices.add((FireflyVertex) v);
                }
            }

            this.db.markCompleted(completedVertexIds);

            if (visited.size() >= workSetSize) {
                break;
            }

            // only use not visited
            current = nextVertices;
        }

        this.db.linkGroups(neighbourRecords, recordId);
    }

    @Override
    protected DetachedVertex buildDetached(final Vertex vertex) {
        if (vertex instanceof DetachedVertex)
            return (DetachedVertex) vertex;

        final MutableDetachedVertexProperty componentProperty = new MutableDetachedVertexProperty(null, property, vertex.id().toString(), null);

        return new DetachedVertex(vertex.id(), "", List.of(componentProperty));
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() >= 3;
    }

    @Override
    public void setup(final Memory memory) {
    }

    @Override
    public void loadState(final Graph graph, final Configuration config) {
        configuration = new BaseConfiguration();
        if (config != null) {
            ConfigurationUtils.copy(config, configuration);
        }

        if (configuration.containsKey(EDGE_TRAVERSAL)) {
            this.edgeTraversal = PureTraversal.loadState(configuration, EDGE_TRAVERSAL, graph);
        }
        this.vertexProgramTraversal = PureTraversal.loadState(configuration, TRAVERSAL_VERTEX_PROGRAM_STEP, graph);

        property = configuration.getString(PROPERTY, ConnectedComponentVertexProgram.COMPONENT);

        init((FireflyGraph) graph);
    }

    @Override
    public void storeState(final Configuration config) {
        if (configuration != null) {
            ConfigurationUtils.copy(configuration, config);
        }
        super.storeState(config);
        this.edgeTraversal.storeState(configuration, EDGE_TRAVERSAL);
        if (null != this.vertexProgramTraversal)
            this.vertexProgramTraversal.storeState(config, TRAVERSAL_VERTEX_PROGRAM_STEP);
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    public enum VertexStatus {
        UNVISITED,
        VISITED,
        COMPLETED
    }
}
