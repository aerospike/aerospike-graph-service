package com.aerospike.firefly.process.computer.structure;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.computer.FireflyGraphComputer;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FireflyComputerGraph extends FireflyGraph {

    // TODO: This will be the new FireflyGraphComputerView. This will ensure that FireflyGraph doesn't suffer any corrupted state issues regarding OLTP/OLAP.
    // TODO: Furthermore, FireflyComputerVertex will hide the complexities of compute properties as in-memory and disk backed vertex data will be all bundled together seemlessly.
    // TODO: Finally, this will set the state better for ResultGraph writebacks.

    protected final Map<String, VertexComputeKey> computeKeys;
    protected final Map<Element, Map<String, List<VertexProperty<?>>>> computeProperties;
    protected final Set<Object> legalVertices = new HashSet<>();
    protected final Map<Object, Set<Object>> legalEdges = new HashMap<>();
    protected final GraphFilter graphFilter;
    protected final Set<String> retainVertexProperties;

    public FireflyComputerGraph(final AerospikeConnection db, final Configuration conf, final Settings gremlinServerSettings, final GraphFilter graphFilter, final Set<VertexComputeKey> computeKeys) {
        super(db, conf, gremlinServerSettings);
        this.computeKeys = new HashMap<>();
        computeKeys.forEach(key -> this.computeKeys.put(key.getKey(), key));
        this.computeProperties = new ConcurrentHashMap<>();
        this.graphFilter = graphFilter;
        if (this.graphFilter.hasFilter()) {
            this.vertices().forEachRemaining(vertex -> {
                boolean legalVertex = false;
                if (this.graphFilter.hasVertexFilter() && this.graphFilter.legalVertex(vertex)) {
                    this.legalVertices.add(vertex.id());
                    legalVertex = true;
                }
                if ((legalVertex || !this.graphFilter.hasVertexFilter()) && this.graphFilter.hasEdgeFilter()) {
                    final Set<Object> edges = new HashSet<>();
                    this.legalEdges.put(vertex.id(), edges);
                    this.graphFilter.legalEdges(vertex).forEachRemaining(edge -> edges.add(edge.id()));
                }
            });
        }
        if (this.graphFilter.hasVertexPropertyFilter()) {
            retainVertexProperties = new HashSet<>(Arrays.asList(((PropertiesStep) graphFilter.getVertexPropertyFilter().getStartStep()).getPropertyKeys()));
        } else {
            retainVertexProperties = null;
        }
    }

    public boolean legalVertex(final Vertex vertex) {
        return !this.graphFilter.hasVertexFilter() || this.legalVertices.contains(vertex.id());
    }


    public boolean legalEdge(final Vertex vertex, final Edge edge) {
        return !this.graphFilter.hasEdgeFilter() || this.legalEdges.get(vertex.id()).contains(edge.id());
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIdsOrVertices) {
        return IteratorUtils.filter(this.vertices(List.of(), vertexIdsOrVertices), this::legalVertex);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIdsOrEdges) {
        return IteratorUtils.filter(this.edges(List.of(), edgeIdsOrEdges), edge -> this.legalEdge(edge.outVertex(), edge));
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        if (!FireflyGraphComputer.class.isAssignableFrom(graphComputerClass))
            throw new IllegalArgumentException(graphComputerClass.getSimpleName() + " is not assignable from " + FireflyGraphComputer.class.getSimpleName());
        else {
            try {
                Class<C> clazz = graphComputerClass.equals(GraphComputer.class) ? (Class<C>) FireflyGraphComputer.class : graphComputerClass;
                return clazz.getConstructor(FireflyComputerGraph.class).newInstance(this);
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return new FireflyGraphComputer(this);
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, db.toString());
    }


}
