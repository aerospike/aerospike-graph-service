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

package com.aerospike.firefly.olap.helper;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ReflectionHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class AttachmentCache {
    private final Set<Object> vertexIds = new HashSet<>();
    private final Set<Object> edgeIds = new HashSet<>();
    private final Map<Object, Element> vertexCache = new HashMap<>();
    private final Map<Object, Element> edgeCache = new HashMap<>();
    private final FireflyGraph graph;

    public AttachmentCache(final FireflyGraph graph) {
        this.graph = graph;
    }

    public boolean needAttachment() {
        return !vertexIds.isEmpty() || !edgeIds.isEmpty();
    }

    public Object get(final Object object) {
        if (object instanceof Vertex) {
            return vertexCache.getOrDefault(((Vertex) object).id(), (Vertex) object);
        } else if (object instanceof Edge) {
            return edgeCache.getOrDefault(((Edge) object).id(), (Edge) object);
        } else if (object instanceof VertexProperty) {
            if (vertexCache.containsKey(((VertexProperty) object).element().id())) {
                final Vertex vertex = (Vertex) vertexCache.get(((VertexProperty) object).element().id());
                final Iterator<VertexProperty<Object>> itty = vertex.properties();
                while (itty.hasNext()) {
                    // vertex property attachment require key, so shortcut here
                    final VertexProperty vp = itty.next();
                    if (vp.id().equals(((VertexProperty<?>) object).id())) {
                        return vp;
                    }
                }
            }
            return object;
        } else if (object instanceof Property && edgeCache.containsKey(((Property) object).element().id())) {
            final Edge edge = (Edge) edgeCache.get(((Property) object).element().id());
            final Iterator<Property<Object>> itty = edge.properties();
            while (itty.hasNext()) {
                // edge property attachment require key, so shortcut here
                final Property p = itty.next();
                if (p.key().equals(((Property<?>) object).key())) {
                    return p;
                }
            }
        }

        return object;
    }

    public Path buildPath(final Path path) {
        if (path == null || path.isEmpty())
            return path;

        final Path attached = MutablePath.make();
        path.forEach((obj, labels) -> attached.extend(get(obj), labels));

        return attached;
    }

    public void attachPath(final Traverser traverser) {
        final Path attached = buildPath(traverser.path());
        if (attached != traverser.path()) {
            final Traverser.Admin t = ProjectedTraverser.tryUnwrap(traverser.asAdmin());
            ReflectionHelper.setFieldValue(t, "path", attached);
        }
    }

    public void fill() {
        if (!vertexIds.isEmpty())
            graph.vertices(vertexIds.toArray(new Object[vertexIds.size()])).forEachRemaining(vertex -> vertexCache.put(vertex.id(), vertex));

        if (!edgeIds.isEmpty())
            graph.edges(edgeIds.toArray(new Object[edgeIds.size()])).forEachRemaining(edge -> edgeCache.put(edge.id(), edge));
    }

    public void collectIds(final Object object) {
        if (object == null)
            return;

        if (object instanceof Traverser)
            collectIds((Traverser) object);
        if (object instanceof ReferenceVertex || object instanceof DetachedVertex)
            vertexIds.add(((Vertex) object).id());
        else if (object instanceof ReferenceEdge || object instanceof DetachedEdge)
            edgeIds.add(((Edge) object).id());
        else if (object instanceof ReferenceVertexProperty || object instanceof DetachedVertexProperty)
            vertexIds.add(((VertexProperty) object).element().id());
        else if (object instanceof ReferenceProperty || object instanceof DetachedProperty) {
            collectIds(((Property) object).element());
        } else if (object instanceof Path) {
            ((Path) object).forEach((obj, labels) -> collectIds(obj));
        } else if (object instanceof Map) {
            ((Map) object).forEach((k, v) -> {
                collectIds(k);
                collectIds(v);
            });
        } else if (object instanceof Collection) {
            ((Collection) object).forEach(i -> collectIds(i));
        }
    }

    private void collectIds(final Traverser traverser) {
        collectIds(traverser.get());

        if (traverser instanceof ProjectedTraverser) {
            for (final Object projection : ((ProjectedTraverser) traverser).getProjections())
                collectIds(projection);
        }

        collectIds(traverser.path());
    }
}
