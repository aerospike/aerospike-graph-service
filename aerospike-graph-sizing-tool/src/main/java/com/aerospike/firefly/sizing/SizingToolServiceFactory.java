package com.aerospike.firefly.sizing;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.service.Service.Type.Start;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class SizingToolServiceFactory <I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {

    @Override
    public String getName() {
        return "sizing-tool";
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Set.of(Type.Start);
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        if (!isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
        }
        return this;
    }

    @Override
    public Type getType() {
        return Start;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        final Graph graph = (Graph) ctx.getTraversal().getGraph().get();
        final SizingToolMain sizingToolMain = new SizingToolMain();
        Map<String, Long> results = sizingToolMain.size(graph);
        return CloseableIterator.of(IteratorUtils.of((R) results));
    }


    @Override
    public Map<String, String> describeParams() {
        // TODO!
        return Map.of("See sizing tool documentation", "TODO.");
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
        Service.super.close();
    }
}
