package com.aerospike.firefly.process.computer.local;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalMessageBoard<M> {

    public Map<MessageScope, Map<Vertex, Queue<M>>> sendMessages = new ConcurrentHashMap<>();
    public Map<MessageScope, Map<Vertex, Queue<M>>> receiveMessages = new ConcurrentHashMap<>();
    public Set<MessageScope> previousMessageScopes = new HashSet<>();
    public Set<MessageScope> currentMessageScopes = new HashSet<>();

    public void completeIteration() {
        this.receiveMessages = this.sendMessages;
        this.sendMessages = new ConcurrentHashMap<>();
        this.previousMessageScopes = this.currentMessageScopes;
        this.currentMessageScopes = new HashSet<>();
    }

    public List<Vertex> getVerticesWithActiveTraversers() {
        final List<Vertex> result = new ArrayList<>();
        for (final MessageScope messageScope : sendMessages.keySet()) {
            if (messageScope instanceof MessageScope.Local) {
                continue;
            }

            final Map<Vertex, Queue<M>> messages = sendMessages.get(messageScope);
            final List allMessages = Stream.of(messages.keySet())
                    .filter(q -> messages.get(q) != null && messages.get(q).stream().anyMatch(t -> !((Traverser.Admin) t).isHalted()))
                    .flatMap(q -> q.stream())
                    .collect(Collectors.toList());

            result.addAll(allMessages);
        }
        return result;
    }
}
