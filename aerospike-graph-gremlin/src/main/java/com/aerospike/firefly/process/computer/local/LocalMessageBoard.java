package com.aerospike.firefly.process.computer.local;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    public void clear() {
        this.sendMessages.clear();
        this.receiveMessages.clear();
        this.previousMessageScopes.clear();
        this.currentMessageScopes.clear();
    }

    public List<Vertex> getVerticesWithTraversers() {
        final List<Vertex> result = new ArrayList<>();
        for (final MessageScope messageScope : sendMessages.keySet()) {
            if (messageScope instanceof MessageScope.Local) {
                continue;
            }

            final Map<Vertex, Queue<M>> messages = sendMessages.get(messageScope);
            final List allMessages = messages.keySet().stream()
                    .filter(q -> messages.get(q) != null
                            // && messages.get(q).stream().anyMatch(ts -> ((TraverserSet) ts).stream().anyMatch(t -> !((Traverser.Admin) t).isHalted())))
                            && messages.get(q).stream().anyMatch(ts -> !((TraverserSet) ts).isEmpty()))
                    .collect(Collectors.toList());

            result.addAll(allMessages);
        }
        return result;
    }

    public TraverserSet getActiveTraversers() {
        final TraverserSet result = new TraverserSet();
        for (final MessageScope messageScope : sendMessages.keySet()) {
            if (messageScope instanceof MessageScope.Local) {
                continue;
            }

            final Map<Vertex, Queue<M>> messages = sendMessages.get(messageScope);
            final List halted = (List) messages.keySet().stream()
                    .filter(q -> messages.get(q) != null)
                    .flatMap(q -> messages.get(q).stream())
                    .flatMap(ts -> ((TraverserSet) ts).stream())
                    .collect(Collectors.toList());

            result.addAll(halted);
        }
        return result;
    }
}
