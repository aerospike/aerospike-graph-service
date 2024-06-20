package com.aerospike.firefly.process.computer.local;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
}
