package com.aerospike.firefly.process.computer.local;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public class BatchSingleMessenger<M> implements Messenger<M>{

    private final Messenger<M> baseMessenger;
    private final M message;

    public BatchSingleMessenger(final Messenger<M> baseMessenger, final M message) {
        this.baseMessenger = baseMessenger;
        this.message = message;
    }

    public Iterator<M> receiveMessages() {
        return IteratorUtils.of(this.message);
    }

    public void sendMessage(final MessageScope messageScope, final M message) {
        this.baseMessenger.sendMessage(messageScope, message);
    }
}
