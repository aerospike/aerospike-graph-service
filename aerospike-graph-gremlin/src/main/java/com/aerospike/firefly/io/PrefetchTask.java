package com.aerospike.firefly.io;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface PrefetchTask {
    Optional<Runnable> getTask(Traversal.Admin<?, ?> traversal);
}
