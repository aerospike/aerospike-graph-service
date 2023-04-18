import java.util.stream.IntStream

try {
    g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using("localhost", 8182, "g"));
    IntStream.range(0, 48).forEach { it ->
        g.V("FIREFLY_WARMUP").next()
    }
} catch (e) {
    println "Failed to perform warmup routine: ${e.getMessage()}"
}

