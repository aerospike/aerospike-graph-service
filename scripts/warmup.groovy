import java.util.stream.IntStream

g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using("localhost", 8182, "g"));
IntStream.range(0, 24).forEach { it ->
    g.V("FIREFLY_WARMUP").next()
}


