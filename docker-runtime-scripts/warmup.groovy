import com.aerospike.firefly.util.WarmupUtil
g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using("localhost", 8182, "g"));
WarmupUtil.invokeWarmup(g)
