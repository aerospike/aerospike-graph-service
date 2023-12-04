# Usage Stats

Usage stats are collected by default and cannot be disabled.

Each time Aerospike Graph Service is started, it will record the start time and periodically record the current time.
The vcpu and memory size are also collected. By doing this we can get a rough idea of the resource usage of the service.

The usage stats can be collected by invoking the follow command in gremlin:

```
g.call("usage-stats").next()
```

This will return a Map<String, Object> that contains the following fields:
- raw: the raw usage stats data
- total-vcpu: the total vcpu years used by all services

The usage stats from a specific date can also be collected by invoking the follow command in gremlin:

```
g.call("usage-stats").with("since", "yyyy-mm-dd").next()
```
example:
```
g.call("usage-stats").with("since", "2023-03-30").next()
```

This will return a Map<String, Object> that contains the following fields:
- raw: the raw usage stats data
- total-vcpu: the total vcpu years used by all services since the provided date
