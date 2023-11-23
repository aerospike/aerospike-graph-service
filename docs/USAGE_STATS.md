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
- total-vcpu-hrs: the total vcpu hours used by all services 
- total-vcpu-yrs: the total vcpu years used by all services
