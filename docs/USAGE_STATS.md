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

Example output:
```
{   'raw': [   {   'epoch-ms-final': 1702327851857,
                   'epoch-ms-start': 1702326901857,
                   'memory-gb': 32,
                   'uuid': '9c0ba414-c9f2-492f-adb9-9792bb6dfed1',
                   'vcpus': 16},
               {   'epoch-ms-final': 1702327148084,
                   'epoch-ms-start': 1702327148084,
                   'memory-gb': 9,
                   'uuid': 'c924f45e-886d-4c04-8039-292d56ea037c',
                   'vcpus': 16},
               {   'epoch-ms-final': 1702325278838,
                   'epoch-ms-start': 1702325278838,
                   'memory-gb': 9,
                   'uuid': '45daadf4-0cdb-49e8-b6aa-2aae7bf489b0',
                   'vcpus': 16},
               {   'epoch-ms-final': 1702323995477,
                   'epoch-ms-start': 1702323995477,
                   'memory-gb': 9,
                   'uuid': '3dfb41cf-0ce1-4326-a2f9-730f63a46db5',
                   'vcpus': 16}],
    'total-vcpu': 0.0004819888381532217}
```