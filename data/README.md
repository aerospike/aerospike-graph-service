# `data/` — sample graphs used by tests and documentation

This directory ships a handful of small graph fixtures that the test
suite, benchmarks, and tutorials load. They are kept deliberately small
so that a `git clone` is still cheap; larger datasets belong outside
the repository and are downloaded on demand by the workload that needs
them.

## Files

### `air-routes-small.*`

A trimmed-down extract of Kelvin R. Lawrence's **Air Routes** graph,
which models a set of airports (vertices) and the commercial flight
routes between them (edges). It is the canonical "first real graph"
used by most TinkerPop and Gremlin tutorials.

Three encodings of the same graph are provided so tests can exercise
each of Gremlin's standard I/O formats without re-serializing:

| File                          | Format  | Purpose                                                                                                   |
| ----------------------------- | ------- | --------------------------------------------------------------------------------------------------------- |
| `air-routes-small.graphml`    | GraphML | XML; default for `io(graphml())` and the bulk-loader integration tests.                                   |
| `air-routes-small.json`       | GraphSON | Gremlin JSON; round-tripped by `io(graphson())`.                                                         |
| `air-routes-small.kryo`       | Gryo    | Binary; fastest load path, used by the OLAP/Spark tests that care about I/O overhead.                    |

**Attribution.** Original data © Kelvin R. Lawrence, released as part
of the [*Practical Gremlin*](https://kelvinlawrence.net/book/Gremlin-Graph-Guide.html)
book and its companion repository,
[`krlawrence/graph`](https://github.com/krlawrence/graph). Redistributed
under the Apache License 2.0, with the author's permission, as a
fixture for this project's test suite and documentation. The
`-small` suffix indicates that this file is a trimmed subset of the
original; see the upstream repository for the full dataset and for
any subsequent corrections or additions.

If you use these files in derivative work, please credit Kelvin
Lawrence and link back to the upstream repository.

### `docker-bulk-load/`

Input staging area used by the Docker-based bulk-loader integration
tests. Contains a `config.properties` and a `sampledata/` directory
that the tests mount into the bulk-loader container.
