# Bulk Loader Design

## Introduction

The TinkerPop framework has no bulk loader in it. Instead you must use `g.addV()`/`g.addE()` steps
to add data to the graph.

These methods are not efficient for adding a lot of data. This is because they end up broken into single vertex and 
edge writes, that have a lot of overhead.

A bulk loader is designed to circumvent this issue. Additionally, we don't want to make our customers manually
parse files and try to convert that parsing to `addV()` and `addE()` calls. This is a huge hassle and also
is very easy to screw up. Instead, we'd like our customers to export their data to a common file format, such as
`csv` and have us take it from there.

## Potential Issues

Some challenges to consider before we get into the design:
- Vertex id type might not be valid for firefly
- You need to know the vertex id’s to write the edges
  - What if we don't have valid id types to insert, how do we know the id's to insert? 

## Potential Solutions

- Can pack original id as a property if type doesn't match
- Can be recording id's in a separate file and then bulk loading the edges with them
- Can auto generate id's in a way that can be reverse engineered, so we can get the id of any vertex any time
- Can front-load the vertices and allow auto-generated ids, then look up the id during edge loading. 
  - This makes edge loading slower, but it is the most flexible option
- Can pre-parse edge files and make a mapping file

## Bulk Loader to Firefly Loading Interface Options

- We can go through TinkerPop, this is slow, but very generic
  - Can thread this to make it faster.
- Go directly through Aerospike driver, this is very fast, but not generic
  - Would require us to rewrite the bulk loader for each data model
  - Would require us to update the bulk loader for any adjustments to data model\
- Go through FireflyGraph interface, this is pretty fast, and generic
  - Can write bulk loader once and load specific graph data model at runtime
  - Fast because we skip TinkerPop machinery and can make batch write interface

Clearly using the FireflyGraph interface directly is the best option.


## What Does the Data Look Like

We haver decided to start with `csv` as the input file format, so let's take a look at how a graph looks in `csv`.

From [Amazon's csv documentation](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html) 
we can see that we get an output vertex file and edge file. Below is the vertex file:
```
~id, name:String, age:Int, lang:String, interests:String[], ~label
v1, "marko", 29, , "sailing;graphs", person
v2, "lop", , "java", , software
```
Now the edge file:
```
~id, ~from, ~to, ~label, weight:Double
e1, v1, v2, created, 0.4
```

## Design

If we can pre-parse the files and find all edges attached to each vertex, we can directly write all edges with the
specified vertex. This avoids querying the vertex id back and modifying the vertices when an edge write comes. In other
words, we can avoid the 2 read-modify-write cycles for each edge insert (on top of the edge itself being inserted).

Our insertion should also be idempotent. If we try to insert data that is already inserted (perhaps the bulk loader dies
and is restarted), we should overwrite as we go, or alternatively (with flags set) wipe the database before starting.

A simple diagram showing the general idea for the design:
```mermaid
flowchart LR
    InputData[(Input Data)]
    InputData<-->DataReader
    subgraph Bulk Loader
    subgraph Read Threads
    DataReader[Reader for\nfile type]-->DataConverter[Convert to\ninternal type]
    end
    DataConverter-->BlockingQueue
    BlockingQueue[Blocking Queue]-->DataWriter
    subgraph Write Threads
    DataWriter[Data Writer]
    end
    end
    subgraph Firefly
    DataWriter-->FireflyGraph
    FireflyGraph-->LinkedGraph
    FireflyGraph-->TupleGraph
    FireflyGraph-->OtherGraph
    end
    LinkedGraph-->Aerospike
    TupleGraph-->Aerospike
    OtherGraph-->Aerospike
    Aerospike[(Aerospike)]
```

To make the bulk loader future-proof, the Reader should be abstract and allow future work to include new readers. The 
reader should convert to an intermediate type in the bulk loader. Future bulk loading inputs could be things such as S3
or other inputs, so this should be as generic as is reasonably possible.

Notice, Firefly will handle its own specific writing by routing requests to the appropriate data model. Some data 
model examples have been listed in the diagram.

The idea is to have a number of read and write threads running at the same time. The read and write threads share a 
block queue, this way we avoid reading too much, or writing too much. Most likely we will be constrained by writes.

The bulk loader should provide an interface that "power users" can take advantage of to implement their own Loader 
when their data source is toi=o large to serialize into a supported format.

## Bulk Loader Inputs

The following should be considered a minimum set of inputs for the bulk loader:
- `--input` or `-i`
  - The input, let's not call it a file as it could be an endpoint in the future.
  - Should accept a comma separated list, because there may be multiple input files / endpoints.
- `--conf` or `-c`
  - The configuration file for Firefly, we need this to start Firefly with the right data model.
- `--overwrite`
  - If set, we will wipe the database before starting. Default should be false.
- `--cautious`
  - If set, we check for the existence of a vertex or edge with the same id before inserting.
  - If it exists, we log it and skip it. Default should be false.

## Design Limitations

- Memory may be a limitation for two factors
  - Mapping of internal Aerospike PK/ID to provided required ~id
    - For extremely large datasets, it may be a problem keeping a mapping of IDs for every vertex and edge
    - Workaround: use the provided ~id in the csv
      - Limitation: ~id must be provided in a format which can be parsed as a `long`
      - This would only be feasible on a clean database ot prevent corruption of existing data
  - Lists of edge IDs for OUT and IN edges
    - This is generated in memory in order to avoid a read-modify-write cycle on vertexes for every edge insert
    - For extremely large datasets, especially ones with super nodes, this can eat up memory quickly
      - However unlikely, one super node may be enough to do this
    - Workaround: read-modify-write
      - Use the internal Firefly API directly which will utilize Firefly storage instead of memory
        - Unfortunately, when this is necessary, it is also when avoiding read-modify-write is most important
    - Potential solution?: 3-prong sorted bulk load
      - Prerequisites
        - The workaround (and all limitations) using the provided csv ~id must be enabled
        - The customer must generate and provide vertex datasets sorted by ~id
        - The customer must generate and provide the same edge datasets twice per edge label, one sorted by ~from and the other ~to
      - This allows us to write the IN and OUT edges of a vertex in one action
      - Downsides
        - Avoids read-modify-write, but still has a single read check per edge count to avoid duplicates
        - Still potentially can be broken by a single super node

- Data merging?
  - Only technically two modes of bulk loading are supported currently
    - Isolated (not using provided ~id in the csv)
      - Inserts the entire dataset as its own isolated system via the generated IDs
      - Existing edges and vertexes in the database are not connected to the newly bulk inserted ones
    - Overwrite (use the provided ~id in the csv)
      - Overwrites existing vertexes and edges if they happen to use the ID provided
      - This is more of a FYI, since we assume if someone is bulk loading using provided ~id it is on a clean database
  - Do we want to support this?
    - If so, need to brainstorm ideas
