### Property Indices

Firefly utilizes Aerospike Secondary Indices for property lookups. This is implemented for Vertex and Edge.

Exact match is supported for String queries and for Numeric queries.
Range (less than, greater than) is supported for Numeric queries.
Substring queries are not currently supported. 

Queries that begin with a global search over the graph `g.V().has(x,y)` will use an index.
Queries that first filter then match `g.V().label('z').has(x,y)` will not utilize index to match because the index is 
over the global set, not over the subset.  

### Label Indices 

Firefly will use an Aerospike secondary index for label lookups.
