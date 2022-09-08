### Standard data layout

```java
    @Test
    void testWrite2VertexWithEdge() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
            .has("type", "taxonomy").as("a")
            .V().has("type", "plant").as("b")
            .addE("IsA").from("b").to("a").property("a","b").iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> s2 = g.V().has("type", "plant").next(2);
        List<Edge> things = g.E().has("a", "b").toList();
        assertEquals(2, (long)g.V(fruit.id()).inE().count().next());
    }
```

A VertexProperty is an Aerospike Record with a name, parent vertex id, key-value map, and type-hint map 
```
Record:
  Name:String, 
  ParentVertex:Long, 
  KeyValues:Map<String,V>, 
  TypeHints:Map<String,Long> 

```
``` 
aql> select * from test."0_V_PROP" 
+----+---------+----------+----------------------------+--------------------+---------+
| PK | VP_NAME | PAR_V_ID | KEY_VALUE                  | TYPE_HINTS         | ID_TYPE |
+----+---------+----------+----------------------------+--------------------+---------+
| -1 | "color" | -1       | MAP('{"color":"yellow"}')  | MAP('{"color":5}') | 1       |
| -2 | "type"  | -1       | MAP('{"type":"plant"}')    | MAP('{"type":5}')  | 1       |
| -3 | "color" | -2       | MAP('{"color":"green"}')   | MAP('{"color":5}') | 1       |
| -5 | "type"  | -3       | MAP('{"type":"taxonomy"}') | MAP('{"type":5}')  | 1       |
| -4 | "type"  | -2       | MAP('{"type":"plant"}')    | MAP('{"type":5}')  | 1       |
+----+---------+----------+----------------------------+--------------------+---------+
```

A Vertex is an Aerospike Record with a String label, associated-id caches for (IN,OUT) Edges + VertexProperties, and a counter for each associated-id cache

```
Record:
         Label          :  String, 
(cache)  VP_N_ID        :  Map<String,List<long>>,
         VP_COUNT       :  long,
         ID_TYPE        :  long,
(cache)  OUT_EDGES      :  Map<String,List<long>>,
         OUT_E_CTR      :  long,
(cache)  IN_EDGES       :  Map<String,List<long>>,
         IN_E_CTR       :  long
         CACHE_DISABLED :  bool
```
```
aql> select * from test."0_VERTEX"
+----+---------+------------------------------------+----------+---------+---------------------+-----------+----------------+-------------------------+----------+
| PK | label   | VP_N_ID                            | VP_COUNT | ID_TYPE | OUT_EDGES           | OUT_E_CTR | CACHE_DISABLED | IN_EDGES                | IN_E_CTR |
+----+---------+------------------------------------+----------+---------+---------------------+-----------+----------------+-------------------------+----------+
| -2 | "lime"  | MAP('{"color":[-3], "type":[-4]}') | 2        | 1       | MAP('{"IsA":[-2]}') | 1         | 0              |                         |          |
| -3 | "fruit" | MAP('{"type":[-5]}')               | 1        | 1       |                     |           | 0              | MAP('{"IsA":[-1, -2]}') | 2        |
| -1 | "lemon" | MAP('{"color":[-1], "type":[-2]}') | 2        | 1       | MAP('{"IsA":[-1]}') | 1         | 0              |                         |          |
+----+---------+------------------------------------+----------+---------+---------------------+-----------+----------------+-------------------------+----------+
```

An Edge is an Aerospike Record that has a String label, an IN and OUT Vertex id, a key-value map of property names and values, a map specifying type hints for property values, and a type hint for the id

```
Record:
         Label          :  String, 
         IN             :  long,
         OUT            :  long,
         0_EDGE         :  Map<String,V>,
         TYPE_HINTS     :  Map<String,List<long>>,
         ID_TYPE        :  long
```

``` 
aql> select * from test."0_EDGE"    
+----+-------+----+-----+------------------+----------------+---------+
| PK | label | IN | OUT | 0_EDGE           | TYPE_HINTS     | ID_TYPE |
+----+-------+----+-----+------------------+----------------+---------+
| -1 | "IsA" | -3 | -1  | MAP('{"a":"b"}') | MAP('{"a":5}') | 1       |
| -2 | "IsA" | -3 | -2  | MAP('{"a":"b"}') | MAP('{"a":5}') | 1       |
+----+-------+----+-----+------------------+----------------+---------+
```
