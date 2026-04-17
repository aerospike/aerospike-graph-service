# ID management

## `~id` vs `id` in Gremlin

`~id` is a special keyword in Gremlin. It enables direct lookup of a
vertex or edge by its canonical identifier — a single-record Aerospike
read, not a secondary-index scan. `T.id` is an alias for `~id`.

- For **vertices**, `~id` may be supplied by the user or auto-generated
  by Aerospike Graph Service.
- For **edges**, `~id` is always auto-generated. It cannot be
  user-supplied.
- Plain `id` (without the leading tilde) is not a special keyword. It
  is an ordinary property, stored and looked up like any other.

## System-generated IDs

Auto-generated `~id` values start at `-1` and descend (`-1`, `-2`, …).
This keeps the positive-integer range fully available for user-supplied
IDs without any conflict-check overhead: if you hand-assign `1`, `2`,
`3`, the system will never collide with your range.

## Examples

```groovy
g.addV("person").property(T.id, 1).property("name", "Alice").iterate()
// Creates a vertex with label=person, ~id=1, name=Alice.

g.addV("person").property(T.id, 2).property("name", "Bob").iterate()
g.addV("person").property(T.id, 3).property("name", "Carol").iterate()

g.addV("person").property(T.id, 3).property("name", "Dave").iterate()
// Error: a vertex with ~id=3 already exists.

g.V(1).next()
// Returns Alice — direct record lookup on ~id=1.

g.V(1, 2, 3).toList()
// Returns [Alice, Bob, Carol] — single batch read of three records.

g.addV("person").property("id", 4).property("name", "Dave").iterate()
// Creates a vertex with property id=4 (ordinary property, not ~id).

g.V(4).next()
// Fails: there is no vertex with ~id=4.

g.V().has("id", 4).next()
// Returns Dave — requires a secondary-index query or scan.
```

## Takeaway

If you care about lookup latency, use `T.id` / `~id`. Properties named
`id` cost a scan or an index hit; `~id` is a point read.
