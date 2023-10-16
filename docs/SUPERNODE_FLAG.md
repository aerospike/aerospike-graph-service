# What Is the ~supernode flag?

The ~supernode flag is a virtual property that can be used to
denote that a vertex will be a supernode. This is useful for
when you know that a vertex will be a supernode, so you do not
spend time (and additional resources) filling the ecache just
to turn it off after.

# How do I use the ~supernode flag?

In the example below we create a vertex and set the ~supernode flag
```
Vertex v = g.addV("test").next();
g.V(v.id()).property("~supernode", true).iterate();
```
The vertex `test` in the above example will now be internally
marked as a supernode and the edge cache will be ignored.

## Notes

Once the supernode flag is set, it cannot be unset. In other words,
you cannot mark a vertex as a supernode and then unmark it
as a supernode.

You cannot read the ~supernode flag back. If you read it back,
it will return nothing.

The value you set the flag to does not matter, all values set
to the ~supernode flag will be treated as true.