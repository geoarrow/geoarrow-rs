# Multi-threading

A Table contains internal "chunking" where the first _n_ rows for all columns are part of the first chunk, then the next _n_ rows for all columns are part of the second chunk, and so on. The geometry column of the table is a [chunked geometry array](../api/core/geometry/chunked.md), which similarly is grouped into batches of _n_ rows.

Multi-threading is enabled out-of-the-box for all operations on these chunked data structures.

This means that to take advantage of multi-threading, your table must have internal chunking. This is the default whenever reading directly from a file (you can tweak the `batch_size` argument).

When creating a table from an existing data source, such as GeoPandas or Pyogrio, the data may not already be chunked. In the future, a `rechunk` operation will be added to assist in creating more internal chunks to ensure operations are multi-threaded.
