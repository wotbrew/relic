# Indexes

Relic allows you to `materialize` indexes to enable certain query optimisations and to allow for direct index access for specialised high-performance work.

- [`:hash`](hash.md) nested hash map
- [`:btree`](btree.md) nested sorted map
- [`:unique`](unique.md) nested hash map that only allows 1 row per key

You shouldn't need to worry about indexes most of the time, as relic creates them for you
for operations that really need them such as `:join`, `:left-join` and `:fk`.