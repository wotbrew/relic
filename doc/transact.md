# Transactions

Unless you are programming [`:const`](const.md) forms, you will need to manipulate tables and state in order to say anything useful.

In relic, you manipulate your database with `transact`.

`transact` is a function that accepts a database and a set of 'transaction ops', returning a new database with the ops applied.

A key idea with relic is that you can only manipulate tables, and you try to keep data in tables to a minimum, rederiving everything else with queries. [`materialization`](materialization.md) enables high performance in the face of this.

## Ops

You basic operations mirror the SQL ones, see the linked docs for more info.

- [`:insert`](insert.md)
- [`:update`](update.md)
- [`:delete`](delete.md)
- [`:delete-exact`](delete-exact.md)
- [`:insert-ignore`](insert-ignore.md)
- [`:insert-or-replace`](insert-or-replace.md)
- [`:insert-or-merge`](insert-or-merge.md)
- [`:insert-or-update`](insert-or-update.md)
- [`:replace-all`](replace-all.md)
- [terse insert](terse-insert.md)


See also [change tracking](change-tracking.md), [constraints](constraints.md).