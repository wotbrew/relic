# :insert

The `:insert` [transact](transact.md) form is how you add rows to tables.

Like other transact forms, it can throw exceptions if [constraints](constraints.md) are violated.

```clojure 
(def db {})

(rel/transact db [:insert :Food {:name "pizza"}, {:name "pie"}])
;; =>
{:Food #{{:name "pizza"}, {:name "pie"}}} 
```

See also [terser insert](terse-insert.md), [`:insert-or-replace`](insert-or-replace.md), [`:insert-or-merge`](insert-or-merge.md),
[`:insert-or-update`](insert-or-update.md).