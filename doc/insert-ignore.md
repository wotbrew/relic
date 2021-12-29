# :insert-ignore

The `:insert-ignore` [`transact`](transact.md) form allow you to insert rows, but on
[`:unique`](unique.md) key conflict, discard the new row and keep the old.

```clojure 
(def db (rel/materialize {} [[:from :Customer] [:unique :id]]))

;; if there is no conflict, its just insert
(def db (rel/transact db [:insert-ignore :Customer {:id 42, :name "bob", :age 33}}]))
;; => 
{:Customer #{{:id 42, :name "bob", :age 33}}}

;; with a conflict, the new row will be discarded.
(rel/transact db [:insert-ignore :Customer {:id 42, :name "alice"}])
;; =>
{:Customer #{{:id 42, :name "bob", :age 33}}}
```

See also [`:insert-or-merge`](insert-or-merge.md),
[`:insert-or-replace`](insert-or-replace.md),
[`:insert-or-update`](insert-or-update.md),
[`:update`](update.md).