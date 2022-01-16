# :insert-or-update

The `:insert-or-update` [`transact`](transact.md) form allow you to insert rows, but on
[`:unique`](unique.md) key conflict, apply a function (or SQL style [update map](update.md)) to the old row instead.

```clojure 
(def db (rel/mat {} [[:from :Customer] [:unique :id]]))

;; if there is no conflict, its just insert
(def db (rel/transact db [:insert-or-update :Customer {:updates [inc [:or :updates 0]]} {:id 42, :name "bob", :age 33}}]))
;; => 
{:Customer #{{:id 42, :name "bob", :age 33}}}

;; with a conflict, the function (or set map) will be applied to the OLD row, and the new row will be discarded.
(rel/transact db [:insert-or-replace :Customer {:updates [inc [:or :updates 0]]} {:id 42, :name "alice"}])
;; =>
{:Customer #{{:id 42, :name "bob", :age 33, :updates 1}}}
```

See also [`:insert-or-merge`](insert-or-merge.md),
[`:insert-or-replace`](insert-or-replace.md),
[`:update`](update.md).