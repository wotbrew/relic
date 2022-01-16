# :insert-or-replace

The `:insert-or-replace` [`transact`](transact.md) form allow you to insert rows, but on
[`:unique`](unique.md) key conflict, replace the old row with the new one.

```clojure 
(def db (rel/mat {} [[:from :Customer] [:unique :id]]))

;; if there is no conflict, its just insert
(def db (rel/transact db [:insert-or-replace :Customer {:id 42, :name "bob", :age 33}}]))
;; => 
{:Customer #{{:id 42, :name "bob", :age 33}}}

;; with a conflict, the whole row will be replaced - we will change the name, and 
;; drop :age.
(rel/transact db [:insert-or-replace :Customer {:id 42, :name "alice"}])
;; =>
{:Customer #{{:id 42, :name "alice"}}}
```

See also [`:insert-or-merge`](insert-or-merge.md),
[`:insert-or-update`](insert-or-update.md).