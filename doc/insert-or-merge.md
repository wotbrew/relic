# :insert-or-merge

The `:insert-or-merge` [`transact`](transact.md) form is used when you want to insert rows, but on [`:unique`](unique.md) key conflict, 
instead merge the new row into the old one.

Takes a binding form that lets you specialise which cols you want to merge.

```clojure 
;; I want to merge by-id.
(def db (rel/mat {} [[:from :Customer] [:unique :id]])

;; lets insert a customer, if you do not have a conflict, this is the 
;; same as insert
(def db (rel/transact db [:insert-or-merge :Customer :* {:id 42, :name "bob"}]))
;; => 
{:Customer #{{:id 42, :name "bob"}}}

;; lets insert again with a conflict
;; the :* binding means, merge 'all' columns from the new row into the old one
;; you could use a vector to be selective.
(rel/transact db [:insert-or-merge :Customer :* {:id 42, :age 33}])
;; =>
{:Customer #{{:id 42, :name "bob", :age 33}}}
```

See also [`:insert-or-replace`](insert-or-replace.md),
[`:insert-or-update`](insert-or-update.md).

## binding

- `:*` all cols
- `[col1, col2..]` a subset of cols