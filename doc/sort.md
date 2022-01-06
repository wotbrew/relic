# :sort

The `:sort` operator allow you to sort rows by some sequence of expressions.

`:sort` should be used in the terminal position of a query (e.g the last operation), otherwise the sort is not guaranteed.

```clojure 
[[:from :Customer]
 [:sort [:age :desc] [:firstname :asc]]]
```

`:sort` takes vectors of `[expr (:asc OR :desc)]` for `ascending` and `descending` sorts.

You can use any row [expression](expr.md) in each sort expression as long all values are `Comparable`. 

Uses a [`btree`](btree.md) index internally, so that it can be [materialized](materialization.md).