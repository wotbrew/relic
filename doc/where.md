# :where

Selects a subset of rows by applying [predicate](expr.md) conditions to them before flowing onwards.

Just like SQL `WHERE`.

```clojure 
[[:from :Customer]
 [:where [< :age 42] [= :name "bob"]]]
```

See [expression reference](expr.md) for information on expressions.