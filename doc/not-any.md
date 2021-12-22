# not-any

An aggregate that tests for all row in the group (pred row) returns falsey, accepts an [expr](expr.md) as an arg.

e.g

```clojure 
;; :has-no-a will be true if all rows have a falsey `:a` col
[[:from :A]
 [:agg [] [:has-no-a [rel/not-any :a]]]
```