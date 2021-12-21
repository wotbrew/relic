# `rel/any`

An aggregate function that tests if for any row in the group (pred row) returns truthy, accepts a [expr](expr.md) as an arg.

e.g 

```clojure 
;; :has-a will be true if any row has a truthy `:a` col
[[:from :A]
 [:agg [] [:has-a [rel/any :a]]]
```