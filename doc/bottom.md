# bottom

An [aggregate](aggregates.md) functions that returns the lowest `n` values for some [expr](expr.md) across the group.

```clojure 
[[:from :A]
 [:agg [] [:lowest-5a [rel/bottom 5 :a]]]]
```