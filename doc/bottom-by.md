# bottom-by

An [aggregate](aggregates.md) function that returns the lowest `n` rows by some [expr](expr.md).

```clojure 
[[:from :A]
 [:agg [] [:lowest-5a [rel/bottom-by 5 :a]]]]
```