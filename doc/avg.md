# avg

An aggregate function that computes the average for all sampled values by some [expr](expr.md)

```clojure 
[[:from :Orders]
 [:agg [] [:aov [rel/avg :total]]]
```