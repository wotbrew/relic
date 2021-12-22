# avg

An aggregate function that computes the average for all sampled values by some [expr](expr.md).

In the below example, we use `avg` to compute the average-order-value (aov) over a group of orders.

```clojure 
;; RELVAR
[[:from :Order]
 [:agg [] [:aov [rel/avg :total]]]]

;; STATE
{:Order [{:total 10.0M}
         {:total 25.0M}]}

;; RESULT 
[{:aov 17.5M}]
```