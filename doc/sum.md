# sum

The `:sum` [aggregate](aggregates.md) function, sums over numeric columns in a group.

Takes an [expression](expr.md) as an argument.

For example, lets total across all the orders.

```clojure 
;; QUERY 
[[:from :Order] 
 [:agg [] [:total [rel/sum :total]]]
 
;; STATE 
{:Order #{{:total 10.0M}, {:total 15.30M}}}

;; RESULTS
({:total 25.30M})
```