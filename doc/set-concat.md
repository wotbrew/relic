# set-concat

The `set-concat` [aggregation](aggregates.md) function can be used to bind a set of values across a group of rows.

Takes an [expression](expr.md) as an argument.

```clojure 
;; QUERY
[[:from :OrderItem]
 [:agg [:order-id] [:items [rel/set-concat :%]]]

;; STATE 
{:OrderItem #{{:order-id 0, :product "bread", :qty 2}, 
              {:order-id 0, :product "eggs", :qty 1}}}
              
;; RESULTS
({:order-id 0, :items #{{:order-id 0, :product "bread", :qty 2}, {:order-id 1, :product "eggs", :qty 1}}})
```