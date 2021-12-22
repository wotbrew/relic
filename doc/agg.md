# :agg

Relic provides a means to group rows and run aggregations on them. This analogous to SQL `GROUP BY`.

Say you have a table of maps representing customer orders, and you want to compute some stats per customer:

```clojure 
[[:from :Order]
 [:agg
  ;; the first arg to :agg is the grouping vector
  ;; the columns to group by (can be empty for all rows)
  [:customer-id]
  
  ;; the rest of the args are aggregate extensions to the grouped
  ;; columns
  [:total-spend [rel/sum :total]]
  [:average-spend [rel/avg :total]]
  [:order-count count]]]

;; given an initial state
{:Order [{:customer-id 42, :total 12.0M}
         {:customer-id 42, :total 25.0M}]}

;; the results would be
({:customer-id 42, :average-spend 18.5M, :order-count 2, :total-spend 37.0M})
```

See also: [aggregates](aggregates.md) for aggregate functions that you can use.

## Spec

```clojure
stmt = [:agg cols & bindings]
cols = [& col]
binding = [new-col agg-expr]
agg-expr = agg-fn|[agg-fn & args]
```