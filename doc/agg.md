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

Let's break down the shape of the `:agg` operator:

`[:agg cols & agg-bindings]`

The `cols` will be a vector of column names, so in the example above `[:customer-id]` is used so we will be aggregating 
over rows grouped by their customer-id. You could have used `[:customer-id, :delivery-date]` or any combination of columns.

The empty vector `[]` can be used to group over all rows.

Each `agg-binding` is a vector tuple `[binding agg-expr]`, the `binding` is the name of the new column 
you want the aggregated value to sit under, the `agg-expr` consists of an aggregation function (and any arguments).

So for `[:total-spend [rel/sum :total]]` 
    
- `:total-spend` is the binding 
- `rel/sum` is an aggregation function

Aggregation functions are built-in, they are not normal functions unfortunately - this is necessary for materialized views to work.

You may later be able to extend relic with user defined aggregations, but the tools for that are not final.

See also: [aggregates](aggregates.md) for aggregate functions that you can use.

## N.B Empty relations

If you are aggregating over all rows, `:agg` will always return a row, even if the input relation is empty.

e.g you will get `{:count 0}` back (not `nil`) if you ask for a count of all rows and there are no rows.