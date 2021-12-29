# min-by

The `min-by` [aggregate](aggregates.md) function allows you to find the row with some lowest value across a set of rows.

In this example, we get the row having the minimum `:id` across the entire `:Customer` table.

```clojure 
;; QUERY
[[:from :Customer] 
 [:agg [] [:min-id-row [rel/min-by :id]]]]

;; STATE 
{:Customer #{{:id 0}, {:id 1}, {:id 2}}}

;; RESULT
({:min-id-row {:id 0}})
```