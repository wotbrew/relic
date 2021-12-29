# min

The `min` [aggregate](aggregates.md) function allows you to find the lowest value across a set of rows.

In this example, we take the minimum `:id` across the entire `:Customer` table.

```clojure 
;; QUERY
[[:from :Customer] 
 [:agg [] [:min-id [min :id]]]]

;; STATE 
{:Customer #{{:id 0}, {:id 1}, {:id 2}}}

;; RESULT
({:min-id 0})
```