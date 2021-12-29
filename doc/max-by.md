# max-by

The `max-by` [aggregate](aggregates.md) function allows you to find the row with some highest value across a set of rows.

In this example, we get the row having the maximum `:id` across the entire `:Customer` table.

```clojure 
;; QUERY
[[:from :Customer] 
 [:agg [] [:max-id-row [rel/max-by :id]]]]

;; STATE 
{:Customer #{{:id 0}, {:id 1}, {:id 2}}}

;; RESULT
({:max-id-row {:id 2}})
```