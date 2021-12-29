# max

The `max` [aggregate](aggregates.md) function allows you to find the highest value across a set of rows.

In this example, we take the maximum `:id` across the entire `:Customer` table.

```clojure 
;; QUERY
[[:from :Customer] 
 [:agg [] [:max-id [max :id]]]]

;; STATE 
{:Customer #{{:id 0}, {:id 1}, {:id 2}}}

;; RESULT
({:max-id 2})
```