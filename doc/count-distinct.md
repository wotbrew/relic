# count-distinct

The count distinct [aggregate](aggregates.md) function can be used to count the number of unique values for some [expression](expr.md) over a group.

The below example counts the number of unique customers in the `:Order` table.

```clojure 
;; RELVAR 
[[:from :Order]
 [:agg [] [:n-customers [rel/count-distinct :customer-id]]]]
;; STATE 
{:Order [{:customer-id 42, :order-id 0}, {:customer-id 42, :order-id 1}, {:customer-id 43, :order-id 2}]}
;; RESULT
[{:n-customers 2}]
```