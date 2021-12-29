# not-any

An aggregate function that tests if for all rows in the group (pred row) returns false-or-nil, accepts a [expr](expr.md) as an arg.

e.g

```clojure 
;; in this example, we use any to test if no customers have an order total over 35
[[:from :Order]
 [:agg
  [:customer-id]
  [:no-high-valued-order [rel/not-any [<= 35.0M :total]]]]]

;; TABLE STATE
{:Order [{:customer-id 42, :total 10.0M}
         {:customer-id 42, :total 12.0M}
         {:customer-id 43, :total 50.0M}]}

;; RESULT
[{:customer-id 42, :no-high-valued-order true}
 {:customer-id 43, :no-high-valued-order false}]
```