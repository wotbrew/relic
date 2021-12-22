# any

An aggregate function that tests if for any row in the group (pred row) returns truthy, accepts a [expr](expr.md) as an arg.

e.g 

```clojure 
;; in this example, we use any to test if the customer has 
;; any order total over 35
[[:from :Order]
 [:agg
  [:customer-id]
  [:has-high-valued-order [rel/any [<= 35.0M :total]]]]]

;; TABLE STATE
{:Order [{:customer-id 42, :total 10.0M}
         {:customer-id 42, :total 12.0M}
         {:customer-id 43, :total 50.0M}]}

;; RESULT
[{:customer-id 42, :has-high-valued-order false}
 {:customer-id 43, :has-high-valued-order true}]
```