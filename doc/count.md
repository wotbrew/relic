# count

You can use `clojure.core/count` as an aggregate function! It does pretty much what you expect, count rows within a group.

Has an optional [expr](expr.md) argument if you want to count rows only where some predicate holds (handy!).

```clojure 
;; QUERY
[[:from :Order]
 [:agg [:customer-id] 
  [:number-of-orders count]
  [:number-of-refunded-orders [count :refunded]]]]
  
;; STATE
{:Order [{:customer-id 42, :order-id 0}, {:customer-id 42, :order-id 1, :refunded true}]}

;; RESULT
[{:customer-id 42, :number-of-orders 2, :number-of-refunded-orders 1}]
```