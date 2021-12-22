# :expand

Provides a means of flattening trees.

## Examples

```clojure 
(rel/q db Order)
;; =>
#{{:customer-id 42, 
  :items [{:product 1, :quantity 2}, {:product 2, :quantity 1}]}}

;; =>
(def OrderItem
  [[:from :Order]
   [:expand [[:product :quantity] :items]]
   [:without :items]])
   
(rel/q db OrderItem)
;; =>
#{{:customer-id 42, :product 1, :quantity 2}, {:customer-id 42, :product 2, :quantity 1}}
```