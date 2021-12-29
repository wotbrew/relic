# :expand

Provides a means of flattening trees, that is you might start your relational data with nested (document) style maps, 
and you want to de-nest to make nested data accessible via joins.

## Examples

```clojure 
;; In this example, Order contains nested 'order' maps with an :items collection.
(rel/q db Order)
;; =>
#{{:customer-id 42, 
  :items [{:product 1, :quantity 2}, {:product 2, :quantity 1}]}}

;; I want to see OrderItems as their own relation, so I :expand them.
(def OrderItem
  [[:from :Order]
   [:expand [[:product :quantity] :items]]
   [:without :items]])
   
;; You can see the expansion columns are included along with the order header columns.   
(rel/q db OrderItem)
;; =>
#{{:customer-id 42, :product 1, :quantity 2}, {:customer-id 42, :product 2, :quantity 1}}
```