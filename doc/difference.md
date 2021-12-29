# :difference

Set difference, returns rows in the left relations that are not in the right. Accepts multiple tables/queries as parameters.
Same kind of thing as `clojure.set/difference`.

## Examples

```clojure

(def GoodCustomer
  [[:from :Customer] 
   [:where [:<= 100 :score]]])

(def BadCustomers
  [[:from :Customer]
   [:difference GoodCustomer]])
```