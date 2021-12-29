# :intersection

Set intersection, returns rows in the left relations that are also in each right relation. Accepts multiple tables/queries as parameters.
Same kind of thing as `clojure.set/intersection`.

## Examples

```clojure

(def GoodCustomer
  [[:from :Customer] 
   [:where [:<= 100 :score]]])

(def John 
  [[:from :Customer]
   [:where [= :firstname "John"]]])

(def GoodJohn
  [[:from GoodCustomer]
   [:intersection John]])
```