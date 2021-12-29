# :union

Set union, returns rows in the left relations and each right relation. Accepts multiple tables/queries as parameters.
Same kind of thing as `clojure.set/union`.

## Examples

```clojure
(def Mary
  [[:from :Customer] 
   [:where [= :firstname "Mary"]]])

(def John 
  [[:from :Customer]
   [:where [= :firstname "John"]]])

(def JohnAndMary
  [[:from John]
   [:union Mary]])

;; union doesn't need a left side
;; the below would also work.
[[:union John Mary]]
```