# :unique

The `:unique` operation creates a unique index & constraint.

Normally you would use `:unique` indexes only as constraints, but they can be used as a performance hint.

Here is an example of indexing customers by name.

```clojure 
;; QUERY
;; you index expressions, here I am going to use a single expression,
;; but I could use multiple and get a nested map, e.g [:unique :a, :b, :c]
[[:from :Customer] 
 [:unique :name]]
 
;; STATE
{:Customer [{:name "bob", :age 42}
            {:name "alice", :age 23}]}
         
;; INDEX
;; a clojure map
;; get with rel/index
{"bob" {:name "bob", :age 42}
 "alice" {:name "alice", :age 23}}
```