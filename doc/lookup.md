# :lookup

The `:lookup` operation is used for queries against [indexes](indexes.md), when you need to be explicit.

One thing to know about `:lookup` is you cannot `materialize` it, it is fairly unique in that regard.

```clojure 
(def CustomerByName [[:from :Customer] [:hash :firstname]])
(def db (rel/materialize {} CustomerByName))
(def db (rel/transact {} [:insert :Customer {:firstname "bob"}]))
(rel/q db [[:lookup CustomerByName "bob"]]) 
;; =>
({:firstname "bob"})
```