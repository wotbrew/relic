# `:from`

`:from` is a statement of 1-arg, either a relvar or keyword (table), it performs no computation, and just flows rows from its argument to the next node in the relvar.


```clojure

;; from a table
[[:from :A]]

;; from another relvar
[[:from [[:from :B]
         [:where [= :foo 42]]]]]

```