# :from

`:from` is an operation of 1-arg, either a query or keyword (table), it performs no computation, and just flows rows from its argument to the next operation in the query.


```clojure

;; from a table
[[:from :A]]

;; from another query
[[:from [[:from :B]
         [:where [= :foo 42]]]]]

```