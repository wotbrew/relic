# :replace-all

The `:replace-all` [transact](transact.md) form lets you completely replace all rows in a table with a new set.

It is the same as issuing `[:delete table] [insert table row1, row2 ...]` but can be more efficient.

```clojure 
(rel/transact db [[:replace-all :Customer customer1 customer2]])
```