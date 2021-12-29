# :fk

The foreign key constraint allows you to ensure referenced rows always exist.

Like other [constraints](constraints.md), `:fk` can be applied to any query, not just tables.

```clojure 
(def OrderMustReferToACustomer
  [[:from :Order] [:fk :Customer {:customer-id :customer-id}]])
  
(def db (rel/materialize {} OrderMustReferToACustomer))

;; this is fine
(rel/transact db [:insert :Customer {:customer-id 42}] [:insert :Order {:customer-id 42}])

;; this is also fine, you can insert out-of-order.
(rel/transact db [:insert :Order {:customer-id 42}] [:insert :Customer {:customer-id 42}])

;; BOOM! Foreign key violation
(rel/transact db [:insert :Order {:customer-id "woops"}])
```

## Cascading deletes

Like in SQL you can specify a `:cascade` option to unwind references that are invalidated by transactions.


The below example would cause `:Order` rows to be themselves be deleted if a transaction deletes the customers they point to. 
```clojure
[[:from :Order]
 [:fk :Customer {:customer-id :customer-id} {:cascade :delete}]]
```

`:cascade` only works if the left-side is a table (at the moment!).