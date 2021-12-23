# :delete

The `:delete` transaction form allow you delete rows by predicate [expressions](expr.md).

In the below example, we delete rows for all people called Tom.

```clojure 
(rel/transact db [:delete :Person [= :name "Tom"]])
```

You can supply multiple predicates

```clojure 
(rel/transact db [:delete :Event [= :type "insert"] [< :ts 1640251544389]])
```

Like other [transaction](transact.md) forms you can only modify tables, all derived state is maintained by relic.