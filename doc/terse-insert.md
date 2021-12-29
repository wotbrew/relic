# Terser insert

It can be convenient for example when constructing an initial database to throw data into the database using some initial state map.

The terse input form allows you to insert into multiple tables with one [transaction](transact.md) form. It is (potentially) faster too.

```clojure 
(rel/transact db {:Customer [{:id 0, :name "Fred"}], :Order [{:customer 0, :total 10.0M}]})
;; is the same as
(rel/transact db [:insert :Customer {:id 0, :name "Fred"}] [:insert :Order {:customer 0, :total 10.0M}])
```

See also [`:insert`](insert.md)