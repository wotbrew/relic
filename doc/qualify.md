# :qualify

Qualify is a convenience form that lets you namespace all columns returned by a query.

```clojure 
;; QUERY
[[:from :Customer]
 [:qualify "customer"]]

;; STATE 
{:Customer #{{:id 42, :name "bob"}}}

;; RESULTS
({:customer/id 42, :customer/name "bob"})
```

See also [`:rename`](rename.md)