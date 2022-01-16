# Materialization

One of the key value propositions of relic, or at least a major super-power is the ability to materialize (`mat`) queries.

By materializing a query, the results are cached, and further more transactions that would change the results automatically invalidate 
the cache. `relic` does this incrementally, it knows how to flow only necessary changes, 
and minimize the work done on change by using a bunch of different internal indexes.

It means you can have aggregations over multiple joined tables in your reagent application for example without
any custom data structures or custom code to maintain the various indexes and structures that would be necessary to incrementally compute it.

```clojure
;; a query like this can change when segments change for orders, orders are changed. 
;; relic will know how to change the sum without recomputing the whole query, it does this with a custom index for the sum that allows 
;; relic not to re-sum rows that haven't been touched.
(rel/mat db [[:from :Order]
             [:join CustomerSegment {:customer-id :customer-id}]
             [:agg [:segment] [:revenue [rel/sum :total]]]])
```

As you are trading memory for time by materializing queries, you might want to dematerialize them. `(rel/demat db query)`

See also: [constraints](constraints.md), [change tracking](change-tracking.md), [query](query.md).