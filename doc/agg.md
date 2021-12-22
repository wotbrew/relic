# :agg

Relic provides a means to group rows and run aggregations on them. This analogous to SQL `GROUP BY`.

## Form

```clojure
stmt = [:agg cols & bindings]
cols = [& col]
binding = [new-col agg-expr]
agg-expr = agg-fn|[agg-fn & args]
```

## Examples

```clojure 
[[:from :A]
 [:agg [:a]
   [:n [rel/sum :b]]
   [:n2 [rel/avg :b]]
   [:c count]]
```

See also: [aggregates](aggregates.md) for aggregate functions.