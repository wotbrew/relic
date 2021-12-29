# top

An [aggregate](aggregates.md) functions that returns the highest `n` values for some [expr](expr.md) across the group.

In the below example we use top to find the highest 5 scores for a table of Player data.

```clojure 
;; QUERY
[[:from :Player]
 [:agg [] [:highest-scores [rel/top 5 :score]]]]

;; STATE
{:Player [{:score 1}
          {:score 200}
          {:score 4323}
          {:score 5555}
          {:score 4242}
          {:score -123}
          {:score 330}]}

;; EXPECTED
[{:highest-scores [5555, 4323, 4242, 330, 200]}]
```