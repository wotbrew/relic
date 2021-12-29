# bottom

An [aggregate](aggregates.md) functions that returns the lowest `n` values for some [expr](expr.md) across the group.

In the below example we use bottom to find the lowest 5 scores for a table of Player data.

```clojure 
;; QUERY
[[:from :Player]
 [:agg [] [:lowest-scores [rel/bottom 5 :score]]]]

;; STATE
{:Player [{:score 1}
          {:score 200}
          {:score 4323}
          {:score 5555}
          {:score 4242}
          {:score -123}
          {:score 330}]}

;; EXPECTED
[{:lowest-scores [-123
                  1
                  200
                  330
                  4242]}]
```