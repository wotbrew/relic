# bottom-by

An [aggregate](aggregates.md) function that returns the lowest `n` rows by some [expr](expr.md).

In the below example, we bind the bottom 5 lowest scoring players.

```clojure 
;; QUERY
[[:from :Player]
 [:agg [] [:lowest-scoring [rel/bottom-by 5 :score]]]]

;; STATE
{:Player [{:score 1
           :name "alice"}
          {:score 200
           :name "bob"}
          {:score 4323
           :name "fred"}
          {:score 5555
           :name "hannah"}
          {:score 4242
           :name "george"}
          {:score -123
           :name "isabel"}
          {:score 330
           :name "dave"}]}

;; RESULT 
 [{:lowest-scoring [{:score -123, :name "isabel"}
                    {:score 1, :name "alice"}
                    {:score 200, :name "bob"}
                    {:score 330, :name "dave"}
                    {:score 4242, :name "george"}]}]
```