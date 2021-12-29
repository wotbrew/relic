# top-by

An [aggregate](aggregates.md) function that returns the highest `n` rows by some [expr](expr.md).

In the below example, we bind the top 5 highest scoring players.

```clojure 
;; QUERY
[[:from :Player]
 [:agg [] [:highest-scoring [rel/top-by 5 :score]]]]

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
({:highest-scoring [{:score 5555, :name "hannah"}
                    {:score 4323, :name "fred"}
                    {:score 4242, :name "george"}
                    {:score 330, :name "dave"}
                    {:score 200, :name "bob"}]})
```