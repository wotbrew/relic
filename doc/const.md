# :const

Provides a constant collection as a relation. Probably the most boring operator, of questionable utility.

Use if you want to do relic processing on known-ahead-of-time sequences of maps, and don't want to use tables for $reasons.

```clojure
[[:const [{:a 1} {:a 2} {:a 3}]]
 [:where [even? :a]]]
;; =>
[{:a 2}]
```