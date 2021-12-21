# Expressions

As relic supports conditions and computation via various [relvar](relvar.md) statements, it needs to support a form of computation expression.

Your options are:

- use clojure functions
- use relic expressions

Where you can use expressions:

- [`:where`](where.md) conditions
- [`:extend`](extend.md) extensions
- [`:select`](select.md) projections
- [`:expand`](expand.md) expansions
- [`:join`](join.md)/[`:left-join`](left-join.md) clauses
- [`:agg`](agg.md) certain aggregates accept expressions as args (e.g [`sum`](sum.md))
- [`:hash`](hash.md) indexed expressions
- [`:btree`](btree.md) indexed expressions

## clojure functions

Clojure functions are fully supported by relic, and it is a major design goal to be able to drop to Clojure to do arbitrary (__pure!__) computation.

`n.b functions must be referentially transparent, or you risk glitches`

```clojure 
;; you can use any function as an expression

(defn my-pred? [{:keys [foo]}] (= foo 42))

[[:from :A] [:where my-pred?]]

;; in clojure you can use symbols if you want.

[[:from :A] [:where `my-pred?]]

;; same rules apply to computing columns

(defn compute-bar [{:keys [foo]}] (inc foo))

[[:from :A] [:extend [:bar compute-bar]]

[[:from :A] [:select [:bar compute-bar]]

;; and expansions
(defn compute-baz-seq [{:keys [foo]}] (range foo))

[[:from :A] [:expand [:baz compute-baz-seq]]
```


## relic expressions

For convenience, and some extra goodies - an expression dsl is provided that lets you do simple computations 'inline',
provides better ergonmics as you are working with rows 100% of the time, and allows for some extra goodies like sub queries, nil safe function application and so on.

Mostly the execution semantics are similar to clojure, but vectors are used for calls over lists, and keywords are substituted for lookups.

The goal is to avoid quote/unquote template shenanigans, and help make programming relic easier for simple cases that don't need a unique clojure function. 

```clojure
;; calls are vectors, `:foo` will be substituted with `(:foo row)`
[[:from :A] [= :foo 42]]

;; non-functions/non-keywords/non-vectors are just treated as constants
[[:from :A] [:where true]]

;; bare keywords are tested against the row `(:foo row)`
[[:from :A] [:where :foo]]

;; expressions can nest 
[[:from :A] [:where [= 42 [inc [dec :foo]]]]]

;; special conditional forms are provided
[[:from :A] [:select [:msg [:if [= 42 :foo] "the answer" "not the answer"]]]]
[[:from :A] [:where [:and :foo :bar]]]
[[:from :A] [:where [:or :foo :bar]]]

;; you can get the row with :%
[[:from :A] [= 10 [count :%]]]

;; you can reference non-keyword keys with ::rel/get
[[:from :A] [:where [::rel/get "foo"]]]

;; you can escape vectors / keywords with :_
[[:from :A] [:where [= [:_ :bar] :foo]]]

;; you can reference the env (see env.md)
[[:from :A] [:where [= :foo [::rel/env :foo]]]]

;; you can issue sub queries with :$1 (first row) and :$ (all rows) (see sub-queries.md)
[[:from :A] [:where [:$1 :B {:a-id :a-id}]]]

;; nil safe calls with :?
[[:from :A] [:where [:? str/includes? :foo :bar]]]

;; expressions have extra exception handling logic, if you cannot afford this, use the unsafe modifier.
[[:from :A] [:where [:! str/includes? :foo :bar]]]
```