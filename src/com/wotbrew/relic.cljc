(ns ^{:author "Dan Stone <wotbrew@gmail.com>"}
  com.wotbrew.relic
  "Functional relational programming for clojure.

  Quick hints:

  a relic database is a map.

  - put data in with `transact`, see also `what-if`.
  - get data out with `q`, see also `index`.
  - go faster maybe with `materialize`, see also `dematerialize`
  - track changes with `track-transact`, `watch` and `unwatch`.

  I like to alias :as rel. @wotbrew"
  (:require [com.wotbrew.relic.impl.dataflow :as dataflow])
  (:refer-clojure :exclude [min max]))

(defn transact
  "Return a new relic database, with the transaction applied. Will throw if any constraints are violated at the end of the
  transaction.

  Accepts transactional commands as args (tx)

  You modify relic databases by submitting commands to tables, e.g [:insert :Customer row1, row2].

  Commands:

  Insert with :insert vectors
  [:insert table row1, row2 ..]

  Insert or replace (insert or on :unique conflict update by deleting colliding rows and inserting the new ones).
  [:insert-or-replace table row1, row2 ..]

  Insert or merge (insert or on :unique conflict update by merging the new row with the old).
  ;; merge all keys from new row
  [:insert-or-merge :Customer :* customer1, :customer2]
  ;; merge a subset
  [:insert-or-merge :Customer [:firstname, :lastname] customer1, customer2]

  Insert or update (insert of on :unique conflict update by updating colliding rows using an update fn-or-map).
  ;; SQL style updates {col expr}
  [:insert-or-update :Customer {:ts now, :updates inc} customer1, customer2]
  ;; like :update, you can use a function of a row as an update
  [:insert-or-update :Customer update-fn customer1, customer2]

  Insert or ignore on :unique conflict
  [:insert-ignore :Customer customer1, customer2 ...]

  Delete rows (exact match) (faster)
  [:delete-exact table rows]

  Delete by predicates with :delete vectors (slower)
  [:delete table expr1 expr2 ..]
  e.g [:delete Customer [< :age 42]]

  Update rows with :update vectors.
  [:update table fn-or-map expr1 expr2 .. ]
  e.g [:update Customer {:age inc} [< :age 42]]

  You can use a map as terser multi table insert form:
  {table [row1, row2 ...], ...}

  ---

  Note:
  As relic stores its state and dataflow graph in metadata, all modifications to the database must be made using
  relic transact/track-transact - all bets are off otherwise.

  --

  See also track-transact, what-if."
  [db & tx]
  (dataflow/transact db tx))

;; --
;; query is based of index lookup
;; in certain cases, e.g for :hash or :btree
;; it might be useful for library users to have raw index access.

(defn index
  "Returns the raw index storing rows for the query. ONLY RETURNS IF THE QUERY IS MATERIALIZED.

  Normally a set, but if the last operation in the query is an index operation, you will get a specialised
  data structure, this can form the basis of using materialized relic indexes in other high-performance work on your data.

  :hash will yield nested maps (path being the expressions in the hash e.g [:hash :a :b :c])
  will yield an index {(a ?row) {(b ?row) {(:c ?row) #{?row}}}

  :btree is the same as hash but gives you a sorted map (sorted at each level) instead to enable range queries.

  :unique will give you an index where the keys map to exactly one row, so [:unique :a :b :c]
  will yield an index {(a ?row) {(b ?row) {(:c ?row) ?row}}}"
  [db query]
  (dataflow/index db query))

(defn materialize
  "Causes relic to maintain the given queries incrementally as the database changes.

  This will make queries effectively free at the cost of decreased write performance.

  Additionally, useful to start maintaining constraints by using queries that throw if invariants are broken.

  e.g (materialize db [[:from Customer] [:unique :email]])

  Constraint quick reference:

   [:req & cols]

     Required key checks

   [:check & check-pred]

     Throws if rows do not meet the predicates.
     Accepts predicates of the row (relic expressions / functions) or a map containing :pred and :error, both being relic expressions.

     e.g:
     [:check [< :age 32]] would require all :age values be under 32.

     The same predicate with a custom error:
     [:check {:pred [< :age 32], :error [str \"invalid age, got\" :age ]}]

   [:fk query|table clause opts]

     Foreign key, e.g ensure a row exists in the target relation given a join clause.

     e.g [:fk Customer {:id :id} {:cascade true}]

     Can use the option :cascade to specify cascading deletes, e.g {:cascade :delete}.

   [:unique & exprs]

     Unique constraint, ensures that only one row exists for some combination of relic expressions (e.g columns)

     e.g [:unique :id] would make sure only one row exists for a given :id value.

     Allows the use of :insert-or-replace in transact calls.

   [:constrain & constraint-operations]

    Lets you combine multiple constraints in one operation.

    e.g [[:from Customer] [:constrain [:req :id :firstname] [:unique :id]]"
  [db & queries]
  (reduce dataflow/materialize db queries))

(defn dematerialize
  "Dematerializes the query, increasing write performance at the cost of reduced query performance.

  Can also be used to remove constraints (e.g queries that throw).

  Note: queries that are being watched with (watch) will continue to be materialized until (unwatch) is called."
  [db & queries]
  (reduce dataflow/dematerialize db queries))

(defn q
  "Queries the db, returns a seq of rows by default.

  Takes a RQL query vector, or a map form [1].

  Queries are relational expressions, they describe some data that you might want.

  RQL queries are represented as vectors of operations.

   e.g [op1, op2, op3]

  Each operation is also a vector, a complete query would look like:

  [[:from :Customer]
   [:where [= :name \"alice\"]]]

  Operators quick guide:

  [:where & expr]
  [:extend & [col|[& col] expr]]
  [:expand & [col expr]]
  [:agg [& group-col] & [col agg-expr]]
  [:join query {left-col right-col, ...}]
  [:left-join query {left-col right-col, ...}]
  [:from query]
  [:without & col]
  [:select & col|[col|[& col] expr]]
  [:difference query]
  [:union query]
  [:intersection query]
  [:qualify namespace-string]
  [:rename {existing-col new-col, ...}]
  [:const collection-of-rows]

  ---

   Transducing:

   If you want a different collection back, you can apply a transducer to the relations rows with :xf
   e.g :xf (map :a) will instead of returning you a collection of rows, will return a collection of (:a row)

  ---

  Sorting:

   Sort with :sort / :rsort

   Pass either a relic expr (e.g a column keyword or function, or relic vector), or coll of expressions to sort by those expressions.
   e.g :sort :a == sort by :a
       :sort [:a :b] == sort by :a then :b
       :sort [[inc a]] == sort by (inc (:a row))

   Note: indexes are not used yet for ad-hoc sorts, but you can use rel/index and :btree for that if you are brave.

  ---

  [1] map forms can be used to issue multiple queries at once, this allows relic to share indexes and intermediate structures
  and can be more efficient.

    {key query|{:q query, :rsort ...}}"
  ([db query-or-binds]
   (if (map? query-or-binds)
     (let [queries (keep (fn [q] (if (map? q) (:q q) q)) (vals query-or-binds))
           missing (remove #(dataflow/materialized? db %) queries)
           db (reduce dataflow/materialize db missing)]
       (reduce-kv
         (fn [m k qr]
           (if (map? q)
             (assoc m k (q db (:q qr) qr))
             (assoc m k (q db qr))))
         {}
         query-or-binds))
     (dataflow/q db query-or-binds)))
  ([db query opts]
   (let [{:keys [sort
                 rsort
                 xf]
          into-coll :into} opts

         sort* (or sort rsort)
         sort-exprs (if (keyword? sort*) [sort*] sort*)
         sort-fns (mapv dataflow/row-fn sort-exprs)
         rs (dataflow/qraw db query)
         sort-fn (when (seq sort-fns)
                   (if (= 1 (count sort-fns))
                     (first sort-fns)
                     (apply juxt sort-fns)))

         rs (cond
              sort (sort-by sort-fn rs)
              rsort (sort-by sort-fn (fn [a b] (compare b a)) rs)
              :else rs)

         rs (cond
              into-coll (if xf (into into-coll xf rs) (into into-coll rs))
              xf (sequence xf rs)
              :else (seq rs))]
     rs)))

(defn what-if
  "Returns the relation for query if you were to apply the transactions with transact.
  Because databases are immutable, it's not hard to do this anyway with q & transact. This is just sugar."
  [db query & tx]
  (q (dataflow/transact db tx) query))

;; --
;; change tracking api

(defn watch
  "Establishes watches on the queries, watched queries are change tracked for subsequent transactions
  such that track-transact will return changes to those queries in its results.

  Returns a new database.

  See track-transact.

  Remove watches with unwatch."
  [db & queries]
  (reduce dataflow/watch db queries))

(defn unwatch
  "Removes a watched query, changes for that query will no longer be tracked.

  Potentially dematerializes the query if it was only materialized to maintain the watch.

  See track-transact."
  [db & queries]
  (reduce dataflow/unwatch db queries))

(defn track-transact
  "Like transact, but instead of returning you a database, returns a map of

    :result the result of (apply transact db tx)
    :changes a map of {query {:added [row1, row2 ...], :deleted [row1, row2, ..]}, ..}

  The :changes allow you to react to additions/removals from derived queries, and build reactive systems."
  [db & tx]
  (dataflow/track-transact db tx))

;; --
;; query analysis

(defn dependencies
  "Returns the (table name) dependencies of the query, e.g what tables it could be affected by."
  [query]
  (dataflow/dependencies query))

;; --
;; min/max

(defn max-by
  "A relic agg function that returns the greatest row by some function. e.g [rel/max-by :a] will return the row for which :a is biggest."
  [expr]
  (dataflow/max-by expr))

(defn min-by
  "A relic agg function that returns the smallest row by some function. e.g [rel/min-by :a] will return the row for which :a is smallest."
  [expr]
  (dataflow/min-by expr))

(defn max
  "A relic agg function that returns the greatest value for the expression as applied to each row."
  [expr]
  (dataflow/max-agg expr))

(defn min
  "A relic agg function that returns the smallest value for the expression as applied to each row."
  [expr]
  (dataflow/min-agg expr))

;; --
;; sum

(def ^:private sum-add-fn #?(:clj +' :cljs +))

(defn sum
  "A relic agg function that returns the sum of the expressions across each row.

  e.g [rel/sum :a] will return the sum of (:a row) applied to each row in the aggregation."
  [& exprs]
  (case (count exprs)
    0 {:combiner (constantly 0) :reducer (constantly 0)}
    1
    (let [expr (first exprs)
          f (dataflow/row-fn expr)
          xf (keep f)]
      {:combiner sum-add-fn
       :reducer #(transduce xf sum-add-fn %)})
    (let [fns (map dataflow/row-fn exprs)
          nums (apply juxt fns)
          xf (comp (mapcat nums) (remove nil?))]
      {:combiner sum-add-fn
       :reducer #(transduce xf sum-add-fn %)})))

;; --
;; set-concat

(defn set-concat [expr]
  (let [f (dataflow/row-fn expr)]
    {:custom-node (fn [left cols [binding]]
                    (conj left
                          [dataflow/group cols f]
                          [dataflow/transform-unsafe (dataflow/bind-group binding identity)]))}))

;; --
;; count-distinct

(defn count-distinct [& exprs]
  (let [expr (if (= 1 (count exprs)) (first exprs) (into [vector] exprs))
        f (dataflow/row-fn expr)]
    {:custom-node (fn [left cols [binding]]
                    (conj left
                          [dataflow/group cols f]
                          [dataflow/transform-unsafe (dataflow/bind-group binding count)]))}))


;; --
;; avg

(defn avg
  "An aggregation that returns the statistical average, uses `/` so return type depends on operands,

  You might get a Ratio, like clojure."
  [expr]
  {:custom-node
   (fn [left cols [binding]]
     (conj left
           [:agg cols [:s [sum expr]] [:n count]]
           (into [:select-unsafe [binding [/ :s [:if [pos? :n] :n 1]]]] cols)))})

;; --
;; any, like some but this one is called any.

(defn any
  "An aggregate function that binds true if any row has a truthy value of expr, false if not."
  [expr]
  (let [f (dataflow/row-fn expr)]
    {:reducer #(some f %)
     :combiner #(and %1 %2)
     :complete boolean}))

(defn not-any
  "An aggregate function that binds false if any row has a truthy value of expr, true if not."
  [expr]
  (let [f (dataflow/row-fn expr)]
    {:reducer #(not-any? f %)
     :combiner #(and %1 %2)
     :complete boolean}))

;; --
;; top / bottom

(defn top-by
  "An aggregate function that binds the n rows with the highest values for expr across the group.

  Materialization can be slow if the n parameter is large, use for small summaries."
  [n expr]
  (assert (nat-int? n) "top requires a 0 or positive integer arg first")
  (let [f (dataflow/row-fn expr)]
    {:custom-node
     (fn [left cols [binding]]
       (conj left
             [dataflow/sorted-group cols f]
             [dataflow/transform-unsafe
              (dataflow/bind-group binding #(into [] (comp (mapcat val) (take n)) (rseq %)))]))}))

(defn bottom-by
  "An aggregate function that binds the n rows with the lowest values for expr across the group.

  Materialization can be slow if the n parameter is large, use for small summaries."
  [n expr]
  (assert (nat-int? n) "bottom requires a 0 or positive integer arg first")
  (let [f (dataflow/row-fn expr)]
    {:custom-node
     (fn [left cols [binding]]
       (conj left
             [dataflow/sorted-group cols f]
             [dataflow/transform-unsafe
              (dataflow/bind-group binding #(into [] (comp (mapcat val) (take n)) (seq %)))]))}))

(defn top
  "An aggregate function that binds the highest n values for the expr across the group.

  Materialization can be slow if the n parameter is large, use for small summaries."
  [n expr]
  (assert (nat-int? n) "top requires a 0 or positive integer arg first")
  (let [f (dataflow/row-fn expr)]
    {:custom-node
     (fn [left cols [binding]]
       (conj left
             [dataflow/sorted-group cols f]
             [dataflow/transform-unsafe
              (dataflow/bind-group binding #(into [] (comp (map key) (take n)) (rseq %)))]))}))

(defn bottom
  "An aggregate function that binds the lowest n values for the expr across the group.

  Materialization can be slow if the n parameter is large, use for small summaries."
  [n expr]
  (assert (nat-int? n) "bottom requires a 0 or positive integer arg first")
  (let [f (dataflow/row-fn expr)]
    {:custom-node
     (fn [left cols [binding]]
       (conj left
             [dataflow/sorted-group cols f]
             [dataflow/transform-unsafe
              (dataflow/bind-group binding #(into [] (comp (map key) (take n)) (seq %)))]))}))

;; --
;; env api

(defn get-env [db] (first (q db ::dataflow/Env)))

(defn set-env-tx [env]
  [:replace-all ::dataflow/Env {::dataflow/env env}])

(defn with-env [db env] (transact db (set-env-tx env)))

(defn update-env [db f & args] (with-env db (apply f (get-env db) args)))

;; --
;; functions for going back and forth between 'normal maps' and relic

(defn strip-meta
  "Given a relic database map, removes any relic meta data."
  [db]
  (vary-meta db (comp not-empty dissoc) ::dataflow/graph))