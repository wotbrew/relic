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
  (:require [com.wotbrew.relic.analyze :as analyze]
            [com.wotbrew.relic.dataflow :as dataflow]))

(defn transact
  "Return a new relic database, with the transaction applied.

  Accepts transactional commands as args (tx)

  You modify relic databases by submitting commands to tables, a table can be referenced by name, or by table relvar.

  Commands:

  Insert with :insert vectors
  [:insert table row1, row2 ..]

  Delete by predicates with :delete vectors
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
  "Returns the raw index storing rows for relvar.

  Normally a set, but if the last statement in the relvar is an index statement, you will get a specialised
  datastructure, this can form the bases of using materialized relic indexes in other high-performance work on your data.

  :hash will yield nested maps (path being the expressions in the hash e.g [:hash :a :b :c])
  will yield an index {(a ?row) {(b ?row) {(:c ?row) #{?row}}}

  :btree is the same as hash but gives you a sorted map instead.

  :unique will give you an index where the keys map to exactly one row, so [:unique :a :b :c]
  will yield an index {(a ?row) {(b ?row) {(:c ?row) ?row}}}"
  [db relvar]
  (let [graph (dataflow/gg db)
        id (dataflow/get-id graph relvar)
        {:keys [mem]} (graph id)]
    mem))

(defn materialize [db & relvars]
  (reduce dataflow/materialize db relvars))

(defn dematerialize [db & relvars]
  (reduce dataflow/dematerialize db relvars))

(defn q
  "Queries relic for a collection of unique rows (relation).

  Takes a relvar, or a map form [1].

  Relvars are relational expressions, they describe some data that you might want.

  Think SQL table, View & Query rolled up into one idea.

  They are modelled as vectors of statements.

   e.g [stmt1, stmt2, stmt3]

  Each statement is also a vector, a complete relvar would look like:

  [[:table :Customer]
   [:where [= :name \"alice\"]]]

  Operators quick guide:

  [:table name]
  [:where & expr]
  [:extend & [col|[& col] expr]]
  [:expand & [col expr]]
  [:agg [& group-col] & [col agg-expr]]
  [:join relvar {left-col right-col, ...}]
  [:left-join relvar {left-col right-col, ...}]
  [:from relvar]
  [:without & col]
  [:select & col|[col|[& col] expr]]
  [:difference relvar]
  [:union relvar]
  [:intersection relvar]
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

    {key relvar|{:q relvar, :rsort ...}}

  Note: Expect only that the result is seqable/reducable, custom Relation type might follow in a later release"
  ([db relvar-or-binds]
   (if (map? relvar-or-binds)
     (let [relvars (keep (fn [q] (if (map? q) (:q q) q)) (vals relvar-or-binds))
           missing (remove #(dataflow/materialized? db %) relvars)
           db (reduce dataflow/materialize db missing)]
       (reduce-kv
         (fn [m k qr]
           (if (map? q)
             (assoc m k (q db (:q qr) qr))
             (assoc m k (q db qr))))
         {}
         relvar-or-binds))
     (dataflow/q db relvar-or-binds)))
  ([db relvar opts]
   (let [{:keys [sort
                 rsort
                 xf]
          into-coll :into} opts

         sort* (or sort rsort)
         sort-exprs (if (keyword? sort*) [sort*] sort*)
         sort-fns (mapv dataflow/row-fn sort-exprs)
         rs (dataflow/q db relvar)
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
              :else rs)]
     rs)))

(defn what-if
  "Returns the relation for relvar if you were to apply the transactions with transact.
  Because databases are immutable, its not hard to do this anyway with q & transact. This is just sugar."
  [db relvar & tx]
  (q (dataflow/transact db tx) relvar))

;; --
;; change tracking api

(defn watch
  "Establishes watches on the relvars, watched relvars are change tracked for subsequent transactions
  such that track-transact will return changes to those relvars in its results.

  Returns a new database.

  See track-transact."
  [db & relvars]
  (reduce dataflow/watch db relvars))

(defn unwatch
  "Removes a watched relvar, changes for that relvar will no longer be tracked.

  See track-transact."
  [db & relvars]
  (reduce dataflow/unwatch db relvars))

(defn track-transact
  "Like transact, but instead of returning you a database, returns a map of

    :result the result of (apply transact db tx)
    :changes a map of {relvar {:added [row1, row2 ...], :deleted [row1, row2, ..]}, ..}

  The :changes allow you to react to additions/removals from derived relvars, and build reactive systems."
  [db & tx]
  (dataflow/track-transact db tx))

;; --
;; relvar analysis
;; analysis of relvars will get better later... be patient :)

(defn dependencies
  "Returns the (table name) dependencies of the relvar, e.g what tables it could be affected by."
  [relvar]
  (analyze/dependencies relvar))

(defn columns
  "Returns the (known) columns on the relvar, e.g what it might return."
  [relvar]
  (analyze/columns relvar))

;; --
;; greatest / least

(defn greatest-by
  "A relic agg function that returns the greatest row by some function. e.g [rel/greatest-by :a] will return the row for which :a is biggest."
  [expr]
  (let [f (dataflow/row-fn expr)
        rf (fn
             ([] nil)
             ([a] a)
             ([a b]
              (cond
                (nil? a) b
                (nil? b) a
                :else
                (let [n (compare (f a) (f b))]
                  (if (< n 0)
                    b
                    a)))))]
    {:combiner rf
     :reducer (fn [rows] (reduce rf rows))}))

(defn least-by
  "A relic agg function that returns the smallest row by some function. e.g [rel/least-by :a] will return the row for which :a is smallest."
  [expr]
  (let [f (dataflow/row-fn expr)
        rf (fn
             ([] nil)
             ([a] a)
             ([a b]
              (cond
                (nil? a) b
                (nil? b) a
                :else
                (let [n (compare (f a) (f b))]
                  (if (< n 0)
                    a
                    b)))))]
    {:combiner rf
     :reducer (fn [rows] (reduce rf rows))}))

(defn- comp-complete [aggregate-map f]
  (if-some [f2 (:complete aggregate-map)]
    (assoc aggregate-map :complete (comp f f2))
    (assoc aggregate-map :complete f)))

(defn greatest
  "A relic agg function that returns the greatest value for the expression as applied to each row."
  [expr]
  (comp-complete (greatest-by expr) (dataflow/row-fn expr)))

(defn least
  "A relic agg function that returns the smallest value for the expression as applied to each row."
  [expr]
  (comp-complete (least-by expr) (dataflow/row-fn expr)))

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
;; any, like some but this one is called any.

(defn any [expr]
  (let [f (dataflow/row-fn expr)]
    {:reducer #(some f %)
     :combiner #(and %1 %2)
     :complete boolean}))

(defn not-any [expr]
  (let [f (dataflow/row-fn expr)]
    {:reducer #(not-any? f %)
     :combiner #(and %1 %2)
     :complete boolean}))

;; --
;; top / bottom

(defn top-by [n expr]
  (assert (nat-int? n) "top requires a 0 or positive integer arg first")
  (let [f (dataflow/row-fn expr)]
    {:custom-node
     (fn [left cols [binding]]
       (conj left
             [dataflow/sorted-group cols f]
             [dataflow/transform-unsafe
              (dataflow/bind-group binding #(into [] (comp (mapcat val) (take n)) (rseq %)))]))}))

(defn bottom-by [n expr]
  (assert (nat-int? n) "bottom requires a 0 or positive integer arg first")
  (let [f (dataflow/row-fn expr)]
    {:custom-node
     (fn [left cols [binding]]
       (conj left
             [dataflow/sorted-group cols f]
             [dataflow/transform-unsafe
              (dataflow/bind-group binding #(into [] (comp (mapcat val) (take n)) (seq %)))]))}))

(defn top [n expr]
  (assert (nat-int? n) "top requires a 0 or positive integer arg first")
  (let [f (dataflow/row-fn expr)]
    {:custom-node
     (fn [left cols [binding]]
       (conj left
             [dataflow/sorted-group cols f]
             [dataflow/transform-unsafe
              (dataflow/bind-group binding #(into [] (comp (map key) (take n)) (rseq %)))]))}))

(defn bottom [n expr]
  (assert (nat-int? n) "bottom requires a 0 or positive integer arg first")
  (let [f (dataflow/row-fn expr)]
    {:custom-node
     (fn [left cols [binding]]
       (conj left
             [dataflow/sorted-group cols f]
             [dataflow/transform-unsafe
              (dataflow/bind-group binding #(into [] (comp (map key) (take n)) (seq %)))]))}))

;; --
;; agg expressions e.g [rel/sum :foo]


;; --
;; env api

(defn get-env [db] (first (q db ::dataflow/Env)))
(defn set-env-tx [env]
  (list [:delete ::dataflow/Env]
        [:insert ::dataflow/Env {::dataflow/env env}]))

(defn with-env [db env]
  (if (seq (db ::dataflow/Env))
    (transact db [:update ::dataflow/Env (constantly {::dataflow/env env})])
    (transact db (set-env-tx env))))
(defn update-env [db f & args] (with-env db (apply f (get-env db) args)))

;; --
;; functions for going back and forth between 'normal maps' and relic

(defn strip-meta
  "Given a relic database map, removes any relic meta data."
  [db]
  (vary-meta db (comp not-empty dissoc) ::dataflow/graph))

;; --
;; ed. probably best not to use right now.

(defn ed [relvar]
  #?(:clj ((requiring-resolve 'com.wotbrew.relic.ed/ed) relvar)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library!")))

(defn ed-transact [& tx]
  #?(:clj (apply (requiring-resolve 'com.wotbrew.relic.ed/global-transact!) tx)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library?!")))

(defn ed-set-env [env]
  #?(:clj ((requiring-resolve 'com.wotbrew.relic.ed/global-set-env!) env)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library?!")))