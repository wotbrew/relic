(ns com.wotbrew.relic.prop-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.wotbrew.relic :as rel]
            [clojure.test.check :as tc]
            [com.wotbrew.relic.impl.util :as u]
            [clojure.set :as set]
            [com.wotbrew.relic.impl.dataflow :as dataflow]))

(defn- available-mat-types [model]
  (cond->
    []
    (seq (:hints model)) (conj :hint)
    (seq (:queries model)) (conj :query :query-part)))

(defn mat [model]
  (let [mut-types (available-mat-types model)]
    (assert (seq mut-types) "model must have at least one :query")
    (gen/bind
      (gen/elements mut-types)
      (fn [mt]
        (case mt
          :hint (gen/elements (:hints model))
          :query (gen/elements (:queries model))
          :query-part (gen/bind
                        (gen/elements (:queries model))
                        (fn [q]
                          (if (keyword? q)
                            (gen/return q)
                            (let [ngen (gen/choose 0 (count q))]
                              (gen/fmap (fn [n]
                                          (let [[mat] (split-at n q)]
                                            (if (seq mat)
                                              (vec mat)
                                              q))) ngen))))))))))

(defn rowgen [table]
  (apply gen/hash-map (for [[k v] table
                            kvs [k
                                 (case v
                                   :integer gen/large-integer)]]
                        kvs)))

(defn data-slice [model]
  (gen/let [table (gen/elements (keys (:tables model)))
            data (gen/vector (rowgen (get (:tables model) table)))]
    (into [:insert table] data)))

(defn data-delete [model]
  (gen/let [table (gen/elements (keys (:tables model)))
            data (gen/vector (rowgen (get (:tables model) table)))]
    (into [:delete-exact table] data)))

(defn mut [model]
  (letfn [(mg [t payload] (gen/tuple (gen/return t) payload))]
    (gen/one-of
      [(mg :mat (mat model))
       (mg :demat (mat model))
       (mg :watch (mat model))
       (mg :unwatch (mat model))
       (mg :insert-data (data-slice model))
       (mg :delete-data (data-delete model))])))

(defn mutseq [model]
  (gen/vector (mut model)))

(defn- domut [db [t payload]]
  (case t
    :mat (rel/materialize db payload)
    :demat (rel/dematerialize db payload)
    :watch (rel/watch db payload)
    :unwatch (rel/unwatch db payload)
    :insert-data (rel/transact db payload)
    :delete-data (rel/transact db payload)))

(defn- domuts [db muts]
  (reduce domut db muts))

(defn- muttx [muts]
  (reduce (fn [m [t payload]]
            (case t
              :insert-data
              (let [[_ t & rows] payload] (update m t #(set/union (set %1) (set rows))))
              :delete-data
              (let [[_ t & rows] payload] (update m t #(set/difference (set %1) (set rows))))
              m)) {} muts))

(defn hinted-db-always-yields-same-result-as-non-hinted
  [model]
  (prop/for-all [q (gen/elements (:queries model))
                 muts (mutseq model)]
    ;; no sort check for now, even sort has only a partially deterministic order
    ;;
    (= (sort-by hash (rel/what-if {} q (muttx muts)))
       (sort-by hash (rel/q (domuts {} muts) q)))))

(defn- delete-all [db] (reduce-kv (fn [db t rs] (rel/transact db (into [:delete-exact t] rs))) db db))

(defn delete-all-leaves-all-memory-and-queries-empty-unless-const
  [model]
  (prop/for-all [q (gen/elements (:queries model))
                 muts (mutseq model)]
    (let [db (-> {} (domuts muts) delete-all)
          graph (dataflow/gg db)
          nodemap (::dataflow/ids graph)
          const? (fn [relvar] (boolean (seq (rel/q {} relvar))))]
      (if (const? q)
        true
        (and (empty? (rel/q db q))
             (reduce-kv
               (fn [_ relvar id]
                 (if (or (and (empty? (rel/q db relvar))
                              (contains? graph id)
                              (empty? (::mem (graph id))))
                         (const? relvar))
                   true
                   (reduced false)))
               true nodemap))))))

(def model1
  "A basic model with one table and some basic agg queries that will
  demonstrate multi-agg joins."
  {:name "model1"
   :tables {:a {:a :integer
                :b :integer}}
   :hints [[[:from :a] [:hash :b]]
           [[:from :a] [:hash :a :b]]
           [[:from :a] [:btree :b :a]]]
   :queries [:a
             [[:from :a]]

             [[:from :a]
              [:agg [:b]
               [:asum [rel/sum :a]]
               [:aavg [rel/avg :a]]]]

             [[:from :a]
              [:where [< :b 100]]
              [:agg [:b]
               [:act [rel/count-distinct :a]]
               [:asum [rel/sum :a]]
               [:aavg [rel/avg :a]]]]]})

(def model2
  {:name "model2"
   :tables {:a {:a :integer
                :b :integer}
            :b {:b :integer
                :c :integer}
            :c {:b :integer
                :c :integer}}
   :hints [[[:from :c] [:btree :c]]
           [[:from :b] [:hash :c :b]]
           [[:from :a] [:hash :b]]]
   :queries [:a
             :b
             [[:from :a]]
             [[:from :b]]
             [[:from :c]]
             [[:from :a]
              [:extend [:br [rel/sel :b {:b :b}]]]]
             [[:from :a]
              [:extend [:br [rel/sel :b {:b :b}]]]
              [:select [:a :c]]]
             [[:from :a]
              [:join :b {:a :b}]
              [:extend
               [:br [rel/sel1 :b {:a :b}]]
               [:br2 [rel/sel1 :b {:b :c}]]]
              [:select :a [:x [:c :br2]] [:y [:c :br1]]]]
             [[:from :a]
              [:join :c {:b :b} :b {:c :c}]
              [:agg [:c] [:bsum [rel/sum :b]]]]
             [[:from :a]
              [:where [even? :b]]
              [:join :b {:b :b} :c {:b :b}]
              [:agg [:c] [:bsum [rel/sum :b]]]]]})

(def model3
  {:name "model3"
   :tables {:a {:a :integer
                :b :integer}
            :b {:b :integer
                :c :integer}
            :c {:b :integer
                :c :integer}}
   :hints [[[:from :c] [:btree :c]]
           [[:from :b] [:hash :c :b]]
           [[:from :a] [:hash :b]]]
   :queries [:a
             :b
             [[:union :a :b]]
             [[:union :a]]
             [[:union :b]]
             [[:union :c]]
             [[:union :a :b :c]]
             [[:intersection :a :b :c]]
             [[:difference :a :b :c]]
             [[:union :c :a]]
             [[:union :c :b]]
             [[:union :b :a]]
             [[:union :c [[:from :a] [:join :b {:b :b}]]]]

             [[:difference
               [[:from :a] [:select :a]]
               [[:from :b] [:select :b]]]]

             [[:intersection
               [[:from :a] [:select :a]]
               [[:from :b] [:select :b]]]]

             [[:from :b]
              [:select :c]
              [:intersection [[:from :c] [:select :c]]]]

             [[:intersection :a [[:select [:a 0]]]]]]})

(def model4
  {:name "model4"
   :tables {:a {:a :integer, :b :integer}
            :b {:b :integer, :c :integer}}
   :hints [[[:from :a] [:btree :b]]]
   :queries [[[:from :a]
              [:agg [:a] [:ntop [rel/top 5 :b]]]]
             [[:from :b]
              [:join :a {:b :b}]
              [:agg [] [:nbotrows [rel/bottom-by 5 :a]]]]
             [[:from :a]
              [:agg []
               [:amax [max :a]]
               [:abot [rel/bottom 32 :a]]
               [:bmin [max [inc :b]]]
               [:bmaxinvr [rel/max-by [- :b]]]]]]})

(defn- qc [model p]
  (println "QC:" (:name model "???") "|" (u/best-effort-fn-name p))
  (let [num-tests 10000
        ret (tc/quick-check num-tests (p model) :max-size 32)]
    ret))

(deftest props-test
  (doseq [model [model1
                 model2
                 model3
                 model4]
          prop [hinted-db-always-yields-same-result-as-non-hinted
                delete-all-leaves-all-memory-and-queries-empty-unless-const]
          :let [res (qc model prop)]]
    (when-not (:pass? res)
      (println "FAIL seed:" (:seed res)))
    (is (:pass? res))))

(deftest regression-union1-test
  (let [q [[:union :a :b :c]]
        muts [[:insert-data [:insert :b {:b 1, :c 0}]]
              [:mat [[:union :a :b :c]]]
              [:delete-data [:delete-exact :c {:b 1, :c 0}]]]
        db1 (domuts {} muts)
        db2 (rel/transact {} (muttx muts))]
    (is (= (rel/q db1 q) (rel/q db2 q)))))