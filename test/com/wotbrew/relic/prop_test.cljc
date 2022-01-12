(ns com.wotbrew.relic.prop-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.wotbrew.relic :as rel]
            [clojure.test.check :as tc]
            [com.wotbrew.relic.impl.util :as u]))

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
    {table data}))

(defn mut [model]
  (gen/one-of
    [(gen/tuple (gen/return :mat) (mat model))
     (gen/tuple (gen/return :demat) (mat model))
     (gen/tuple (gen/return :watch) (mat model))
     (gen/tuple (gen/return :unwatch) (mat model))
     (gen/tuple (gen/return :deliver-data) (data-slice model))]))

(defn mutseq [model]
  (gen/vector (mut model)))

(defn- domut [db [t payload]]
  (case t
    :mat (rel/materialize db payload)
    :demat (rel/dematerialize db payload)
    :watch (rel/watch db payload)
    :unwatch (rel/unwatch db payload)
    :deliver-data (rel/transact db payload)))

(defn- domuts [db muts]
  (reduce domut db muts))

(defn- muttx [muts]
  (reduce (fn [m [t d]]
            (if (= t :deliver-data)
              (merge-with (fn [a b] (into (set a) (set b))) m d)
              m)) {} muts))

(defn hinted-db-always-yields-same-result-as-non-hinted
  [model]
  (prop/for-all [q (gen/elements (:queries model))
                 muts (mutseq model)]
    ;; no sort check for now, even sort has only a partially deterministic order
    ;;
    (= (sort-by hash (rel/what-if {} q (muttx muts)))
       (sort-by hash (rel/q (domuts {} muts) q)))))

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

(defn- qc [model p]
  (println "QC:" (:name model "???") "|" (u/best-effort-fn-name p))
  (let [num-tests 10000
        ret (tc/quick-check num-tests (p model) :max-size 32)]
    ret))

(deftest props-test
  (doseq [model [model1 model2]
          prop [hinted-db-always-yields-same-result-as-non-hinted]
          :let [res (qc model prop)]]
    (when-not (:pass? res)
      (println "FAIL seed:" (:seed res)))
    (is (:pass? res))))