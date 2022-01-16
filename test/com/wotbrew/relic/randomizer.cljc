(ns com.wotbrew.relic.randomizer
  "poor mans testing tool, helped find bugs with tpch suite. Can probably port to test-check by generating
  random sorts / slice and dicing input data along with standard mat/watch/hint."
  (:require [com.wotbrew.relic :as rel]
            [clojure.set :as set]))

(defn- random-mat [model]
  (let [hint-or-mat (rand-nth (cond-> [:query
                                       :query-part]
                                      (seq (:hints model)) (conj :hint)))]
    (case hint-or-mat
      :hint (rand-nth (:hints model))
      :query (:q (rand-nth (:queries model)))
      :query-part (let [q (:q (rand-nth (:queries model)))
                        n (rand-int (count q))
                        [mat] (split-at n q)]
                    (if (seq mat)
                      (vec mat)
                      q)))))

(defn- mat-mutation [model db data]
  (let [mat (random-mat model)]
    {:mat mat}))

(defn- demat-mutation [model db data]
  (let [mat (random-mat model)]
    {:demat mat}))

(defn- deliver-more-rows [model db data]
  (let [[table d] (rand-nth (seq data))
        d (shuffle d)
        d (take (rand-int (count d)) d)]
    {:insert {table d}}))

(defn- mutseq
  [n]
  (let [muts [mat-mutation
              demat-mutation
              deliver-more-rows]]
    (repeatedly n #(rand-nth muts))))

(defn- domut [db data {:keys [mat demat insert]}]
  (cond-> [db data]
          mat (update 0 rel/mat mat)
          demat (update 0 rel/mat demat)
          insert ((fn [[db data]]
                    (let [db (rel/transact db insert)
                          data (reduce-kv #(update %1 %2 set/difference (set %3)) data insert)]
                      [db data])))))

(def last-muts (atom []))

(defn mutdb [model n data]
  (let [data (reduce #(update %1 %2 set) data (keys data))
        db {}
        mseq (mutseq n)
        _ (reset! last-muts [])
        [db data] (reduce
                    (fn [[db data] m]
                      (let [m (m model db data)]
                        (swap! last-muts conj m)
                        (domut db data m)))
                    [db data]
                    mseq)]
    (rel/transact db data)))