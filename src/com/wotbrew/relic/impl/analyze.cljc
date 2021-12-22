(ns ^:no-doc com.wotbrew.relic.impl.analyze
  (:require [com.wotbrew.relic.impl.dataflow :as dataflow]))

(defmulti columns* (fn [_ stmt] (dataflow/operator stmt)))

(defmethod columns* :default [_ _] [])

(defn full-columns [relvar]
  (if (empty? relvar)
    []
    (let [stmt (dataflow/head-stmt relvar)
          left (dataflow/left-relvar relvar)]
      (columns* left stmt))))

(defn full-dependencies [relvar]
  (if (empty? relvar)
    #{}
    (if-some [table (dataflow/unwrap-table relvar)]
      #{table}
      (let [{:keys [deps]} (dataflow/to-dataflow {} relvar)]
        (set (mapcat full-dependencies deps))))))

(defn dependencies
  "Returns the (table name) dependencies of the relvar, e.g what tables it could be affected by."
  [relvar]
  (distinct (map #(nth (first %) 1 nil) (full-dependencies relvar))))

(defn columns
  "Returns the (known) columns on the relvar, e.g what it might return."
  [relvar]
  (map :k (full-columns relvar)))