(ns com.wotbrew.relic.analyze
  (:require [com.wotbrew.relic.dataflow :as dataflow]))

(defmulti columns* (fn [_ stmt] (dataflow/operator stmt)))

(defmethod columns* :default [_ _] [])

(defn full-columns [relvar]
  (if (empty? relvar)
    []
    (let [stmt (dataflow/head-stmt relvar)
          left (dataflow/left-relvar relvar)]
      (columns* left stmt))))

(defn full-dependencies [relvar]
  (if-some [table (dataflow/unwrap-table relvar)]
    #{table}
    (let [left (dataflow/left-relvar relvar)
          stmt (dataflow/head-stmt relvar)
          {:keys [deps]} (dataflow/to-dataflow left stmt)]
      (set (mapcat full-dependencies deps)))))

(defn dependencies
  "Returns the (table name) dependencies of the relvar, e.g what tables it could be affected by."
  [relvar]
  (distinct (map #(nth % 1) (full-dependencies relvar))))

(defn columns
  "Returns the (known) columns on the relvar, e.g what it might return."
  [relvar]
  (map :k (full-columns relvar)))