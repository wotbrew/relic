(ns com.wotbrew.tarpit
  "Implements functional relational programming as described in the out-of-the-tarpit paper.

  Modification primitives:
  [:insert rel match-rel]
  [:update rel overwrites match-rel]
  [:delete rel match-rel]"
  (:require [com.wotbrew.tarpit.interpreter :as interpreter]
            [com.wotbrew.tarpit.protocols :as p]
            [com.wotbrew.tarpit.fusion :as fusion]
            [clojure.core :as clj]
            [clojure.set :as set]
            [clojure.string :as str])
  (:refer-clojure :exclude [extend update])
  (:import (clojure.lang IPersistentSet IPersistentVector)))

(defn rel "Defines a base relation" [& keys] (into [:rel] keys))

(defn const [& rows] [:const (set rows)])
(defn dynamic [f] [:dynamic f])

(defn extend [rel & extensions] (fusion/extend rel extensions))

(defn project [rel & keys] (fusion/project rel keys))
(defn project-away [rel & keys] (fusion/project-away rel keys))
(defn restrict [rel & restrictions] (fusion/restrict rel restrictions))
(defn summarize [rel keys & agg] (into [:summarize rel keys] agg))
(defn join [rel1 rel2 km] (fusion/join rel1 rel2 km))
(defn union [rel1 rel2] [:union rel1 rel2])
(defn difference [rel1 rel2] [:difference rel1 rel2])
(defn intersection [rel1 rel2] [:intersection rel1 rel2])

(defn eq [value prop] [:eq value prop])
(defn unique-key [rel & keys] (into [:unique-key rel] keys))
(defn foreign-key [rel1 rel2 km] (into [:foreign-key rel1 rel2 km] keys))

(defn insert [rel row] [:insert rel row])
(defn update [rel set-map match-rel] [:update rel set-map match-rel])
(defn delete [rel match-rel] [:delete rel match-rel])

(defn q [st rel]
  (p/row-set rel st))

(defn modify [st modifications]
  (interpreter/modify st modifications))

(extend-protocol p/Rel
  IPersistentVector
  (rel-id [rel] rel)
  (row-set [rel st] (interpreter/q st rel)))

(defn- possible-variable? [x]
  (and (keyword? x)
       (if (namespace x)
         (not (str/starts-with? (namespace x) "_"))
         true)))

(defn unescape-keyword [x]
  (if (and (keyword? x) (namespace x))
    (if (str/starts-with? (namespace x) "_")
      (keyword (subs (namespace x) 0) (name x))
      x)
    x))

(defn- build-fn [get-variable f args]
  (let [vpattern (mapv (comp #{true 1 false 0} boolean get-variable) args)
        [a1 a2 :as args] (mapv unescape-keyword args)]
    (case vpattern
      [] f
      [1] (comp f a1)
      [0] (fn [_] (f a1))
      [0 0] (fn [_] (f a1 a2))
      [1 0] (fn [row] (f (a1 row) a2))
      [0 1] (fn [row] (f a1 (a2 row)))
      [1 1] (fn [row] (f (a1 row) (a2 row)))
      (let [variables (set (filter get-variable args))]
        (fn [row]
          (let [args (map #(if (variables %) (% row) %) args)]
            (apply f args)))))))

(extend-protocol p/Extension
  IPersistentVector
  (extension->fn [[k [f & args]] get-variable]
    (let [f (build-fn get-variable f args)]
      #(assoc % k (f %))))
  (extension-keys [[k]] #{k})
  (extension-deps [[_ [f & args]]] (set (filter possible-variable? args))))

(extend-protocol p/Restriction
  IPersistentVector
  (restriction->fn [[f & args] get-variable]
    (build-fn get-variable f args))
  (restriction-deps [[_ & args]] (set (filter possible-variable? args))))

(extend-protocol p/Table
  IPersistentSet
  (table-add [tbl rows] (set/union tbl (set rows)))
  (table-delete [tbl rows] (set/difference tbl (set rows))))

(defrecord State [schema]
  p/State
  (get-schema [st] schema)
  (materialized-row-set [st rel] (get st (p/rel-id rel) #{}))
  (add-rows [st rel rows] (clj/update st (p/rel-id rel) (fnil p/table-add #{}) rows))
  (del-rows [st rel rows] (clj/update st (p/rel-id rel) (fnil p/table-delete #{}) rows)))

(defn empty-state [schema] (->State schema))

(comment
  (let [Customer [:rel :customer-id :email :name]
        Order [:rel :customer-id :delivery-date]
        Item [:rel :customer-id :delivery-date :product]

        OrderItemCount
        (-> Order
            (join Item {:customer-id :customer-id
                        :delivery-date :delivery-date})
            (summarize
              [:customer-id :delivery-date]
              :num-items [count]))

        OrderInfo
        (-> Order
            (join OrderItemCount {:customer-id :customer-id, :delivery-date :delivery-date}))]

    (-> (empty-state
          {:constraints [[:foreign-key Order Customer {:customer-id :customer-id}]
                         [:foreign-key Item Order {:customer-id :customer-id, :delivery-date :delivery-date}]
                         [:unique-key Customer :customer-id]
                         [:unique-key Customer :email]]})
        (modify
          [[:insert Customer {:customer-id 0, :email "a@b.com", :name "Fred"}]
           [:insert Customer {:customer-id 1, :email "b@b.com", :name "Alice"}]
           [:insert Order {:customer-id 0, :delivery-date "mon"}]
           [:insert Order {:customer-id 1, :delivery-date "tues"}]
           [:insert Item {:customer-id 0, :delivery-date "mon", :product "eggs"}]
           [:insert Item {:customer-id 0, :delivery-date "mon", :product "bacon"}]
           [:insert Item {:customer-id 1, :delivery-date "tues", :product "cheese"}]])
        (q OrderInfo))))