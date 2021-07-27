(ns com.wotbrew.tarpit
  "Implements functional relational programming as described in the out-of-the-tarpit paper.

  Modification primitives:
  [:insert rel match-rel]
  [:update rel overwrites match-rel]
  [:delete rel match-rel]"
  (:require [com.wotbrew.tarpit.naive :as naive]
            [com.wotbrew.tarpit.protocols :as p]
            [clojure.core :as clj])
  (:refer-clojure :exclude [extend update]))

(defn rel "Defines a base relation" [& keys] (into [:rel] keys))

(defn extend [rel & extensions] (into [:extend rel] extensions))
(defn project [rel & keys] (into [:project rel] keys))
(defn project-away [rel & keys] (into [:project-away rel] keys))
(defn restrict [rel & restrictions] (into [:restrict rel] restrictions))
(defn summarize [rel keys & agg] (into [:summarize rel keys] agg))
(defn join [rel1 rel2 km] [:join rel1 rel2 km])
(defn union [rel1 rel2] [:union rel1 rel2])
(defn difference [rel1 rel2] [:difference rel1 rel2])
(defn intersection [rel1 rel2] [:intersection rel1 rel2])

(defn eq [value prop] [:eq value prop])
(defn unique-key [rel & keys] (into [:unique-key rel] keys))
(defn foreign-key [rel1 rel2 km] (into [:foreign-key rel1 rel2 km] keys))

(defn insert [rel row] [:insert rel row])
(defn update [rel set-map match-rel] [:update rel set-map match-rel])
(defn delete [rel match-rel] [:delete rel match-rel])

(extend-protocol p/RelId
  Object
  (rel-id [rel] rel))

(defn q [st rel]
  (naive/q st rel))

(defn modify [st & modifications]
  (naive/modify st modifications))

(defrecord State [schema]
  p/State
  (get-schema [st] schema)
  (rows [st rel] (get st (p/rel-id rel) #{})))

(defn empty-state [schema] (->State schema))