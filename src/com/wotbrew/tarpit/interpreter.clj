(ns com.wotbrew.tarpit.interpreter
  "Allows interpretation of relation AST directly.

  An interpreter (interpreter) for a relation is a function of state -> set

  An interpreter (mod-interpreter) for a modification is a function of state -> state"
  (:require [clojure.set :as set]
            [com.wotbrew.tarpit.errors :as errors]
            [com.wotbrew.tarpit.protocols :as p])
  (:refer-clojure :exclude [ensure]))

(defn- substitute [row arg]
  (if (contains? row arg)
    (get row arg)
    arg))

(defmulti vec-interpreter (fn [rel] (nth rel 0)))

(defn interpreter [rel]
  (if (vector? rel)
    (vec-interpreter rel)
    (fn [st] (p/row-set rel st))))

(defmethod vec-interpreter :rel [rel]
  (fn [st] (p/materialized-row-set st rel)))

(defmethod vec-interpreter :const [[_ rows]]
  (constantly (set rows)))

(defmethod vec-interpreter :dynamic [[_ f]]
  (fn [st] ((interpreter (f)) st)))

(defmethod vec-interpreter :extend [[_ rel & extensions]]
  (let [base-interpreter (interpreter rel)]
    (fn [st]
      (let [base (base-interpreter st)]
        (reduce
          (fn [acc row]
            (loop [row row
                   extensions extensions]
              (if-some [[k ext & extensions] (seq extensions)]
                (let [[f & args] ext]
                  (recur (assoc row k (apply f (map (partial substitute row) args))) extensions))
                (conj acc row))))
          #{}
          base)))))

(defmethod vec-interpreter :project [[_ rel & projections]]
  (let [base-interpreter (interpreter rel)]
    (fn [st]
      (let [base (base-interpreter st)]
        (set/project base projections)))))

(defmethod vec-interpreter :project-away [[_ rel & projections]]
  (let [base-interpreter (interpreter rel)]
    (fn [st]
      (let [base (base-interpreter st)]
        (set (map #(apply dissoc % projections) base))))))

(defmethod vec-interpreter :restrict [[_ rel & restrictions]]
  (let [base-interpreter (interpreter rel)
        pred (fn [row]
               (loop [restrictions restrictions]
                 (if-some [[r & restrictions] (seq restrictions)]
                   (let [[f & args] r]
                     (if (apply f (map (partial substitute row) args))
                       (recur restrictions)
                       false))
                   true)))]
    (fn [st]
      (set/select pred (base-interpreter st)))))

(defmethod vec-interpreter :join [[_ rel1 rel2 km]]
  (let [i1 (interpreter rel1)
        i2 (interpreter rel2)]
    (fn [st]
      (let [base1 (i1 st)
            base2 (i2 st)]
        (set/join base1 base2 km)))))

(defmethod vec-interpreter :union [[_ rel1 rel2]]
  (let [i1 (interpreter rel1)
        i2 (interpreter rel2)]
    (fn [st] 
      (set/union (i1 st) (i2 st)))))

(defmethod vec-interpreter :difference [[_ rel1 rel2]]
  (let [i1 (interpreter rel1)
        i2 (interpreter rel2)]
    (fn [st]
      (set/difference (i1 st) (i2 st)))))

(defmethod vec-interpreter :intersection [[_ rel1 rel2]]
  (let [i1 (interpreter rel1)
        i2 (interpreter rel2)]
    (fn [st]
      (set/intersection (i1 st) (i2 st)))))

(defmethod vec-interpreter :summarize [[_ rel key & aggregates]]
  (let [base-interpreter (interpreter rel)]
    (fn [st]
      (set
        (for [[group rows] (set/index (base-interpreter st) key)]
          (loop [group group
                 aggregates aggregates]
            (if-some [[k agg & aggregates] (seq aggregates)]
              (let [[f & args] agg]
                (recur (assoc group k (apply f rows args)) aggregates))
              group)))))))

(defn q [st rel]
  ((interpreter rel) st))

(defmulti mod-interpreter (fn [mod] (nth mod 0)))

(defmethod mod-interpreter :insert [[_ rel row]]
  (fn [st]
    (p/add-rows st rel [row])))

(defmethod mod-interpreter :update [[_ rel set-map match-rel]]
  (let [match-interpreter (interpreter match-rel)]
    (fn [st]
      (let [matched (match-interpreter st)
            new-rows (set (map (fn [m] (merge m set-map)) matched))]
        (-> st
            (p/del-rows rel matched)
            (p/add-rows rel new-rows))))))

(defmethod mod-interpreter :delete [[_ rel match-rel]]
  (let [match-interpreter (interpreter match-rel)]
    (fn [st]
      (let [matched (match-interpreter st)]
        (p/del-rows st rel matched)))))

(defn check [st constraint]
  (case (nth constraint 0)
    :unique-key
    (let [[_ rel & keys] constraint]
      (check st [<= [(fn [coll] (reduce max 0 (map ::n coll))) [:summarize rel keys ::n [count]]] 1]))
    :foreign-key
    (let [[_ rel1 rel2 km] constraint]
      (check st [<= [count rel1] [count [:join rel1 rel2 km]]]))
    (let [[pred & args] constraint
          subs-arg (fn [arg]
                     (if (vector? arg)
                       (let [[f rel] arg]
                         (f (q st rel)))
                       arg))]
      (apply pred (map subs-arg args)))))

(defn ensure [st constraint]
  (when-not (check st constraint)
    (throw (errors/constraint-violation st constraint))))

(defn modify [st modifications]
  (let [rf (fn [st mod] ((mod-interpreter mod) st))
        st (reduce rf st modifications)]
    (doseq [constraint (:constraints (p/get-schema st))]
      (ensure st constraint))
    st))