(ns com.wotbrew.tarpit.naive
  "An FRP implementation for maps that is super slow, but works."
  (:require [clojure.set :as set]
            [com.wotbrew.tarpit.errors :as errors]
            [com.wotbrew.tarpit.protocols :as p]))

(defn empty-state [schema]
  {:tarpit/schema schema})

(defn- substitute [row arg]
  (if (contains? row arg)
    (get row arg)
    arg))

(defn q [st rel]
  (case (nth rel 0)

    :rel (p/rows st rel)

    :extend (let [[_ rel & extensions] rel
                  base (q st rel)]
              (reduce
                (fn [acc row]
                  (loop [row row
                         extensions extensions]
                    (if-some [[k ext & extensions] (seq extensions)]
                      (let [[f & args] ext]
                        (recur (assoc row k (apply f (map (partial substitute row) args))) extensions))
                      (conj acc row))))
                #{}
                base))

    :project (let [[_ rel & projections] rel
                   base (q st rel)]
               (set/project base projections))

    :project-away (let [[_ rel & projections] rel
                        base (q st rel)]
                    (set (map #(apply dissoc % projections) base)))

    :restrict (let [[_ rel & restrictions] rel
                    base (q st rel)
                    pred (fn [row]
                           (loop [restrictions restrictions]
                             (if-some [[r & restrictions] (seq restrictions)]
                               (let [[f & args] r]
                                 (if (apply f (map (partial substitute row) args))
                                   (recur restrictions)
                                   false))
                               true)))]
                (set/select pred base))

    :join (let [[_ rel1 rel2 km] rel
                base1 (q st rel1)
                base2 (q st rel2)]
            (set/join base1 base2 km))

    :summarize
    (let [[_ rel key & aggregates] rel
          base (q st rel)]
      (set
        (for [[group rows] (set/index base key)]
          (loop [group group
                 aggregates aggregates]
            (if-some [[k agg & aggregates] (seq aggregates)]
              (let [[f & args] agg]
                (recur (assoc group k (apply f rows args)) aggregates))
              group)))))))

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
                     ;;todo vector escape?
                     (if (vector? arg)
                       (let [[f rel] arg]
                         (f (q st rel)))
                       arg))]
      (apply pred (map subs-arg args)))))

(defn ensure [st constraint]
  (when-not (p/check st constraint)
    (throw (errors/constraint-violation st constraint))))

(defn modify [st modifications]
  (let [st (reduce
             (fn [st [op & args]]
               (case op
                 :insert
                 (let [[rel row] args]
                   (update st (p/rel-id rel) (fnil conj #{}) row))
                 :update
                 (let [[rel set-map match-rel] args
                       matched (q st match-rel)
                       new-rows (set (map (fn [m] (merge m set-map)) matched))
                       f (fn [xset] (-> xset set (set/difference matched) (set/union new-rows)))]
                   (update st (p/rel-id rel) f))
                 :delete
                 (let [[rel match-rel] args
                       matched (q st match-rel)]
                   (update st (p/rel-id rel) (fnil set/difference #{}) matched))))
             st
             modifications)]
    (doseq [constraint (:constraints (p/get-schema st))]
      (ensure st constraint))
    st))