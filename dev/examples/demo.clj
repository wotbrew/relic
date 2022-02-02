(ns examples.demo
  (:require [com.wotbrew.relic :as r]
            [dev]))

(def db
  (-> {}
      (r/mat [[:from :Var] [:fk :Namespace {:ns :ns}]])
      (r/transact
        {:Namespace (for [ns (all-ns)] {:ns ns})}
        {:Var (for [ns (all-ns) [v _] (ns-publics ns)] {:ns ns, :var v})})))

(def VarInfo
  [[:from :Var]
   [:extend
    [:ns-str [str :ns]]]])

(def CoreVar
  [[:from VarInfo]
   [:where [= :ns-str "clojure.core"]]])

(defn core-q [] (dev/bench (count (r/q db CoreVar))))

(def NsStats
  [[:from :Namespace]
   [:left-join
    [[:from :Var]
     [:agg [:ns] [:var-count count]]]
    {:ns :ns}]
   [:extend [:var-count [:or :var-count 0]]]])

(defn clj-agg-q []
  (let [namespaces (r/q db :Namespace)
        vars (r/q db :Var)]
    (dev/bench
      (let [var-groups (reduce (fn [s var] (update s (:ns var) (fnil conj #{}) var)) {} vars)]
        (->> (set (for [ns namespaces
                        :let [vars (var-groups (:ns ns))]]
                    (assoc ns :var-count (count vars))))
             (sort-by :var-count >)
             (into [] (take 5)))))))
;; => 5ms

(defn r-agg-q []
  (dev/bench (r/q db NsStats {:rsort :var-count, :xf (take 5)})))

;; => 8.5ms

(time (count (r/q db NsStats)))

(def db (time (r/mat db NsStats)))

(defn create-var []
  (clojure.lang.Var/intern ^clojure.lang.Namespace (find-ns 'clojure.core) (gensym "relic-rocks")))

(def db (time (r/transact db {:Var [{:var (create-var), :ns (find-ns 'clojure.core)}]})))

(r/q db NsStats {:rsort :var-count, :xf (take 5)})

(def db (time (r/demat db NsStats)))