(ns com.wotbrew.tarpit.fusion
  "The fusion api allows optimisations to be applied as you dynamically
  construct relations."
  (:require [com.wotbrew.tarpit.protocols :as p]
            [clojure.set :as set]
            [com.wotbrew.tarpit.errors :as errors]
            [clojure.string :as str])
  (:refer-clojure :exclude [extend]))

(defn- extension-fn [variables extensions]
  (let [fns (mapv (fn [extension] (p/extension->fn extension variables)) extensions)]
    (case (count fns)
      0 identity
      1 (first fns)
      (fn [row]
        (loop [ret row
               i 0]
          (if (= i (count fns))
            ret
            (let [e (nth fns i)]
              (recur (e ret) (unchecked-inc-int i)))))))))

(defn- restrict-pred [variables restrictions]
  (let [fns (mapv (fn [restriction] (p/restriction->fn restriction variables)) restrictions)]
    (case (count fns)
      0 (constantly true)
      1 (first fns)
      (fn [row]
        (loop [i 0]
          (if (= i (count fns))
            true
            (let [r (nth fns i)]
              (if (r row)
                (recur (unchecked-inc-int i))
                false))))))))

(declare scan?)

(defn variable-cannot-exist? [rel k]
  (let [{:keys [base-keys, extension-keys, projected-keys, projected-away-keys]} rel]
    (cond
      (contains? extension-keys k) false
      (contains? base-keys k) false
      (contains? projected-keys k) false
      (contains? projected-away-keys k) true
      projected-keys true
      base-keys true
      :else false)))

(defn- get-dynamic-variable [rel k]
  (cond
    (not (keyword? k)) nil
    (variable-cannot-exist? rel k) nil
    (and (namespace k) (str/starts-with? (namespace k) "_")) nil
    :else {}))

(defrecord Scan
  [base-rel

   xf

   extensions
   restrictions

   base-keys
   extension-keys
   projected-keys
   projected-away-keys]
  p/Rel
  (rel-id [rel] rel)
  (row-set [rel st]
    (let [filter-pred (restrict-pred (partial get-dynamic-variable rel) restrictions)
          extend-fn (extension-fn (partial get-dynamic-variable rel) extensions)

          project-fn
          (cond
            (nil? projected-keys) identity
            (nil? base-keys) #(select-keys % projected-keys)
            (= (set/union base-keys extension-keys) projected-keys) identity
            :else #(select-keys % projected-keys))

          project-away-fn
          (if (empty? projected-away-keys)
            identity
            #(reduce dissoc % projected-away-keys))

          xf (comp
               (or xf identity)
               (map extend-fn)
               (filter filter-pred)
               (map (comp project-fn project-away-fn)))]

      (into #{} xf (p/row-set base-rel st)))))

(defn scan? [rel] (instance? Scan rel))

(defn- scan
  [rel]
  (if (and (vector? rel) (= :rel (first rel)))
    (let [[_ & ks] rel
          ks (set ks)]
      (map->Scan {:base-rel rel, :base-keys ks, :extension-keys #{}}))
    (map->Scan {:base-rel rel, :extension-keys #{}})))

(defn- add-keys-to-projection-if-projected [rel ks]
  (if-not (scan? rel)
    (add-keys-to-projection-if-projected (scan rel) ks)
    (let [{:keys [projected-keys projected-away-keys]} rel]
      (cond-> rel
              projected-keys (update :projected-keys into ks)
              projected-away-keys (update :projected-away-keys into ks)))))

(defn- add-extension [rel extension]
  (if-not (scan? rel)
    (add-extension (scan rel) extension)
    (let [{:keys [extensions restrictions]} rel
          deps (p/extension-deps extension)
          ks (p/extension-keys extension)]

      (doseq [dep deps]
        (when (variable-cannot-exist? rel dep)
          (throw (errors/extension-dep-cannot-exist dep rel))))

      (cond
        restrictions
        (let [xf (filter (restrict-pred (partial get-dynamic-variable rel) restrictions))
              rel (assoc rel :restrictions nil)]
          (add-extension
            (if (:xf rel)
              (update rel :xf comp xf)
              (assoc rel :xf xf))
            extension))

        extensions
        (-> rel
            (update :extensions conj extension)
            (update :extension-keys into ks)
            (add-keys-to-projection-if-projected ks))

        :else
        (-> rel
            (assoc :extensions [extension],
                   :extension-keys ks)
            (add-keys-to-projection-if-projected ks))))))

(defn extend [rel extensions]
  (reduce add-extension rel extensions))

(defn add-restriction [rel restriction]
  (if-not (scan? rel)
    (add-restriction (scan rel) restriction)
    (let [{:keys [extensions restrictions extension-keys]} rel
          deps (p/restriction-deps restriction)]
      (doseq [dep deps]
        (when (variable-cannot-exist? rel dep)
          (throw (errors/restriction-dep-cannot-exist dep rel))))

      (cond
        (some extension-keys deps)
        (let [xf (filter (extension-fn (partial get-dynamic-variable rel) extensions))
              rel (assoc rel :extensions nil, :extension-keys #{})]
          (add-restriction
            (if (:xf rel)
              (update rel :xf comp xf)
              (assoc rel :xf xf))
            restriction))

        restrictions
        (update rel :restrictions conj restriction)

        :else
        (assoc rel :restrictions [restriction])))))

(defn restrict [rel restrictions]
  (reduce add-restriction rel restrictions))

(defn project [rel ks]
  (if-not (scan? rel)
    (project (scan rel) ks)
    (do (doseq [k ks]
          (when (variable-cannot-exist? rel k)
            (errors/warn-project-key-cannot-exist k rel)))
        (let [ks (remove #(variable-cannot-exist? rel %) ks)]
          (assoc rel :projected-keys (set ks), :projected-away-keys nil)))))

(defn project-away [rel ks]
  (if-not (scan? rel)
    (project-away (scan rel) ks)
    (do (doseq [k ks]
          (when (variable-cannot-exist? rel k)
            (errors/warn-project-away-key-cannot-exist k rel)))
        (let [ks (remove #(variable-cannot-exist? rel %) ks)]
          (update rel :projected-away-keys (fnil into #{}) ks)))))

(defn join [rel1 rel2 km]
  (let [s1 (scan rel1)
        s2 (scan rel2)]
    (doseq [[k1 k2] km]
      (when (variable-cannot-exist? s1 k1)
        (throw (errors/join-left-cannot-exist s1 s2 k1 k2)))
      (when (variable-cannot-exist? s2 k2)
        (throw (errors/join-right-cannot-exist s1 s2 k1 k2))))
    [:join s1 s2 km]))