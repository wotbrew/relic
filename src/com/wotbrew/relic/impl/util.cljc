(ns com.wotbrew.relic.impl.util
  "Generic utility functions, for use everywhere. Depends on nothing."
  (:require [clojure.string :as str]))

(defn dissoc-in
  "Recursive dissoc, like assoc-in. Removes intermediates.
  Not safe on records or vectors. Map only."
  [m ks]
  (if-let [[k & ks] (seq ks)]
    (if (seq ks)
      (let [v (dissoc-in (get m k) ks)]
        (if (empty? v)
          (dissoc m k)
          (assoc m k v)))
      (dissoc m k))
    m))

(defn disjoc
  "Convenience for remove the element x of the set at (m k) returning a new map.
  If the resulting set is empty, drop k from the map."
  [m k x]
  (let [ns (disj (get m k) x)]
    (if (empty? ns)
      (dissoc m k)
      (assoc m k ns))))

(defn disjoc-in
  "Recursive disjoc for a set nested in a map given a path, see disjoc.
  Removes intermediates. Not safe on records or vectors. Map only."
  [m ks x]
  (let [[k & ks] ks]
    (if ks
      (let [nm (disjoc-in (get m k) ks x)]
        (if (empty? nm)
          (dissoc m k)
          (assoc m k nm)))
      (let [ns (disj (get m k) x)]
        (if (empty? ns)
          (dissoc m k)
          (assoc m k ns))))))

(defn update-in2
  "Like clojure.core/update-in but lets you specify the type of empty-map to use, e.g for nesting (sorted-map)."
  ([m empty ks f & args]
   (let [up (fn up [m ks f args]
              (let [[k & ks] ks]
                (if ks
                  (assoc m k (up (get m k empty) ks f args))
                  (assoc m k (apply f (get m k) args)))))]
     (up m ks f args))))

(def set-conj (fnil conj #{}))

(defn raise
  "Throws an ex-info."
  ([msg] (throw (ex-info msg {})))
  ([msg map] (throw (ex-info msg map)))
  ([msg map cause] (throw (ex-info msg map cause))))

(defn realise-coll
  "If coll is an eduction, yields a vector."
  [maybe-eduction]
  (cond
    (nil? maybe-eduction) nil
    (coll? maybe-eduction) maybe-eduction
    :else (vec maybe-eduction)))

;; cross platform mutable set
(defn mutable-set [] (volatile! (transient #{})))
(defn add-to-mutable-set [mset v] (vswap! mset conj! v) mset)
(defn del-from-mutable-set [mset v] (vswap! mset disj! v) mset)
(defn iterable-mut-set [mset] (when mset (persistent! @mset)))

;; cross platform mutable list
(defn mutable-list [] (volatile! (transient [])))
(defn add-to-mutable-list [mlist v] (vswap! mlist conj! v) mlist)
(defn iterable-mut-list [mlist] (when mlist (persistent! @mlist)))

(defn best-effort-fn-name [expr]
  (let [expr-str (str #?(:clj expr :cljs (.-name expr)))
        split-expr (str/split expr-str #"\$")
        ns (str/join "." (butlast split-expr))
        f (last split-expr)
        show-ns (if (= #?(:clj "clojure.core" :cljs "cljs.core") ns) false true)
        [f] (str/split (str f) #"\@")]
    #?(:clj  (clojure.lang.Compiler/demunge (if show-ns (str ns "/" f) f))
       :cljs (if (empty? f) "fn" (demunge (if show-ns (str ns "/" f) f))))))

(defn vector-if-any-non-nil
  ([a] (when (some? a) [a]))
  ([a b] (when (or (some? a) (some? b)) [a b]))
  ([a b c] (when (or (some? a) (some? b) (some? c)) [a b c]))
  ([a b c & more] (when (or (some? a) (some? b) (some? c) (some some? more)) (into [a b c] more))))