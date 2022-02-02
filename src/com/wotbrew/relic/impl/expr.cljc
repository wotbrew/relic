(ns com.wotbrew.relic.impl.expr
  (:require [com.wotbrew.relic.impl.util :as u]
            [com.wotbrew.relic.impl.relvar :as r]
            #?(:clj [clojure.core :as clj]
               :cljs [cljs.core :as clj]))
  (:refer-clojure :exclude [< <= > >=]))

(def ^:dynamic *env-deps* nil)

(defn- track-env-dep [k]
  (if *env-deps*
    (set! *env-deps* (conj *env-deps* k))
    (u/raise "Can only use ::rel/env in :extend/:select/:expand statements.")))

(declare row-fn)

(defn- expr-mut-reduction [expr]
  (let [xfn (row-fn expr)]
    (fn [arg-buf row]
      (if-some [xval (xfn row)]
        (u/add-to-mutable-list arg-buf xval)
        (reduced nil)))))

(defn- unsafe-row-fn-call [f args]
  (let [args (mapv row-fn args)]
    (case (count args)
      0 f
      1 (comp f (first args))
      2 (let [[a b] args] #(f (a %) (b %)))
      3 (let [[a b c] args] #(f (a %) (b %) (c %)))
      4 (let [[a b c d] args] #(f (a %) (b %) (c %) (d %)))
      5 (let [[a b c d e] args] #(f (a %) (b %) (c %) (d %) (e %)))
      (let [get-args (apply juxt args)]
        #(apply f (get-args %))))))

(defn- nil-safe-row-fn-call [f args]
  (let [args (mapv row-fn args)]
    (case (count args)
      0 f
      1 (let [[a] args]
          (fn apply1 [row]
            (when-some [aval (a row)]
              (f aval))))
      2 (let [[a b] args]
          (fn apply2 [row]
            (when-some [aval (a row)]
              (when-some [bval (b row)]
                (f aval bval)))))
      (let [as-rf (mapv expr-mut-reduction args)]
        (fn [row]
          (let [arg-buf (u/mutable-list)
                arg-buf (reduce (fn [arg-buf f] (f arg-buf row)) arg-buf as-rf)]
            (when arg-buf
              (apply f (u/iterable-mut-list arg-buf)))))))))

(defn- demunge-expr [expr] (u/best-effort-fn-name expr))

;; not going to use keywords as I do not want
;; exfiltration to be possible using edn user input

(defonce sub-select-first (fn [& _] (u/raise "Cannot use rel/sel1 outside of relic expressions.")))
(defonce sub-select (fn [& _] (u/raise "Cannot use rel/sel outside of relic expressions.")))
(defonce env (fn [& _] (u/raise "Cannot use rel/env outside of relic expressions.")))

(defn <
  ([x] true)
  ([x y]
   (clj/< (compare x y) 0))
  ([x y & more]
   (if (< x y)
     (if (next more)
       (recur y (first more) (next more))
       (< y (first more)))
     false)))

(defn <=
  ([x] true)
  ([x y]
   (clj/<= (compare x y) 0))
  ([x y & more]
   (if (<= x y)
     (if (next more)
       (recur y (first more) (next more))
       (<= y (first more)))
     false)))

(defn >
  ([x] true)
  ([x y]
   (clj/> (compare x y) 0))
  ([x y & more]
   (if (> x y)
     (if (next more)
       (recur y (first more) (next more))
       (> y (first more)))
     false)))

(defn >=
  ([x] true)
  ([x y]
   (clj/>= (compare x y) 0))
  ([x y & more]
   (if (>= x y)
     (if (next more)
       (recur y (first more) (next more))
       (>= y (first more)))
     false)))

(defn safe-expr-str [expr]
  (cond
    (fn? expr) (demunge-expr expr)

    (keyword? expr) expr

    (vector? expr) (with-out-str (print (mapv safe-expr-str expr)))

    (identical? sub-select-first expr) "sel"

    (identical? sub-select expr) "sel1"

    (identical? env expr) "env"

    :else (str "(" (type expr) ")")))

(defn- add-expr-ex-handler
  "Adds default exception handling to relic function calls, `f` is a function of a row."
  [f expr]
  (fn with-ex-handler [row]
    (try
      (f row)
      #?(:clj
         (catch NullPointerException e
           (u/raise (str "Null pointer exception thrown by relic expression, consider using " (safe-expr-str (into [:?] expr)))
                    {:expr expr} e)))
      (catch #?(:clj Throwable :cljs js/Error) e
        (u/raise (str "Exception thrown by relic expression " (safe-expr-str expr)) {:expr expr} e)))))

(defn- to-function [f]
  (cond
    (identical? clj/< f) <
    (identical? clj/<= f) <=
    (identical? clj/> f) >
    (identical? clj/>= f) >=
    (fn? f) f
    (keyword? f) f
    :else (u/raise "Expected a function in expression prefix position")))

(def ^:dynamic *implicit-joins* nil)

(defn- add-implicit-join [relvar clause]
  (when-some [j *implicit-joins*]
    (set! *implicit-joins* (conj j [relvar clause]))))

(defn row-fn [expr]
  (cond
    (= [] expr) (constantly [])

    (= :% expr) identity

    (vector? expr)
    (let [[f & args] expr]
      (cond
        ;; unsafe ops are matched against sentinel vals
        ;; this avoids exfiltration via injection attack using
        ;; edn data
        (identical? env f)
        (let [[k not-found] args]
          (track-env-dep k)
          (fn [row]
            (-> row ::env (get k not-found))))

        (identical? sub-select f)
        (let [[relvar clause] args
              relvar (r/to-relvar relvar)
              k [relvar clause]]
          (add-implicit-join relvar clause)
          (fn [row]
            (row k)))

        (identical? sub-select-first f)
        (let [[relvar clause] args
              relvar (r/to-relvar relvar)
              k [relvar clause]]
          (add-implicit-join relvar clause)
          (fn [row] (first (row k))))

        :else
        (case f
          :and (apply every-pred (map row-fn args))
          :or (apply some-fn (map row-fn args))
          :not (unsafe-row-fn-call not args)
          :if (let [[c t e] (map row-fn args)]
                (if e
                  (fn [row]
                    (if (c row)
                      (t row)
                      (e row)))
                  (fn [row]
                    (when (c row)
                      (t row)))))

          :com.wotbrew.relic/get
          (let [[k not-found] args]
            (fn [row] (row k not-found)))

          (:com.wotbrew.relic/esc :_)
          (let [[v] args]
            (constantly v))

          (:com.wotbrew.relic/nil-safe :?)
          (let [[f & args] args]
            (add-expr-ex-handler (nil-safe-row-fn-call (to-function f) args) expr))

          (:com.wotbrew.relic/unsafe :!)
          (let [[f & args] args]
            (unsafe-row-fn-call (to-function f) args))

          (add-expr-ex-handler (unsafe-row-fn-call (to-function f) args) expr))))

    (keyword? expr) expr

    (fn? expr) expr

    :else (constantly expr)))

(defn call-expr? [expr]
  (vector? expr))

(defn operator [expr]
  (when (call-expr? expr) (nth expr 0)))

(defn const-expr?
  [expr]
  (cond
    (vector? expr) (contains? #{:_ :com.wotbrew.relic/esc} (operator expr))
    (keyword? expr) false
    (fn? expr) false
    :else true))

(defn unwrap-const
  [expr]
  (if (const-expr? expr)
    (if (vector? expr)
      (recur (nth expr 1 nil))
      expr)
    expr))

(defn eq-expr? [expr]
  (= = (operator expr)))

(defn- binary-dyn-const?
  ([expr]
   (when (and (call-expr? expr) (= 3 (count expr)))
     (let [[_ a b] expr]
       (binary-dyn-const? a b))))
  ([a b]
   (and (or (const-expr? a)
            (const-expr? b))
        (or (not (const-expr? a))
            (not (const-expr? b))))))

(defn eq-to-const?
  "True if [= a b] where either a or b is const (but not both)"
  [expr]
  (and (eq-expr? expr)
       (binary-dyn-const? expr)))

(defn destructure-const-test
  "If expr is a simple test, [op const-expr expr] or [op expr const-expr].

  Where op is =,<,>,>=,<=.

  Return a map :test (:=, :< :<= :>= :>), :const (expr), :row (expr).

  Test should always be applied in the following order (test row const), (test is flipped if operands in different order)

  Used for choosing :where plans according to indexes."
  [expr]
  (when (binary-dyn-const? expr)
    (let [[op a b] expr
          c (if (const-expr? a) (unwrap-const a) (unwrap-const b))
          d (if (const-expr? a) b a)
          t (fn [t]
              {:test t
               :const c
               :row d})
          ab (not (identical? d b))]
      (condp identical? op
        clj/< (t (if ab :< :>))
        < (t (if ab :< :>))

        clj/<= (t (if ab :<= :>=))
        <= (t (if ab :<= :>=))

        clj/> (t (if ab :> :<))
        > (t (if ab :> :<))

        clj/>= (t (if ab :>= :<=))
        >= (t (if ab :>= :<=))

        = (t :=)

        nil))))