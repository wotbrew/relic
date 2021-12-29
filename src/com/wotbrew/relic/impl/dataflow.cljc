(ns ^:no-doc com.wotbrew.relic.impl.dataflow
  "Core dataflow implementation - the 'innards'. All functions in this namespace
  should be considered private."
  (:require [com.wotbrew.relic.impl.generic-agg-index :as gai]
            [clojure.set :as set]
            [clojure.string :as str]))

(defn- dissoc-in
  "Recursive dissoc, like assoc-in.
  Removes intermediates.
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

(defn- disjoc
  "Convenience for remove the element x of the set at (m k) returning a new map.
  If the resulting set is empty, drop k from the map."
  [m k x]
  (let [ns (disj (get m k) x)]
    (if (empty? ns)
      (dissoc m k)
      (assoc m k ns))))

(defn- disjoc-in
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

(defn- update-in2
  ([m empty ks f & args]
   (let [up (fn up [m ks f args]
              (let [[k & ks] ks]
                (if ks
                  (assoc m k (up (get m k empty) ks f args))
                  (assoc m k (apply f (get m k) args)))))]
     (up m ks f args))))

(def ^:private set-conj (fnil conj #{}))

(defn- raise
  ([msg] (throw (ex-info msg {})))
  ([msg map] (throw (ex-info msg map)))
  ([msg map cause] (throw (ex-info msg map cause))))

(def ^:dynamic *env-deps* nil)
(defn- track-env-dep [k]
  (if *env-deps*
    (set! *env-deps* (conj *env-deps* k))
    (raise "Can only use ::rel/env in :extend/:select/:expand statements.")))

;; cross platform mutable set
(defn- mutable-set [] (volatile! (transient #{})))
(defn- add-to-mutable-set [mset v] (vswap! mset conj! v) mset)
(defn- del-from-mutable-set [mset v] (vswap! mset disj! v) mset)
(defn- iterable-mut-set [mset] (when mset (persistent! @mset)))

;; cross platform mutable list
(defn- mutable-list [] (volatile! (transient [])))
(defn- add-to-mutable-list [mlist v] (vswap! mlist conj! v) mlist)
(defn- iterable-mut-list [mlist] (when mlist (persistent! @mlist)))

(declare row-fn)

(defn- expr-mut-reduction [expr]
  (let [xfn (row-fn expr)]
    (fn [arg-buf row]
      (if-some [xval (xfn row)]
        (add-to-mutable-list arg-buf xval)
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
          (let [arg-buf (mutable-list)
                arg-buf (reduce (fn [arg-buf f] (f arg-buf row)) arg-buf as-rf)]
            (when arg-buf
              (apply f (iterable-mut-list arg-buf)))))))))

(defn- demunge-expr [expr]
  (let [expr-str (str expr)
        [ns f] (str/split expr-str #"\$")
        show-ns (if (= "clojure.core" ns) false true)
        [f] (str/split (str f) #"\@")]
    #?(:clj  (clojure.lang.Compiler/demunge (if show-ns (str ns "/" f) f))
       :cljs (str expr))))

(defn- safe-print-expr [expr]
  (cond
    (fn? expr) (demunge-expr expr)

    (keyword? expr) expr

    (vector? expr) (with-out-str (print (mapv safe-print-expr expr)))

    :else (str "(" (type expr) ")")))

(defn- add-expr-ex-handler
  "Adds default exception handling to relic function calls, `f` is a function of a row."
  [f expr]
  (fn with-ex-handler [row]
    (try
      (f row)
      #?(:clj
         (catch NullPointerException e
           (raise (str "Null pointer exception thrown by relic expression, consider using " (safe-print-expr (into [:?] expr)))
                  {:expr expr} e)))
      (catch #?(:clj Throwable :cljs js/Error) e
        (raise (str "Exception thrown by relic expression " (safe-print-expr expr)) {:expr expr} e)))))

(defn- to-function [f]
  (cond
    (fn? f) f
    #?@(:clj [(qualified-symbol? f) @(requiring-resolve f)])
    #?@(:clj [(symbol? f) @(resolve f)])
    (keyword? f) f
    :else (raise "Expected a function in expression prefix position")))

(def ^:dynamic *implicit-joins* nil)

(defn- add-implicit-join [relvar clause]
  (when-some [j *implicit-joins*]
    (set! *implicit-joins* (conj j [relvar clause]))))

(defn to-relvar [relvar-or-table]
  (if (keyword? relvar-or-table)
    [[:table relvar-or-table]]
    relvar-or-table))

(defn row-fn [expr]
  (cond
    (= [] expr) (constantly [])

    (= :% expr) identity

    (vector? expr)
    (let [[f & args] expr]
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

        :com.wotbrew.relic/env
        (let [[k not-found] args]
          (track-env-dep k)
          (fn [row]
            (-> row ::env (get k not-found))))

        :com.wotbrew.relic/get
        (let [[k not-found] args]
          (fn [row] (row k not-found)))

        (:com.wotbrew.relic/esc :_)
        (let [[v] args]
          (constantly v))

        (:com.wotbrew.relic/join-coll :$)
        (let [[relvar clause] args
              relvar (to-relvar relvar)
              k [relvar clause]]
          (add-implicit-join relvar clause)
          (fn [row]
            (row k)))

        (:com.wotbrew.relic/join-first :$1)
        (let [[relvar clause] args
              relvar (to-relvar relvar)
              k [relvar clause]]
          (add-implicit-join relvar clause)
          (fn [row] (first (row k))))

        (:com.wotbrew.relic/nil-safe :?)
        (let [[f & args] args]
          (add-expr-ex-handler (nil-safe-row-fn-call (to-function f) args) expr))

        (:com.wotbrew.relic/unsafe :!)
        (let [[f & args] args]
          (unsafe-row-fn-call (to-function f) args))

        (add-expr-ex-handler (unsafe-row-fn-call (to-function f) args) expr)))

    (keyword? expr) expr

    #?@(:clj [(qualified-symbol? expr) @(requiring-resolve expr)])

    #?@(:clj [(symbol? expr) @(resolve expr)])

    (fn? expr) expr

    :else (constantly expr)))

(defn where-pred [exprs]
  (case (count exprs)
    0 (constantly true)
    1 (row-fn (first exprs))
    (apply every-pred (map row-fn exprs))))

(defn without-fn [cols]
  #(apply dissoc % cols))

(defn- overwrite-key
  "If v is not nil, assoc into m, otherwise dissoc."
  [m k v]
  (if (nil? v)
    (dissoc m k)
    (assoc m k v)))

(defn- overwrite-keys
  ([m1 m2] (reduce-kv overwrite-key m1 m2))
  ([m1 m2 ks]
   (if (nil? m2)
     m1
     (reduce (fn [m k] (overwrite-key m k (m2 k))) m1 ks))))

(defn bind-fn [binding]
  (if (keyword? binding)
    (case binding
      (:com.wotbrew.relic/* :*) overwrite-keys
      #(overwrite-key %1 binding %2))
    #(overwrite-keys %1 %2 binding)))

(defn extend-form-fn [form]
  (cond
    (vector? form)
    (let [[binding expr] form
          expr-fn (row-fn expr)
          bind (bind-fn binding)]
      (fn bind-extend [row] (bind row (expr-fn row))))

    :else (throw (ex-info "Not a valid binding form, expected a vector [binding expr]" {}))))

(defn extend-form-cols [form]
  (let [[binding] form]
    (if (keyword? binding)
      (case binding
        (:* :com.wotbrew.relic/*) (raise "Cannot use ::rel/* in select expressions yet, use :extend.")
        #{binding})
      (set binding))))

(defn extend-fn [extensions]
  (case (count extensions)
    0 identity
    1 (extend-form-fn (first extensions))
    (apply comp (rseq (mapv extend-form-fn extensions)))))

(defn- expand-form-fn [form]
  (let [[binding expr] form
        expr-fn (row-fn expr)
        bind-fn (bind-fn binding)]
    (fn [row]
      (for [v (expr-fn row)]
        (bind-fn row v)))))

(defn expand-fn [expansions]
  (case (count expansions)
    0 (constantly #{})
    1 (expand-form-fn (first expansions))
    (let [exp-xforms (mapv (comp mapcat expand-form-fn) expansions)
          exp-xf (apply comp (rseq exp-xforms))]
      #(into #{} exp-xf [%]))))

(defn operator [stmt] (nth stmt 0))
(defn left-relvar [relvar] (when (peek relvar) (subvec relvar 0 (dec (count relvar)))))
(defn head-stmt [relvar] (peek relvar))

(defn table-relvar?
  "True if the relvar is a table."
  [relvar]
  (case (count relvar)
    1 (= :table (operator (head-stmt relvar)))
    false))

(defn unwrap-from [relvar]
  (if (= :from (operator (head-stmt relvar)))
    (let [[_ relvar] (head-stmt relvar)]
      (unwrap-from (to-relvar relvar)))
    relvar))

(defn unwrap-table
  "If the relvar is a table (or :from chain containing a table) then unwrap the underlying table relvar and return it."
  [relvar]
  (if (table-relvar? relvar)
    relvar
    (let [relvar (unwrap-from relvar)]
      (when (table-relvar? relvar)
        relvar))))

(defn unwrap-table-key [relvar]
  (let [[_ table-key] (some-> (unwrap-table relvar) head-stmt)]
    table-key))

(defn req-col-check [relvar col]
  (let [table-key-or-name (if-some [tk (unwrap-table-key relvar)] tk "(derived relvar)")]
    {:pred [some? col],
     :error [str [:com.wotbrew.relic/esc col] " required by " [:com.wotbrew.relic/esc table-key-or-name] ", but not found."]}))

(def ^:dynamic *check-violations* nil)

(defn- bad-check [relvar check row]
  (if-some [m *check-violations*]
    (do
      (set! *check-violations* (update m [relvar check] set-conj row))
      row)
    (let [{:keys [error]} check
          error-fn (row-fn (or error "Check constraint violation"))]
      (raise (error-fn row) {:check check, :row row, :relvar relvar}))))

(defn- good-check [relvar check row]
  row)

(defn- check-pred [relvar check]
  (if (map? check)
    (let [{:keys [pred]} check
          pred-fn (row-fn pred)]
      (fn [m]
        (let [r (pred-fn m)]
          (if r
            (good-check relvar check m)
            (bad-check relvar check m)))))
    (let [f2 (row-fn check)]
      (fn [m]
        (let [r (f2 m)]
          (if r
            (good-check relvar check m)
            (bad-check relvar check m)))))))

(defn check-fn [relvar checks]
  (case (count checks)
    0 identity
    1 (check-pred relvar (first checks))
    (apply comp (rseq (mapv #(check-pred relvar %) checks)))))


(def ^:dynamic *ids* nil)
(def ^:dynamic *idn* nil)

(defn get-id [graph relvar]
  ((::ids graph {}) relvar))

(defn get-node [graph relvar]
  (graph (get-id graph relvar)))

(defn id [relvar]
  (or (get *ids* relvar)
      (if-not *idn*
        relvar
        (let [n *idn*
              nid n]
          (set! *ids* (assoc *ids* relvar nid))
          (set! *idn* (inc n))
          nid))))

(defn mem
  [relvar]
  (if-some [table-key (unwrap-table-key relvar)]
    [(fn mget ([db] (db table-key)) ([db default] (db table-key default)))
     (fn mset [& _] (raise "Cannot call mset on table memory"))]
    (let [id (id relvar)]
      [(fn mget
         ([db] (:mem (db id)))
         ([db default] (:mem (db id) default)))
       (fn mset
         [db val]
         (assoc db id (assoc (db id) :mem val)))])))

(defn- flow [relvar f & more]
  (loop [ret (array-map (id relvar) f)
         more more]
    (if-some [[relvar f & tail] (seq more)]
      (recur (assoc ret (id relvar) f) tail)
      ret)))

(defn- same-size? [coll1 coll2] (= (count coll1) (count coll2)))

(defn transform
  "Returns a transform node, applying (f) to each row. As (f) may narrow rows such that n input rows
  = 1 output row, a deduping index is used that keeps track of all inputs, a row is only deletable if all
  possible inputs that can produce the output have been deleted.

  It is important therefore to use transform also on any narrowing, such as join, expand and so on."
  [graph self left f]
  (let [[mget mset] (mem self)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [adds (mutable-set)
                              dels (mutable-set)
                              idx (mget db {})
                              nidx (reduce (fn [idx row]
                                             (let [new-row (f row)
                                                   s (idx new-row #{})
                                                   ns (disj s row)]
                                               (cond
                                                 (same-size? s ns) idx
                                                 (empty? ns) (do (add-to-mutable-set dels new-row)
                                                                 (dissoc idx new-row))
                                                 :else (assoc idx new-row ns)))) idx deleted)
                              nidx (reduce (fn [idx row]
                                             (let [new-row (f row)
                                                   s (idx new-row #{})
                                                   ns (conj s row)]
                                               (if (same-size? s ns)
                                                 idx
                                                 (let [idx (assoc idx new-row ns)]
                                                   (add-to-mutable-set adds new-row)
                                                   idx)))) nidx inserted)
                              db (mset db nidx)
                              adds (iterable-mut-set adds)
                              dels (iterable-mut-set dels)]
                          (forward db adds dels))))
     :provide (fn [db] (some-> (mget db) not-empty keys))}))

(defn transform-unsafe
  "Like transform but with no narrowing protection, faster but only use if you guarantee that inputs rows do not converge
  on the same output rows, otherwise you'll get glitches."
  [graph self left f]
  (let [xf (map f)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward] (forward db (eduction xf inserted) (eduction xf deleted))))}))

(defn union
  "Set union, rows in either left or right are flowed."
  [graph self left right]
  (let [[mgetl] (mem left)
        [mgetr] (mem right)]
    {:deps [left right]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [ridx (mgetr db #{})
                              deleted (eduction (remove ridx) deleted)]
                          (forward db inserted deleted)))
                 right (fn [db inserted deleted forward]
                         (let [lidx (mgetl db #{})
                               deleted (eduction (remove lidx) deleted)]
                           (forward db inserted deleted))))}))

(defn intersection
  "Set intersection, rows that are both in left and right are flowed."
  [graph self left right]
  (let [[mgetl] (mem left)
        [mgetr] (mem right)]
    {:deps [left right]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [ridx (mgetr db #{})
                              inserted (eduction (filter ridx) inserted)
                              deleted (eduction (filter ridx) deleted)]
                          (forward db inserted deleted)))
                 right (fn [db inserted deleted forward]
                         (let [lidx (mgetl db #{})
                               inserted (eduction (filter lidx) inserted)
                               deleted (eduction (filter lidx) deleted)]
                           (forward db inserted deleted))))}))

(defn difference
  "Set difference, rows that are in left and not right are flowed."
  [graph self left right]
  (let [[mgetl] (mem left)
        [mgetr] (mem right)]
    {:deps [left right]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [ridx (mgetr db #{})
                              inserted (eduction (remove ridx) inserted)
                              deleted (eduction (remove ridx) deleted)]
                          (forward db inserted deleted)))
                 right (fn [db inserted deleted forward]
                         (let [lidx (mgetl db #{})
                               ;; flip :)
                               inserted2 (eduction (filter lidx) deleted)
                               deleted2 (eduction (filter lidx) inserted)]
                           (forward db inserted2 deleted2))))}))

(defn provide
  "Generic provision node, e.g if your statement results only ever provides rows to insert,
  never flowing - then use this. :const and :lookup would be examples of provide."
  [graph self _ f]
  {:deps []
   :provide f})

(defn runf
  "A dataflow node that runs the callbacks on-insert / on-delete (presumably for side effects). Used for constraints."
  [graph self left on-insert on-delete]
  {:deps [left]
   :flow (flow left
               (fn [db inserted deleted forward]
                 (let [inserted (if (coll? inserted)
                                  (do (run! on-insert inserted) inserted)
                                  (eduction
                                    (map (fn [x] (on-insert x) x)) inserted))
                       deleted (if (coll? deleted)
                                 (do (run! on-delete deleted) deleted)
                                 (eduction
                                   (map (fn [x] (on-delete x) x)) deleted))]
                   (forward db inserted deleted))))})

(defn where
  "A dataflow node that removes rows that do not meet (pred row) before flowing."
  [graph self left pred]
  (let [xf (filter pred)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (forward db (eduction xf inserted) (eduction xf deleted))))}))

(defrecord Joined [left right])

(defn expand
  "f is a fn of row to a collection of rows, emitting (Joined parent child) for each child in coll."
  [graph self left f]
  (let [exf
        (fn [rf]
          (fn
            ([acc] (rf acc))
            ([acc row]
             (reduce
               (fn [acc rrow]
                 (rf acc (->Joined row rrow)))
               acc
               (f row)))))]
    {:deps [left]
     :flow (flow left
                 (fn [db inserted deleted forward]
                   (forward db (eduction exf inserted) (eduction exf deleted))))}))

(defn- index-seek-from-exprs [exprs]
  (let [fns (mapv row-fn exprs)
        path (if (empty? fns) (constantly []) (apply juxt fns))]
    (if (seq fns)
      (fn [idx row] (get-in idx (path row)))
      (fn [idx _] (when idx (idx nil))))))

(defn- index-seek-fn [index]
  (let [[& exprs] (head-stmt index)]
    (index-seek-from-exprs exprs)))

(defn find-hash-index
  "Looks up an index that allows seeking on the exprs, returns a tuple [index-relvar, seek-fn, unique-seek-fn (maybe)]. "
  ([graph left exprs] (find-hash-index graph left exprs exprs))
  ([graph left exprs row-exprs]
   (let [hash (conj left (into [:hash] exprs))]
     [hash (index-seek-from-exprs row-exprs)])))

(defn join
  "Set join dataflow node, uses the best indexes available (or creates missing indexes if necessary).

  Emits (Joined left right) rows."
  [graph self left seekl right seekr]
  (let [[mgetl] (mem left)
        [mgetr] (mem right)]
    {:deps [right left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [ridx (mgetr db {})
                              xf (fn [rf]
                                   (fn
                                     ([acc] (rf acc))
                                     ([acc row]
                                      (reduce
                                        (fn [acc rrow]
                                          (rf acc (->Joined row rrow)))
                                        acc
                                        (seekr ridx row)))))]
                          (forward db (eduction xf inserted) (eduction xf deleted))))
                 right (fn [db inserted deleted forward]
                         (let [lidx (mgetl db {})
                               xf (fn [rf]
                                    (fn
                                      ([acc] (rf acc))
                                      ([acc rrow]
                                       (reduce
                                         (fn [acc row]
                                           (rf acc (->Joined row rrow)))
                                         acc
                                         (seekl lidx rrow)))))]
                           (forward db (eduction xf inserted) (eduction xf deleted)))))}))

(defrecord JoinColl [left coll])

(defn join-coll
  "Emits (JoinColl row rrows) nodes, like join but always emits a row (e.g for left-join) and the right rows are grouped."
  [graph self left seekl right seekr]
  (let [[mget mset] (mem self)
        [mgetl] (mem left)
        [mgetr] (mem right)]
    {:deps [right left]
     :flow (flow
             left (fn [db inserted deleted forward]
                    (let [idx (mget db {})
                          ridx (mgetr db {})

                          adds (mutable-set)
                          dels (mutable-set)

                          nidx
                          (reduce
                            (fn [idx row]
                              (let [j (idx row)]
                                (if j
                                  (do (add-to-mutable-set dels j)
                                      (dissoc idx row))
                                  idx)))
                            idx
                            deleted)

                          nidx
                          (reduce
                            (fn [idx row]
                              (let [j (idx row)]
                                (when j
                                  (add-to-mutable-set dels j))
                                (let [nj (->JoinColl row (seekr ridx row))]
                                  (add-to-mutable-set adds nj)
                                  (assoc idx row nj))))
                            nidx
                            inserted)

                          adds (iterable-mut-set adds)
                          dels (iterable-mut-set dels)

                          db (mset db nidx)]
                      (forward db adds dels)))
             right (fn [db inserted deleted forward]
                     (let [idx (mget db {})
                           lidx (mgetl db {})
                           ridx (mgetr db {})

                           adds (mutable-set)
                           dels (mutable-set)

                           lrows (into #{} (comp cat (mapcat #(seekl lidx %))) [inserted deleted])

                           nidx
                           (reduce
                             (fn [idx row]
                               (let [j (idx row)
                                     nj (->JoinColl row (seekr ridx row))]
                                 (add-to-mutable-set dels j)
                                 (add-to-mutable-set adds nj)
                                 (assoc idx row nj)))
                             idx
                             lrows)

                           adds (iterable-mut-set adds)
                           dels (iterable-mut-set dels)

                           db (mset db nidx)]
                       (forward db adds dels))))

     :provide (fn [db] (some-> db mget vals))}))

(defn left-join [graph self left seekl right seekr]
  (conj
    left
    [join-coll seekl right seekr]
    [expand (fn [{:keys [left coll]}]
              (if (seq coll)
                coll
                [nil]))]
    [transform-unsafe #(update % :left :left)]))

(defn save-set [graph self left]
  (let [[mget mset] (mem self)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [os (mget db)
                              s (or os #{})
                              s (transient s)
                              s (reduce disj! s deleted)
                              s (reduce conj! s inserted)
                              ns (persistent! s)
                              db (mset db ns)]
                          (if os
                            (forward db inserted deleted)
                            (forward db ns deleted)))))
     :provide (fn [db] (not-empty (mget db)))}))

(defn- enumerate-nested-map-of-sets [m depth]
  (case depth
    0 (eduction cat (vals m))
    1 (eduction cat (vals m))
    2 (eduction (comp (mapcat vals) cat) (vals m))
    (let [xf (apply comp (repeat (- depth 1) (mapcat vals)))]
      (eduction (comp xf cat) (vals m)))))

(defn save-hash [graph self left fns]
  (let [path (if (empty? fns) (constantly []) (apply juxt fns))
        add-row (fn [m row] (update-in m (path row) set-conj row))
        del-row (fn [m row] (disjoc-in m (path row) row))
        [mget mset] (mem self)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [m (mget db)
                              m (reduce del-row m deleted)
                              m (reduce add-row m inserted)
                              db (mset db m)]
                          (forward db inserted deleted))))
     :provide (fn [db] (when-some [m (not-empty (mget db))]
                         (enumerate-nested-map-of-sets m (count fns))))}))

(defn save-btree [graph self left fns]
  (let [path (if (empty? fns) (constantly []) (apply juxt fns))
        sm (sorted-map)
        add-row (fn [m row] (update-in2 m sm (path row) set-conj row))
        del-row (fn [m row] (disjoc-in m (path row) row))
        [mget mset] (mem self)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [m (mget db sm)
                              m (reduce del-row m deleted)
                              m (reduce add-row m inserted)
                              db (mset db m)]
                          (forward db inserted deleted))))
     :provide (fn [db] (when-some [m (not-empty (mget db))]
                         (enumerate-nested-map-of-sets m (count fns))))}))

(defn- enumerate-nested-map-of-maps [m depth]
  (case depth
    0 (vals m)
    1 (vals m)
    2 (eduction (mapcat vals) (vals m))
    (let [xf (apply comp (repeat (- depth 1) (mapcat vals)))]
      (eduction comp xf (vals m)))))

(defn save-unique [graph self left fns]
  (let [path (if (empty? fns) (constantly []) (apply juxt fns))
        raise-violation (fn [old-row new-row] (raise "Unique constraint violation" {:relvar left, :old-row old-row, :new-row new-row}))
        on-collision (fn on-collision [old-row new-row] (raise-violation old-row new-row))
        replace-fn (fn [old-row row] (cond (nil? old-row) row
                                           (= old-row row) row
                                           :else (on-collision old-row row)))
        add-row (fn [m row] (update-in m (path row) replace-fn row))
        del-row (fn [m row] (dissoc-in m (path row)))
        [mget mset] (mem self)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [m (mget db)
                              m (reduce del-row m deleted)
                              m (reduce add-row m inserted)
                              db (mset db m)]
                          (forward db inserted deleted))))
     :provide (fn [db] (when-some [m (not-empty (mget db))] (enumerate-nested-map-of-maps m (count fns))))}))

(def ^:dynamic *foreign-key-violations* nil)
(def ^:dynamic *foreign-key-cascades* nil)

(defn- bad-fk [left right clause row cascade]
  (when (and (= cascade :delete) *foreign-key-cascades*)
    (set! *foreign-key-cascades* (update *foreign-key-cascades* [left right clause] set-conj (dissoc row ::fk))))
  (if *foreign-key-violations*
    (set! *foreign-key-violations* (update *foreign-key-violations* [left right clause] set-conj (dissoc row ::fk)))
    (raise "Foreign key violation" {:relvar left
                                    :references right
                                    :clause clause
                                    :row (dissoc row ::fk)})))

(defn- good-fk [left right clause row cascade]
  (when (and (= cascade :delete) *foreign-key-cascades*)
    (set! *foreign-key-cascades* (disjoc *foreign-key-cascades* [left right clause] (dissoc row ::fk))))
  (when *foreign-key-violations*
    (set! *foreign-key-violations* (disjoc *foreign-key-violations* [left right clause] (dissoc row ::fk)))))

(defn fk [graph self left seekl right seekr clause origin references {:keys [cascade]}]
  (let [good #(good-fk origin references clause % cascade)
        bad #(bad-fk origin references clause % cascade)
        on-insert (fn [{row :left
                        :keys [coll]
                        :as m}]
                    (if (empty? coll)
                      (bad row)
                      (good row)))]
    (conj (pop left)
          [runf identity (fn [row]
                           (good row))]
          (peek left)
          [join-coll seekl right seekr]
          [runf on-insert identity]
          [transform-unsafe :left])))

(defn- flow-noop [db & _]
  db)

(defn- flow-pass [db inserted deleted forward]
  (forward db inserted deleted))

(defn add-implicit-joins [thunk & remainder]
  (binding [*env-deps* #{}
            *implicit-joins* #{}]
    (let [relvar (thunk)
          env-deps *env-deps*
          joins *implicit-joins*

          join-statements
          (mapv (fn [[relvar clause :as jk]]
                  [:join-as-coll relvar clause jk {:unsafe true}]) joins)

          env-join
          (when (seq env-deps)
            [:left-join
             (conj [[:from ::Env]] [:select [::env [select-keys ::env env-deps]]])
             {}])

          without-cols
          (if (seq env-deps)
            [::env]
            [])

          without-cols (into without-cols joins)]

      (if (or (seq joins) env-join)
        (let [left (left-relvar relvar)
              stmt (head-stmt relvar)
              left (if env-join (conj left env-join) left)
              left (reduce conj left join-statements)
              left (conj left stmt)
              left (reduce conj left remainder)]
          (conj left (into [:without-unsafe] without-cols)))
        (reduce conj relvar remainder)))))

(defn- join-merge [{:keys [left, right]}] (overwrite-keys left right))

(defn- add-join [left right clause graph]
  (add-implicit-joins
    (fn []
      (let [[left seekl] (find-hash-index graph left (keys clause) (vals clause))
            [right seekr] (find-hash-index graph right (vals clause) (keys clause))]
        (conj left [join seekl right seekr] [transform join-merge])))))

(defn- add-left-join [left right clause graph]
  (add-implicit-joins
    (fn []
      (let [[left seekl] (find-hash-index graph left (keys clause) (vals clause))
            [right seekr] (find-hash-index graph right (vals clause) (keys clause))]
        (conj left
              [left-join seekl right seekr]
              [transform join-merge])))))

(defn- project-fn [kvec]
  (case (count kvec)
    0 (constantly {})
    #(select-keys % kvec)))

(defn- pass [graph _ left]
  {:deps [left]
   :flow (flow left flow-pass)})

(defrecord Grouped [group value])

(defn bind-group
  [binding f]
  (fn [{:keys [group value]}]
    (assoc group binding (f value))))

(defn group [graph self left cols elfn]
  (let [cols (vec (set cols))
        group-fn (if (empty? cols) (constantly []) (apply juxt cols))
        group->row #(zipmap cols %)
        adder (if (= identity elfn)
                (fn [idx row changed]
                  (let [group (group-fn row)
                        eset (idx group #{})
                        nset (conj eset row)]
                    (if (same-size? eset nset)
                      idx
                      (do (add-to-mutable-set changed group)
                          (assoc idx group nset)))))
                (fn [idx row changed]
                  (let [group (group-fn row)
                        [rows eset] (idx group)
                        nrows (set-conj rows row)
                        nset (if-some [v (elfn row)] (set-conj eset v) eset)]
                    (if (same-size? nrows rows)
                      idx
                      (do (when-not (identical? nset eset)
                            (add-to-mutable-set changed group))
                          (assoc idx group [nrows nset]))))))
        deleter (if (= identity elfn)
                  (fn [idx row changed]
                    (let [group (group-fn row)
                          eset (idx group #{})
                          nset (disj eset row)]
                      (if (same-size? eset nset)
                        idx
                        (do (add-to-mutable-set changed group)
                            (if (empty? nset)
                              (dissoc idx group)
                              (assoc idx group nset))))))
                  (fn [idx row changed]
                    (let [group (group-fn row)
                          [rows eset] (idx group)
                          nrows (disj rows row)
                          nset (if-some [v (elfn row)] (disj eset v) eset)]
                      (if (same-size? nrows rows)
                        idx
                        (do (when-not (same-size? nset eset)
                              (add-to-mutable-set changed group))
                            (if (empty? nrows)
                              (dissoc idx group)
                              (assoc idx group [nrows nset])))))))

        sfn (if (= identity elfn)
              identity
              #(nth % 1))

        [mget mset] (mem self)]
    {:deps [left]
     :flow (flow
             left
             (fn [db inserted deleted forward]
               (let [idx (mget db {})

                     changed (mutable-set)

                     nidx (reduce #(deleter %1 %2 changed) idx deleted)
                     nidx (reduce #(adder %1 %2 changed) nidx inserted)
                     db (mset db nidx)

                     changed (iterable-mut-set changed)

                     deleted
                     (eduction
                       (keep
                         (fn [group]
                           (when-some [entry (idx group)]
                             (->Grouped (group->row group) (sfn entry)))))
                       changed)

                     inserted
                     (eduction
                       (keep
                         (fn [group]
                           (when-some [entry (nidx group)]
                             (->Grouped (group->row group) (sfn entry)))))
                       changed)]
                 (forward db inserted deleted))))
     :provide
     (fn [db]
       (when-some [idx (mget db)]
         (eduction (map (fn [[group entry]] (->Grouped (group->row group) (sfn entry)))) idx)))}))

(defn sorted-group [graph self left cols elfn]
  (let [cols (vec (set cols))
        group-fn (if (empty? cols) (constantly []) (apply juxt cols))
        group->row #(zipmap cols %)

        add-row (fn [coll row]
                  (if coll
                    (update coll (elfn row) set-conj row)
                    (update (sorted-map) (elfn row) set-conj row)))
        rem-row (fn [coll row]
                  (if coll
                    (disjoc coll (elfn row) row)
                    (sorted-map)))

        adder (fn [idx row changed]
                (let [group (group-fn row)
                      ev (idx group)
                      nv (add-row ev row)]
                  (if (identical? ev nv)
                    idx
                    (do
                      (add-to-mutable-set changed group)
                      (assoc idx group nv)))))

        deleter (fn [idx row changed]
                  (let [group (group-fn row)
                        ev (idx group)
                        nv (rem-row ev row)]
                    (if (identical? ev nv)
                      idx
                      (do
                        (del-from-mutable-set changed group)
                        (if (empty? nv)
                          (dissoc idx group)
                          (assoc idx group nv))))))

        sfn identity
        [mget mset] (mem self)]
    {:deps [left]
     :flow (flow
             left
             (fn [db inserted deleted forward]
               (let [idx (mget db {})

                     changed (mutable-set)

                     nidx (reduce #(deleter %1 %2 changed) idx deleted)
                     nidx (reduce #(adder %1 %2 changed) nidx inserted)
                     db (mset db nidx)

                     changed (iterable-mut-set changed)

                     deleted
                     (eduction
                       (keep
                         (fn [group]
                           (when-some [entry (idx group)]
                             (->Grouped (group->row group) (sfn entry)))))
                       changed)

                     inserted
                     (eduction
                       (keep
                         (fn [group]
                           (when-some [entry (nidx group)]
                             (->Grouped (group->row group) (sfn entry)))))
                       changed)]

                 (forward db inserted deleted))))
     :provide (fn [db]
                (when-some [idx (mget db)]
                  (eduction (map (fn [[group entry]] (->Grouped (group->row group) (sfn entry)))) idx)))}))

(defn row-count
  "A relic agg function that can be used in :agg expressions to calculate the number of rows in a relation."
  ([]
   {:custom-node (fn [left cols [binding]]
                   (conj left
                         [group cols identity]
                         [transform-unsafe (bind-group binding count)]))})
  ([expr]
   (let [f (row-fn [:if expr :%])]
     {:custom-node (fn [left cols [binding]]
                     (conj left
                           [group cols f]
                           [transform-unsafe (bind-group binding count)]))})))

(defn max-by [expr]
  (let [f (row-fn expr)
        rf (fn
             ([] nil)
             ([a] a)
             ([a b]
              (cond
                (nil? a) b
                (nil? b) a
                :else
                (let [n (compare (f a) (f b))]
                  (if (< n 0)
                    b
                    a)))))]
    {:combiner rf
     :reducer (fn [rows] (reduce rf rows))}))

(defn min-by [expr]
  (let [f (row-fn expr)
        rf (fn
             ([] nil)
             ([a] a)
             ([a b]
              (cond
                (nil? a) b
                (nil? b) a
                :else
                (let [n (compare (f a) (f b))]
                  (if (< n 0)
                    a
                    b)))))]
    {:combiner rf
     :reducer (fn [rows] (reduce rf rows))}))

(defn- comp-complete [aggregate-map f]
  (if-some [f2 (:complete aggregate-map)]
    (assoc aggregate-map :complete (comp f f2))
    (assoc aggregate-map :complete f)))

(defn max-agg [expr]
  (comp-complete (max-by expr) (row-fn expr)))

(defn min-agg [expr]
  (comp-complete (min-by expr) (row-fn expr)))

;; --
;; agg expressions e.g [rel/sum :foo]

(defn- agg-expr-agg [expr]
  (cond
    (map? expr) expr

    (#{count 'count `count} expr) (row-count)

    (fn? expr) (expr)

    (vector? expr)
    (let [[f & args] expr]
      (cond
        (#{count 'count `count} f) (apply row-count args)
        (#{min 'min `min} f) (apply min-agg args)
        (#{max 'max `max} f) (apply max-agg args)
        :else (apply f args)))

    :else (throw (ex-info "Not a valid agg form, expected a agg function or vector" {:expr expr}))))

;; --
;;

(defn- agg-form-fn [form]
  (let [[binding expr] form
        {:keys [combiner reducer complete]} (agg-expr-agg expr)]
    (assert (keyword? binding) "Only keyword bindings are not permitted for agg forms")
    (assert (keyword? binding) "* bindings are not permitted for agg forms")
    {:complete (if complete (fn [m] (update m binding complete)) identity)
     :combiner
     (fn [m a b] (assoc m binding (combiner (binding a) (binding b))))
     :reducer
     (fn [m rows] (assoc m binding (reducer rows)))}))

(defn- agg-fns [forms]
  (case (count forms)
    1
    (let [[binding expr] (first forms)
          agg (agg-expr-agg expr)]
      (assoc agg :merger #(assoc %1 binding %2)))
    (let [fs (mapv agg-form-fn forms)
          reducer-fns (mapv :reducer fs)
          combiner-fns (mapv :combiner fs)
          complete-fns (mapv :complete fs)]
      {:complete (fn [m] (reduce (fn [m f] (f m)) m complete-fns))
       :reducer (fn [rows] (reduce (fn [m reducer-fn] (reducer-fn m rows)) {} reducer-fns))
       :combiner (fn [a b] (reduce (fn [m combiner-fn] (combiner-fn m a b)) {} combiner-fns))})))

(defn- realise-collection [maybe-eduction]
  (if (coll? maybe-eduction)
    maybe-eduction
    (vec maybe-eduction)))

(defn generic-agg [graph self left cols aggs]
  (let [key-fn (project-fn cols)
        key-xf (map key-fn)
        {:keys [reducer combiner complete merger]} (agg-fns aggs)
        complete (or complete identity)
        merge-fn (or merger merge)
        empty-idx (gai/index key-fn reducer combiner complete merge-fn 32)
        [mget mset] (mem self)
        get-row (fn [idx k] (when-some [{:keys [indexed-row]} (idx k)] @indexed-row))]
    {:deps [left]
     :flow (flow
             left
             (fn [db inserted deleted forward]
               (let [idx (mget db empty-idx)
                     deleted (realise-collection deleted)
                     inserted (realise-collection inserted)
                     ks (into #{} key-xf deleted)
                     ks (into ks key-xf inserted)
                     nidx (reduce gai/unindex-row idx deleted)
                     nidx (reduce gai/index-row nidx inserted)
                     db (mset db nidx)
                     old-rows
                     (eduction
                       (keep (partial get-row idx))
                       ks)
                     new-rows
                     (eduction
                       (keep (partial get-row nidx))
                       ks)]
                 (forward db new-rows old-rows))))}))

(defn- get-custom-agg-node [left cols [_ expr :as agg]]
  (when-some [n (:custom-node (agg-expr-agg expr))]
    (n left cols agg)))

(defn agg [graph self left cols aggs]
  (if (empty? aggs)
    (conj left [transform (project-fn (vec (set cols)))])
    (let [col-set (set cols)
          cols (vec col-set)
          join-clause (zipmap cols cols)
          _ (run! (fn [[binding]]
                    (when (contains? col-set binding)
                      (raise "Cannot bind aggregate to a grouping key"))) aggs)
          [custom-nodes standard-aggs] ((juxt keep remove) #(get-custom-agg-node left cols %) aggs)]
      (cond
        (empty? custom-nodes) (conj left [generic-agg cols standard-aggs])
        (and (= 1 (count custom-nodes)) (empty? standard-aggs)) (first custom-nodes)
        :else
        (let [joins (concat (for [node custom-nodes]
                              [:join node join-clause])
                            (when (seq standard-aggs)
                              [[:join (conj left [generic-agg cols standard-aggs]) join-clause]]))
              left (conj left (into [:select] cols))
              left (reduce conj left joins)]
          left)))))

(declare full-dependencies)

(defn lookup [graph self _ index path]
  (let [path (vec path)
        [iget] (mem index)]
    (when-not (graph (id index))
      (raise ":lookup used but index is not materialized" {:index index}))
    (when-not (= (count path) (dec (count (head-stmt index))))
      (raise ":lookup path length must currently match indexed expressions"))
    {:provide (fn [db]
                (when-some [i (iget db)]
                  (get-in i path)))}))

(defn- require-set
  "Certain functions will require that an intermediate relvar is materialized as a set.

  Returns a (potentially new) relvar that ensures mget will return a set of rows at all times."
  [relvar]
  (if (empty? relvar)
    relvar
    (case (operator (head-stmt relvar))
      :set relvar
      (if (unwrap-table-key relvar)
        relvar
        (conj relvar [:set])))))

(defn- relvar-or-dual [relvar]
  (if (empty? relvar)
    [[:const [{}]]]
    relvar))

(defn to-dataflow* [graph relvar self]
  (let [relvar (to-relvar relvar)
        left (left-relvar relvar)
        stmt (head-stmt relvar)
        operator (operator stmt)]
    (if (fn? operator)
      (let [[op & args] stmt] (apply op graph self left args))
      (case operator
        :from
        (let [[_ relvar] stmt
              relvar (if (keyword? relvar)
                       [[:table relvar]]
                       relvar)]
          (conj relvar [pass]))

        :where
        (let [[_ & exprs] stmt]
          (add-implicit-joins #(conj left [where (where-pred exprs)])))

        :without
        (let [[_ & cols] stmt]
          (conj left [transform (without-fn cols)]))

        :without-unsafe
        (let [[_ & cols] stmt]
          (conj left [transform-unsafe (without-fn cols)]))

        :join-as-coll
        (let [[_ right clause binding {:keys [unsafe]}] stmt
              right (to-relvar right)]
          (add-implicit-joins
            (fn []
              (let [[left seekl] (find-hash-index graph left (keys clause) (vals clause))
                    [right seekr] (find-hash-index graph right (vals clause) (keys clause))]
                (conj left
                      [join-coll seekl right seekr]
                      [(if unsafe transform-unsafe transform)
                       (fn [{:keys [left coll]}]
                         (assoc left binding coll))])))))

        :join
        (let [[_ & joins] stmt]
          (reduce (fn [left [right clause]] (add-join left (to-relvar right) clause graph)) left (partition-all 2 joins)))

        :left-join
        (let [[_ & joins] stmt]
          (reduce (fn [left [right clause]] (add-left-join left (to-relvar right) clause graph)) left (partition-all 2 joins)))

        :extend
        (let [[_ & extends] stmt]
          (add-implicit-joins #(conj left [transform (extend-fn extends)])))

        :extend-unsafe
        (let [[_ & extends] stmt]
          (add-implicit-joins #(conj left [transform-unsafe (extend-fn extends)])))

        :expand
        (let [[_ & expansions] stmt]
          (add-implicit-joins #(conj left [expand (expand-fn expansions)]) [transform join-merge]))

        :select
        (let [[_ & selections] stmt
              left (relvar-or-dual left)
              cols (filterv keyword? selections)
              extensions (filterv vector? selections)
              deps (vec (into (set cols) (mapcat extend-form-cols) extensions))
              project (project-fn deps)]
          (if (seq extensions)
            (conj left (into [:extend] extensions) [transform project])
            (conj left [transform project])))

        :select-unsafe
        (let [[_ & selections] stmt
              left (relvar-or-dual left)
              cols (filterv keyword? selections)
              extensions (filterv vector? selections)
              deps (vec (into (set cols) (mapcat extend-form-cols) extensions))
              project (project-fn deps)]
          (if (seq extensions)
            (add-implicit-joins #(conj left [transform-unsafe (comp project (extend-fn extensions))]))
            (conj left [transform-unsafe project])))

        :set
        (let [[_] stmt]
          (conj left [save-set]))

        :hash
        (let [[_ & exprs] stmt]
          (add-implicit-joins #(conj left [save-hash (mapv row-fn exprs)])))

        :btree
        (let [[_ & exprs] stmt]
          (add-implicit-joins #(conj left [save-btree (mapv row-fn exprs)])))

        :check
        (let [[_ & checks] stmt
              left (require-set left)]
          (add-implicit-joins #(conj left [runf (check-fn left checks) identity])))

        :req
        (let [[_ & cols] stmt]
          (conj left (into [:check] (map #(req-col-check left %)) cols)))

        :fk
        (let [[_ right clause opts] stmt]
          (add-implicit-joins
            (fn []
              (let [references (to-relvar right)
                    origin left
                    _ (when (and (:cascade opts) (not (unwrap-table origin)))
                        (raise "Cascading :fk constraints are only allowed on table relvars" {:relvar origin, :references references, :clause clause}))
                    [left seekl] (find-hash-index graph left (keys clause) (vals clause))
                    [right seekr] (find-hash-index graph references (vals clause) (keys clause))]
                (conj left [fk seekl right seekr clause origin references opts])))))

        :unique
        (let [[_ & exprs] stmt]
          (add-implicit-joins #(conj left [save-unique (mapv row-fn exprs)])))

        :constrain
        (let [[_ & constraints] stmt]
          (reduce conj left constraints))

        :const
        (let [[_ coll] stmt]
          [[provide (constantly coll)]])

        :intersection
        (let [[_ & relvars] stmt]
          (reduce conj (require-set left) (mapv (fn [r] [intersection (require-set (to-relvar r))]) relvars)))

        :union
        (let [[_ & relvars] stmt
              left (require-set left)]
          (reduce conj (require-set left) (mapv (fn [r] [union (require-set (to-relvar r))]) relvars)))

        :difference
        (let [[_ & relvars] stmt
              left (require-set left)]
          (reduce conj (require-set left) (mapv (fn [r] [difference (require-set (to-relvar r))]) relvars)))

        :table
        (let [[_ table-key] stmt]
          {:deps []
           :table table-key
           :flow {nil flow-pass}
           :provide (fn [db] (db table-key))})

        :agg
        (let [[_ cols & aggs] stmt]
          (conj left [agg cols aggs]))

        :qualify
        (let [[_ namespace] stmt]
          (conj left [transform #(reduce-kv (fn [m k v] (assoc m (keyword namespace (name k)) v)) {} %)]))

        :rename
        (let [[_ renames] stmt]
          (conj left [transform #(set/rename-keys % renames)]))

        :lookup
        (let [[_ index & path] stmt]
          [[lookup index path]])))))

(defn to-dataflow [graph relvar]
  (let [ret (to-dataflow* graph relvar relvar)]
    ((fn ! [ret]
       (if (vector? ret)
         (! (to-dataflow* graph ret relvar))
         ret)) ret)))

(defn full-dependencies [relvar]
  ((fn rf [s relvar]
     (if (empty? relvar)
       s
       (if-some [table (unwrap-table relvar)]
         (conj s table)
         (let [stmt (head-stmt relvar)
               op (operator stmt)]
           (case op
             :lookup (let [[_ i] stmt] (full-dependencies i))
             (let [{:keys [deps]} (to-dataflow {} relvar)]
               (reduce rf s deps)))))))
   #{} relvar))

(defn dependencies
  "Returns the (table name) dependencies of the relvar, e.g what tables it could be affected by."
  [relvar]
  (distinct (map #(nth (first %) 1 nil) (full-dependencies relvar))))

(defn- tag-generation [graph node-id generation]
  (let [{e-gen :generation
         :keys [provide
                deps]
         :as node} (graph node-id)]
    (if (and provide e-gen)
      graph
      (let [graph (assoc graph node-id (assoc node :generation generation))]
        (reduce #(tag-generation %1 (id %2) generation) graph deps)))))

(defn- depend [graph dep dependent]
  (let [{:keys [dependents]
         :or {dependents #{}}
         :as node} (graph dep)
        dependents (conj dependents dependent)
        node (assoc node :dependents dependents)
        graph (assoc graph dep node)]
    graph))

(defn- undepend [graph dep dependent]
  (let [{:keys [dependents]
         :or {dependents #{}}
         :as node} (graph dep)
        dependents (disj dependents dependent)
        node (assoc node :dependents dependents)]
    (assoc graph dep node)))

(defn- unlink [graph table-key]
  (let [tables (::tables graph {})
        id (tables table-key)]
    (if id
      (update graph id dissoc :linked)
      graph)))

(defn- add-to-graph* [graph relvar]
  (cond
    (empty? relvar) graph
    (contains? graph (*ids* relvar)) graph
    :else
    (let [{:keys [deps table] :as node} (to-dataflow graph relvar)
          nid (id relvar)
          graph (assoc graph nid (assoc node :relvar relvar :score 0))
          graph (if table (update graph ::tables assoc table nid) graph)
          graph (reduce add-to-graph* graph deps)
          graph (reduce #(depend %1 (id %2) nid) graph deps)
          graph (tag-generation graph nid (::generation graph 0))
          graph (reduce unlink graph (dependencies relvar))]
      graph)))

(defn add-to-graph [graph relvar]
  (binding [*ids* (::ids graph {})
            *idn* (::idn graph 0)]
    (let [graph (add-to-graph* graph relvar)
          graph (assoc graph ::ids *ids* ::idn *idn*)]
      graph)))

(defn del-from-graph* [graph relvar]
  (cond
    (= [] relvar) graph
    (contains? (::materialized graph) relvar) graph
    (contains? (::watched graph) (get-id graph relvar)) graph
    (not (contains? graph (get-id graph relvar))) graph
    :else
    (let [{:keys [dependents
                  deps
                  table]} (graph (id relvar))]
      (if (seq dependents)
        graph
        (let [nid (id relvar)
              graph (dissoc graph nid)
              graph (if table (update graph ::tables dissoc table) graph)
              graph (reduce #(undepend %1 (id %2) nid) graph deps)
              graph (reduce del-from-graph* graph deps)
              _ (set! *ids* (dissoc *ids* relvar))
              graph (reduce unlink graph (dependencies relvar))]
          graph)))))

(defn del-from-graph [graph relvar]
  (binding [*ids* (::ids graph {})
            *idn* (::idn graph 0)]
    (let [graph (del-from-graph* graph relvar)
          graph (assoc graph ::ids *ids* ::idn *idn*)]
      graph)))

(defn inc-generation [graph]
  (assoc graph ::generation (inc (::generation graph 0))))

(def ^:private ^:dynamic *tracking* nil)

(defn- track [id added deleted]
  (when (and *tracking* (*tracking* id))
    (let [ntracking (update *tracking* id (fn [{:keys [adds dels]}]
                                            {:adds (reduce add-to-mutable-list (or adds (mutable-list)) added)
                                             :dels (reduce add-to-mutable-list (or dels (mutable-list)) deleted)}))]
      (set! *tracking* ntracking)))
  nil)

(defn- tracked-flow [id f]
  (fn tracking-flow [db inserted deleted]
    (track id inserted deleted)
    (f db inserted deleted)))

(defn link
  [graph table-key]
  (letfn [(sort-dependents [dependents]
            (sort-by (comp :score graph) > dependents))
          (forward-fn [graph dependents]
            (case (count dependents)
              0 flow-noop
              1 (:linked (graph (first dependents)))
              (let [dependents (sort-dependents dependents)
                    fns (mapv (comp :linked graph) dependents)]
                (fn flow-fan-out [db inserted deleted]
                  (reduce
                    (fn flow-step [db f] (f db inserted deleted))
                    db
                    fns)))))
          (build-fn [graph edge id]
            (let [{:keys [dependents
                          flow]
                   :as node} (graph id)
                  dependents (sort-dependents dependents)
                  flow-fn (get flow edge flow-noop)
                  graph (reduce #(build-fn %1 id %2) graph dependents)
                  forward (tracked-flow id (forward-fn graph dependents))
                  linked (fn flow [db inserted deleted] (flow-fn db inserted deleted forward))]
              (assoc graph id (assoc node :linked linked))))]
    (if-some [table-id (-> graph ::tables (get table-key))]
      (build-fn graph nil table-id)
      graph)))

;; d1, d2* d3
;;  |  |   |
;; c1, c2  |
;; _|__|___|
;; b1
;; |
;; a1 provide

;; a1,b1,c1,d1 have generation 0
;; add d2 to graph with generation 1
;; we need to recur remove the generation on deps until we hit a :provide node

(defn init [graph relvar]
  (let [new-generation (::generation graph)
        provided (volatile! #{})]
    (letfn [(id [relvar] (get-id graph relvar))
            (sort-dependents [dependents]
              (sort-by (comp :score graph) > dependents))
            (forward-fn [graph dep dependents]
              (case (count dependents)
                0 flow-noop
                1 (get (:init (graph (first dependents))) dep)
                (let [dependents (sort-dependents dependents)
                      fns (mapv (comp #(get % dep) :init graph) dependents)]
                  (fn flow-fan-out [db inserted deleted]
                    (reduce
                      (fn flow-step [db f] (f db inserted deleted))
                      db
                      fns)))))
            (dirty? [graph id]
              (let [generation (:generation (graph id))]
                (cond
                  (= new-generation generation) true
                  generation false
                  :else true)))
            (dirty [graph id]
              (let [{:keys [dependents]} (graph id)]
                (filterv #(dirty? graph %) dependents)))
            (link-uninitialised [graph uninit]
              (let [{:keys [deps
                            flow]
                     :as node} (graph uninit)
                    dirty (sort-dependents (dirty graph uninit))
                    graph (reduce link-uninitialised graph dirty)
                    forward (forward-fn graph uninit dirty)
                    init (reduce
                           (fn [m edge]
                             (let [dep (id edge)
                                   flow-fn (get flow dep flow-noop)]
                               (assoc m dep (fn init [db inserted deleted]
                                              (flow-fn db inserted deleted forward)))))
                           {}
                           deps)]
                (assoc graph uninit (assoc node :init init :init-forward forward))))
            (init [graph relvar]
              (let [{:keys [deps
                            generation
                            provide
                            results]} (graph (id relvar))

                    already-fired (contains? @provided (id relvar))

                    rs (when-not already-fired
                         (or results (when (and provide
                                                (or (not= generation new-generation)
                                                    (empty? deps)))
                                       (provide graph))))]

                (cond
                  already-fired graph

                  rs
                  (let [graph (link-uninitialised graph (id relvar))
                        f (:init-forward (graph (id relvar)))]
                    (vswap! provided conj (id relvar))
                    (f graph rs nil))

                  :else
                  (reduce init graph deps))))]
      (init graph relvar))))

(defn- result [graph self left box]
  (let [swap (fn [existing inserted deleted]
               (if existing
                 (let [e (transient (set existing))
                       e (reduce disj! e deleted)
                       e (reduce conj! e inserted)]
                   (persistent! e))
                 inserted))]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (vswap! box swap inserted deleted)
                        db))}))

(defn gg [db] (::graph (meta db) {}))
(defn- sg [db graph] (vary-meta db assoc ::graph graph))

(defn qraw
  "A version of query who's return type is undefined, just some seqable/reducable collection of rows."
  [db query]
  (if (keyword? query)
    (query db)
    (let [graph (gg db)]
      (or (:results (graph (get-id graph query)))
          (let [rbox (volatile! nil)
                query (conj query [result rbox])
                graph (inc-generation graph)
                graph (add-to-graph graph query)
                _ (init graph query)]
            @rbox)))))

(defn q
  [db query]
  (seq (qraw db query)))

(defn- mat [graph self left]
  (let [left-id (id left)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted _]
                        (let [s (:results (db left-id) #{})
                              s (transient s)
                              s (reduce disj! s deleted)
                              s (reduce conj! s inserted)]
                          (update db left-id assoc :results (persistent! s)))))
     :provide (fn [db] (:results (db left)))}))

(defn materialize
  ([db relvar] (materialize db relvar {}))
  ([db relvar opts]
   (if (contains? (::materialized (gg db)) relvar)
     db
     (let [graph (gg db)
           graph (inc-generation graph)
           graph (if (:ephemeral opts) graph (update graph ::materialized set-conj relvar))
           relvar (conj relvar [mat])
           graph (add-to-graph graph relvar)
           graph (init graph relvar)]
       (sg db graph)))))

(defn dematerialize [db relvar]
  (let [graph (gg db)
        graph (update graph ::materialized disj relvar)
        relvar (conj relvar [mat])
        graph (del-from-graph graph relvar)]
    (sg db graph)))

(defn change-table [db table-key inserts deletes]
  (let [graph (gg db)
        table-id (-> graph ::tables (get table-key))
        {:keys [linked
                relvar]} (graph table-id)]
    (if linked
      (let [data (graph table-key)
            drows (when deletes (if data (into [] (filter data) deletes) (set deletes)))
            ndata (if drows (reduce disj data drows) data)
            nrows (if ndata (into [] (remove ndata) inserts) (set inserts))
            ndata (if ndata (into ndata nrows) nrows)


            graph (assoc graph table-key ndata)
            db (assoc db table-key ndata)
            graph (linked graph nrows drows)]
        (sg db graph))
      (if relvar
        (let [graph (link graph table-key)]
          (change-table (sg db graph) table-key inserts deletes))
        (let [data (graph table-key)
              drows (when deletes (if data (into [] (filter data) deletes) (set deletes)))
              ndata (if data (into data inserts) (set inserts))
              ndata (if drows (reduce disj ndata drows) ndata)
              graph (assoc graph table-key ndata)
              db (assoc db table-key ndata)]
          (sg db graph))))))

(defn watch [db relvar]
  (let [relvar (to-relvar relvar)
        db (materialize db relvar {:ephemeral true})
        graph (gg db)
        graph (update graph ::watched set-conj (get-id graph relvar))]
    (sg db graph)))

(defn unwatch [db relvar]
  (let [relvar (to-relvar relvar)
        graph (gg db)
        graph (update graph ::watched disj (get-id graph relvar))
        db (sg db graph)
        db (if (contains? (::materialized graph) relvar)
             db
             (dematerialize db relvar))]
    db))

(defn- to-table-key [k-or-relvar]
  (if (keyword? k-or-relvar)
    k-or-relvar
    (unwrap-table-key k-or-relvar)))

(defn- delete-where [db table-key exprs]
  (let [rows (q db (conj [[:table table-key]] (into [:where] exprs)))]
    (if (seq rows)
      (change-table db table-key nil rows)
      db)))

(defn- update-f-or-set-map-to-fn [f-or-set-map]
  (if (map? f-or-set-map)
    (reduce-kv (fn [f k e] (comp f (let [f2 (row-fn e)] #(overwrite-key % k (f2 %))))) identity f-or-set-map)
    f-or-set-map))

(defn- update-where [db table-key f-or-set-map exprs]
  (let [rows (q db (conj [[:table table-key]] (into [:where] exprs)))
        f (update-f-or-set-map-to-fn f-or-set-map)
        new-rows (eduction (map f) rows)]
    (if (seq rows)
      (change-table db table-key new-rows rows)
      db)))

(defn materialized? [db relvar]
  (contains? (::materialized (gg db)) relvar))

(defn- head-operator [relvar]
  (when (vector? relvar)
    (some-> relvar head-stmt operator)))

(defn index? [relvar] (#{:hash :btree :unique} (head-operator relvar)))
(defn unique? [relvar] (= :unique (head-operator relvar)))
(defn hash? [relvar] (= :hash (head-operator relvar)))
(defn btree? [relvar] (= :btree (head-operator relvar)))
(defn from? [relvar] (= :from (head-operator relvar)))

(defn find-indexes
  ([db relvar] (find-indexes db identity relvar))
  ([db pred relvar]
   (let [graph (gg db)
         relvar (unwrap-from (to-relvar relvar))
         id (get-id graph relvar)
         {:keys [dependents]} (graph id)]
     (iterable-mut-list
       (reduce
         (fn !
           [lst child-id]
           (let [child-node (graph child-id)
                 relvar (:relvar child-node)]
             (cond
               (and (index? relvar) (pred relvar)) (add-to-mutable-list lst relvar)
               (from? relvar) (reduce ! lst (:dependents child-node))
               :else lst)))
         (mutable-list)
         dependents)))))

(defn index [db relvar]
  (let [graph (gg db)
        id (get-id graph relvar)
        {:keys [mem]} (graph id)]
    mem))

(defn transact-error [tx]
  (raise "Unrecognized transact form" {:tx tx}))

(defn- upsert-base [db table-key f rows]
  (let [unique-indexes (find-indexes db unique? table-key)

        path-fns
        (reduce
          (fn [m unique]
            (let [[_ & exprs] (head-stmt unique)
                  path (if (seq exprs) (apply juxt (map row-fn exprs)) (constantly [nil]))]
              (assoc m unique path)))
          {}
          unique-indexes)

        drop-self (volatile! #{})
        drop-old (volatile! #{})
        replacement (volatile! {})

        _ (run!
            (fn [unique]
              (let [path (path-fns unique)
                    idx (index db unique)
                    m2 {}
                    seek #(get-in idx %)]
                (reduce
                  (fn [m2 row]
                    (let [pth (path row)
                          self-collision (m2 pth)
                          old-row (seek pth)
                          new-row (if old-row (f old-row row) row)]
                      (when (and self-collision (not= self-collision row))
                        (vswap! drop-self conj self-collision))
                      (cond
                        (nil? old-row) nil
                        :else (vswap! drop-old conj old-row))
                      (when-not (identical? row new-row)
                        (vswap! replacement assoc row new-row))
                      (assoc m2 pth row)))
                  m2
                  rows)))
            unique-indexes)

        add-rows (eduction (comp (remove @drop-self) (distinct) (map #(@replacement % %))) rows)
        del-rows @drop-old

        db (change-table db table-key add-rows del-rows)]
    db))

(defn- insert-or-replace [db table-key rows]
  (upsert-base db table-key (fn [_ new] new) rows))

(defn- insert-or-update [db table-key f rows]
  (let [f (update-f-or-set-map-to-fn f)]
    (upsert-base db table-key (fn [old _] (f old)) rows)))

(defn- insert-or-merge [db table-key binding rows]
  (upsert-base db table-key (bind-fn binding) rows))

(defn- insert-ignore [db table-key rows]
  ;; we can tune this further, but this works.
  (insert-or-update db table-key identity rows))

(defn- transact* [db tx]
  (cond
    (map? tx) (reduce-kv (fn [db table rows] (change-table db (to-table-key table) rows nil)) db tx)
    (seq? tx) (reduce transact* db tx)
    (nil? tx) db
    :else
    (let [[op table & args] tx
          table-key (to-table-key table)]
      (case op
        :insert (change-table db table-key args nil)
        :delete-exact (change-table db table-key nil args)
        :delete (delete-where db table-key args)
        :update (let [[f-or-set-map & exprs] args] (update-where db table-key f-or-set-map exprs))
        :insert-or-replace (insert-or-replace db table-key args)
        :insert-or-update (let [[f-or-set-map & rows] args] (insert-or-update db table-key f-or-set-map rows))
        :insert-or-merge (let [[binding & rows] args] (insert-or-merge db table-key binding rows))
        :insert-ignore (insert-ignore db table-key args)
        :replace-all (change-table db table-key args (q db table-key))
        (transact-error tx)))))

(defn- cascade [db]
  (let [cascade-tx (reduce-kv (fn [acc [table2 _references _clause] rows]
                                (conj acc (into [:delete-exact table2] rows)))
                              [] *foreign-key-cascades*)
        _ (set! *foreign-key-cascades* {})
        db (reduce transact* db cascade-tx)]
    (if (seq *foreign-key-cascades*)
      (cascade db)
      db)))

(defn transact [db tx-coll]
  (binding [*foreign-key-cascades* {}
            *foreign-key-violations* {}
            *check-violations* {}]
    (let [db (reduce transact* db tx-coll)
          db (cascade db)]

      ;; todo rollup errors?
      (doseq [[[relvar references clause] rows] *foreign-key-violations*]
        (when (seq rows)
          (raise "Foreign key violation" {:relvar relvar, :references references, :clause clause, :rows rows})))

      (doseq [[[relvar check] rows] *check-violations*
              :let [graph (gg db)
                    id (get-id graph relvar)
                    idx (if-some [tk (unwrap-table-key relvar)]
                          (graph tk #{})
                          (:mem (graph id) #{}))
                    rows (seq (filter idx rows))]]
        (when-some [row (first rows)]
          (let [error (row-fn (:error check "Check violation"))]
            (raise (error row) {:relvar relvar, :check check, :rows rows}))))

      db)))

(defn track-transact
  "Like transact, but instead of returning you a database, returns a map of

    :db the result of (apply transact db tx)
    :changes a map of {relvar {:added [row1, row2 ...], :deleted [row1, row2, ..]}, ..}

  The :changes allow you to react to additions/removals from derived relvars, and build reactive systems."
  [db tx-coll]
  (binding [*tracking* (zipmap (::watched (gg db)) (repeat #{}))]
    (let [ost db
          ograph (gg ost)
          db (transact db tx-coll)
          graph (gg db)
          changes (for [[id {:keys [adds dels]}] *tracking*
                        :let [{:keys [results, relvar] :or {results #{}}} (graph id)
                              {oresults :results, :or {oresults #{}}} (ograph id)]]
                    [relvar {:added (filterv results (iterable-mut-list adds))
                             :deleted (filterv (every-pred (complement results) oresults) (iterable-mut-list dels))}])]
      {:db db
       :changes (into {} changes)})))