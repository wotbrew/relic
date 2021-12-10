(ns com.wotbrew.relic.dataflow
  (:require [com.wotbrew.relic.generic-agg-index :as gai]
            [clojure.set :as set]))

(defn- raise
  ([msg] (throw (ex-info msg {})))
  ([msg map] (throw (ex-info msg map)))
  ([msg map cause] (throw (ex-info msg map cause))))

(def Env [[:table ::Env]])
(def ^:dynamic *env-deps* nil)
(defn- track-env-dep [k]
  (if *env-deps*
    (set! *env-deps* (conj *env-deps* k))
    (raise "Can only use ::rel/env in :extend/:select/:expand statements.")))

(defn row-fn [expr]
  (cond
    (= [] expr) (constantly [])

    (= :com.wotbrew.relic/% expr) identity

    (vector? expr)
    (let [[f & args] expr
          [f args]
          (cond
            #?@(:clj [(qualified-symbol? f) [@(requiring-resolve f) args]])
            #?@(:clj [(symbol? f) [@(resolve f) args]])
            (= f :and) [(apply every-pred (map row-fn args))]
            (= f :or) [(apply some-fn (map row-fn args))]
            (= f :not) [not args]
            (= f :if) (let [[c t e] (map row-fn args)]
                        [(if e
                           (fn [row]
                             (if (c row)
                               (t row)
                               (e row)))
                           (fn [row]
                             (when (c row)
                               (t row))))])
            (= f :com.wotbrew.relic/env)
            (let [[k not-found] args]
              (track-env-dep k)
              [(fn [row]
                 (-> row ::env (get k not-found)))])

            (= f :com.wotbrew.relic/get) (let [[k not-found] args] [(fn [row] (row k not-found))])

            (= f :com.wotbrew.relic/esc) (let [[v] args] [(constantly v)])
            :else [f args])
          args (map row-fn args)]
      (case (count args)
        0 f
        1 (let [[a] args] (comp f a))
        2 (let [[a b] args] #(f (a %) (b %)))
        3 (let [[a b c] args] #(f (a %) (b %) (c %)))
        4 (let [[a b c d] args] #(f (a %) (b %) (c %) (d %)))
        5 (let [[a b c d e] args] #(f (a %) (b %) (c %) (d %) (e %)))
        (let [get-args (apply juxt args)]
          #(apply f (get-args %)))))

    (keyword? expr) expr

    #?@(:clj [(qualified-symbol? expr) @(requiring-resolve expr)])

    #?@(:clj [(symbol? expr) @(resolve expr)])

    (fn? expr) expr

    (map? expr)
    (if (seq expr)
      (apply every-pred (map (fn [[k v]] (let [kf (row-fn k)] #(= v (kf %)))) expr))
      (constantly true))

    :else (constantly expr)))

(defn where-pred [exprs]
  (case (count exprs)
    0 (constantly true)
    1 (row-fn (first exprs))
    (apply every-pred (map row-fn exprs))))

(defn without-fn [cols]
  #(apply dissoc % cols))

(defn- assoc-if-not-nil [m k v]
  (if (nil? v)
    m
    (assoc m k v)))

(defn bind-fn [binding]
  (if (keyword? binding)
    (if (= :com.wotbrew.relic/* binding)
      #(merge % %2)
      #(assoc-if-not-nil % binding %2))
    #(merge % (select-keys %2 binding))))

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
      (if (= :com.wotbrew.relic/* binding)
        (raise "Cannot use ::rel/* in select expressions yet, use :extend.")
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
    (let [[_ relvar] (head-stmt relvar)] (unwrap-from relvar))
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

(defn- check-pred [relvar check]
  (if (map? check)
    (let [{:keys [pred, error]} check
          pred-fn (row-fn pred)
          error-fn (row-fn (or error "Check constraint violation"))]
      (fn [m]
        (let [r (pred-fn m)]
          (if r
            m
            (raise (error-fn m) {:expr pred, :check check, :row m, :relvar relvar})))))
    (let [f2 (row-fn check)]
      (fn [m]
        (let [r (f2 m)]
          (if r
            m
            (raise "Check constraint violation" {:expr check, :check check, :row m, :relvar relvar})))))))

(defn check-fn [relvar checks]
  (case (count checks)
    0 identity
    1 (check-pred relvar (first checks))
    (apply comp (rseq (mapv #(check-pred relvar %) checks)))))

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

;; cross platform mutable set
(defn- mutable-set [] (volatile! (transient #{})))
(defn- add-to-mutable-set [mset v] (vswap! mset conj! v) mset)
(defn- del-from-mutable-set [mset v] (vswap! mset disj! v) mset)
(defn- iterable-mut-set [mset] (persistent! @mset))

;; cross platform mutable list
(defn- mutable-list [] (volatile! (transient [])))
(defn- add-to-mutable-list [mlist v] (vswap! mlist conj! v) mlist)
(defn- iterable-mut-list [mlist] (persistent! @mlist))

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
  (let [id (id relvar)]
    [(fn mget
       ([db] (:mem (db id)))
       ([db default] (:mem (db id) default)))
     (fn mset
       [db val]
       (assoc db id (assoc (db id) :mem val)))]))

(defn- flow [relvar f & more]
  (loop [ret (array-map (id relvar) f)
         more more]
    (if-some [[relvar f & tail] (seq more)]
      (recur (assoc ret (id relvar) f) tail)
      ret)))

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
                                                 (identical? s ns) idx
                                                 (empty? ns) (do (add-to-mutable-set dels new-row)
                                                                 (dissoc idx new-row))
                                                 :else (assoc idx new-row ns)))) idx deleted)
                              nidx (reduce (fn [idx row]
                                             (let [new-row (f row)
                                                   s (idx new-row #{})
                                                   ns (conj s row)]
                                               (if (identical? s ns)
                                                 idx
                                                 (let [idx (assoc idx new-row ns)]
                                                   (when (empty? s)
                                                     (add-to-mutable-set adds new-row))
                                                   idx)))) nidx inserted)
                              db (mset db nidx)]
                          (forward db (iterable-mut-set adds) (iterable-mut-set dels)))))}))

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
  (let [left (conj left [:set])
        right (conj right [:set])
        [mgetl] (mem left)
        [mgetr] (mem right)]
    {:deps [left right]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [ridx (mgetr db #{})
                              deleted (eduction (filter ridx) deleted)]
                          (forward db inserted deleted)))
                 right (fn [db inserted deleted forward]
                         (let [lidx (mgetl db #{})
                               inserted (eduction (filter lidx) inserted)]
                           (forward db inserted deleted))))}))

(defn difference
  "Set difference, rows that are in left and not right are flowed."
  [graph self left right]
  (let [left (conj left [:set])
        right (conj right [:set])
        [mgetl] (mem left)
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
  never flowing - then use this. :const and :hash-lookup would be examples of provide."
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
     :flow (flow left (fn [db inserted deleted forward] (forward db (eduction exf inserted) (eduction exf deleted))))}))

(defn find-hash-index
  "Looks up an index that allows seeking on the exprs, returns a tuple [index-relvar, seek-fn, unique-seek-fn (maybe)]. "
  [graph left exprs]
  (let [fns (mapv row-fn exprs)
        path (if (empty? fns) (constantly []) (apply juxt fns))]
    [(conj left (into [:hash] exprs))
     (if (seq fns)
       (fn [idx row] (get-in idx (path row)))
       (fn [idx _] (when idx (idx nil))))]))

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
                       (forward db adds dels))))}))

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
                        (let [s (mget db #{})
                              s (transient s)
                              s (reduce disj! s deleted)
                              s (reduce conj! s inserted)
                              db (mset db (persistent! s))]
                          (forward db inserted deleted))))
     :provide (fn [db] (not-empty (mget db)))}))

(defn- enumerate-nested-map-of-sets [m depth]
  (if (= 0 depth)
    (eduction cat (vals m))
    (loop [depth depth
           coll (vals m)]
      (case depth
        0 (eduction cat coll)
        (recur (dec depth) (eduction (mapcat vals) coll))))))

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
     :provide (fn [db] (when-some [m (not-empty (mget db))] (enumerate-nested-map-of-sets m (count fns))))}))

(defn save-btree [graph self left fns]
  (let [path (if (empty? fns) (constantly []) (apply juxt fns))
        sm (sorted-map)
        add-row (fn [m row] (update-in2 m sm (path row) set-conj row))
        del-row (fn [m row] (disjoc-in m (path row) row))
        [mget mset] (mem self)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [m (mget db {})
                              m (reduce del-row m deleted)
                              m (reduce add-row m inserted)
                              db (mset db m)]
                          (forward db inserted deleted))))
     :provide (fn [db] (when-some [m (not-empty (mget db))] (enumerate-nested-map-of-sets m (count fns))))}))

(defn- enumerate-nested-map-of-maps [m depth]
  (loop [depth depth
         coll (vals m)]
    (case depth
      0 coll
      (recur (dec depth) (eduction (mapcat vals) coll)))))

(def ^:private ^:dynamic *upsert-collisions* nil)

(defn save-unique [graph self left fns]
  (let [path (if (empty? fns) (constantly []) (apply juxt fns))
        raise-violation (fn [old-row new-row] (raise "Unique constraint violation" {:relvar left, :old-row old-row, :new-row new-row}))
        upsert-collision
        (if-some [[_ table-key] (some-> (unwrap-table left) head-stmt)]
          (fn [old-row new-row]
            (set! *upsert-collisions* (update *upsert-collisions* table-key set-conj old-row))
            new-row)
          raise-violation)
        on-collision (fn on-collision [old-row new-row]
                       (if *upsert-collisions*
                         (upsert-collision old-row new-row)
                         (raise-violation old-row new-row)))
        replace-fn (fn [old-row row] (if (nil? old-row) row (on-collision old-row row)))
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
  (when (and cascade *foreign-key-cascades*)
    (set! *foreign-key-cascades* (update *foreign-key-cascades* [left right clause] set-conj (dissoc row ::fk))))
  (if *foreign-key-violations*
    (set! *foreign-key-violations* (update *foreign-key-violations* [left right clause] set-conj (dissoc row ::fk)))
    (raise "Foreign key violation" {:relvar left
                                    :references right
                                    :clause clause
                                    :row (dissoc row ::fk)})))

(defn- good-fk [left right clause row cascade]
  (when (and cascade *foreign-key-cascades*)
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

(defn add-implicit-joins [thunk]
  (binding [*env-deps* #{}]
    (let [relvar (thunk)
          env-deps *env-deps*]
      (if (empty? env-deps)
        relvar
        (let [left (left-relvar relvar)
              stmt (head-stmt relvar)]
          (conj left
                [:left-join
                 (conj Env [:select [::env [select-keys ::env env-deps]]])
                 {}]
                stmt
                [:without ::env]))))))

(defn- join-merge [{:keys [left, right]}] (merge left right))

(defn- add-join [left right clause graph]
  (add-implicit-joins
    (fn []
      (let [[left seekl] (find-hash-index graph left (keys clause))
            [right seekr] (find-hash-index graph right (vals clause))]
        (conj left [join seekl right seekr] [transform join-merge])))))

(defn- add-left-join [left right clause graph]
  (add-implicit-joins
    (fn []
      (let [[left seekl] (find-hash-index graph left (keys clause))
            [right seekr] (find-hash-index graph right (vals clause))]
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
                    (if (identical? eset nset)
                      idx
                      (do (add-to-mutable-set changed group)
                          (assoc idx group nset)))))
                (fn [idx row changed]
                  (let [group (group-fn row)
                        [rows eset] (idx group)
                        nrows (set-conj rows row)
                        nset (if-some [v (elfn row)] (set-conj eset v) eset)]
                    (if (identical? nrows rows)
                      idx
                      (do (when-not (identical? nset eset)
                            (add-to-mutable-set changed group))
                          (assoc idx group [nrows nset]))))))
        deleter (if (= identity elfn)
                  (fn [idx row changed]
                    (let [group (group-fn row)
                          eset (idx group #{})
                          nset (disj eset row)]
                      (if (identical? eset nset)
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
                      (if (identical? nrows rows)
                        idx
                        (do (when-not (identical? nset eset)
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
                 (forward db inserted deleted))))}))

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

                 (forward db inserted deleted))))}))

(defn row-count
  "A relic agg function that can be used in :agg expressions to calculate the number of rows in a relation."
  ([]
   {:custom-node (fn [left cols [binding]]
                   (conj left
                         [group cols identity]
                         [transform-unsafe (bind-group binding count)]))})
  ([expr]
   (let [f (row-fn [:if expr :com.wotbrew.relic/%])]
     {:custom-node (fn [left cols [binding]]
                     (conj left
                           [group cols f]
                           [transform-unsafe (bind-group binding count)]))})))

;; --
;; agg expressions e.g [rel/sum :foo]

(defn- agg-expr-agg [expr]
  (cond
    (map? expr) expr

    (#{count 'count `count} expr) (row-count)

    (fn? expr) (expr)

    (vector? expr)
    (let [[f & args] expr]
      (if (#{count 'count `count} f)
        (apply row-count args)
        (apply f args)))

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
        [mget mset] (mem self)]
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
                       (keep
                         (fn [k]
                           (when-some [{:keys [indexed-row]} (idx k)]
                             @indexed-row)))
                       ks)
                     new-rows
                     (eduction
                       (keep
                         (fn [k]
                           (when-some [{:keys [indexed-row]} (nidx k)]
                             @indexed-row)))
                       ks)]
                 (forward db new-rows old-rows))))}))

(defn- get-custom-agg-node [left cols [_ expr :as agg]]
  (when-some [n (:custom-node (agg-expr-agg expr))]
    (n left cols agg)))

(defn agg [graph self left cols aggs]
  (if (empty? aggs)
    (conj left [transform (project-fn (vec (set cols)))])
    (let [cols (vec (set cols))
          join-clause (zipmap cols cols)
          [custom-nodes standard-aggs] ((juxt keep remove) #(get-custom-agg-node left cols %) aggs)]
      (case (count custom-nodes)
        0 (conj left [generic-agg cols standard-aggs])
        1 (first custom-nodes)
        (let [joins (concat (for [node custom-nodes]
                              [:join node join-clause])
                            (when (seq standard-aggs)
                              [[:join (conj left [generic-agg cols standard-aggs]) join-clause]]))
              left (conj left (into [:select] cols))
              left (reduce conj left joins)]
          left)))))

(defn- join-expr? [expr] (and (vector? expr) (= :com.wotbrew.relic/join-coll (nth expr 0 nil))))
(defn- extend-expr [[_ expr]] expr)
(defn- join-ext? [extension] (join-expr? (extend-expr extension)))

(defn- join-first-expr? [expr] (and (vector? expr) (= :com.wotbrew.relic/join-first (nth expr 0 nil))))
(defn- join-first-ext? [extension] (join-first-expr? (extend-expr extension)))

(defn split-extensions [extensions]
  (for [extensions (partition-by (fn [ext] (cond
                                             (join-ext? ext) :join
                                             (join-first-ext? ext) :join1
                                             :else :std)) extensions)]
    (cond
      (join-ext? (first extensions))
      (for [[binding [_ relvar clause]] extensions
            :let [_ (assert (keyword? binding) "only keyword bindings allowed for join-coll expressions")]]
        [:join-as-coll relvar clause binding])

      (join-first-ext? (first extensions))
      (for [[binding [_ relvar clause]] extensions
            stmt
            [[:join-as-coll relvar clause ::join-expr]
             [transform #(assoc-if-not-nil (dissoc % ::join-expr) binding (first (::join-expr %)))]]]
        stmt)

      :else [(into [:extend*] extensions)])))

(defn to-dataflow* [graph relvar self]
  (let [left (left-relvar relvar)
        stmt (head-stmt relvar)
        operator (operator stmt)]
    (if (fn? operator)
      (let [[op & args] stmt] (apply op graph self left args))
      (case operator
        :from
        (let [[_ relvar] stmt] (conj relvar [pass]))

        :where
        (let [[_ & exprs] stmt]
          (add-implicit-joins #(conj left [where (where-pred exprs)])))

        :without
        (let [[_ & cols] stmt]
          (conj left [transform (without-fn cols)]))

        :join-as-coll
        (let [[_ right clause binding] stmt]
          (add-implicit-joins
            (fn []
              (let [[left seekl] (find-hash-index graph left (keys clause))
                    [right seekr] (find-hash-index graph right (vals clause))]
                (conj left
                      [join-coll seekl right seekr]
                      [transform (fn [{:keys [left coll]}]
                                   (assoc left binding coll))])))))

        :join
        (let [[_ & joins] stmt]
          (reduce (fn [left [right clause]] (add-join left right clause graph)) left (partition-all 2 joins)))

        :left-join
        (let [[_ & joins] stmt]
          (reduce (fn [left [right clause]] (add-left-join left right clause graph)) left (partition-all 2 joins)))

        :extend*
        (let [[_ & extends] stmt]
          (add-implicit-joins #(conj left [transform (extend-fn extends)])))

        :extend-unsafe*
        (let [[_ & extends] stmt]
          (add-implicit-joins #(conj left [transform-unsafe (extend-fn extends)])))

        :extend
        (let [[_ & extends] stmt]
          (if (seq extends)
            (transduce cat conj left (split-extensions extends))
            (conj left [pass])))

        :expand
        (let [[_ & expansions] stmt]
          (add-implicit-joins #(conj left [expand (expand-fn expansions)] [transform join-merge])))

        :select
        (let [[_ & selections] stmt
              cols (filterv keyword? selections)
              extensions (filterv vector? selections)
              deps (vec (into (set cols) (mapcat extend-form-cols) extensions))
              project (project-fn deps)]
          (if (seq extensions)
            (conj left (into [:extend] extensions) [transform project])
            (conj left [transform project])))

        :select-unsafe
        (let [[_ & selections] stmt
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
        (let [[_ & checks] stmt]
          (add-implicit-joins #(conj left [runf (check-fn left checks) identity])))

        :req
        (let [[_ & cols] stmt]
          (conj left (into [:check] (map #(req-col-check left %)) cols)))

        :fk
        (let [[_ right clause opts] stmt]
          (add-implicit-joins
            (fn []
              (let [references right
                    origin left
                    _ (when (and (:cascade opts) (not (unwrap-table origin)))
                        (raise "Cascading :fk constraints are only allowed on table relvars" {:relvar origin, :references references, :clause clause}))
                    [left seekl] (find-hash-index graph left (keys clause))
                    [right seekr] (find-hash-index graph right (vals clause))]
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
          (reduce conj left (mapv (fn [r] [intersection r]) relvars)))

        :union
        (let [[_ & relvars] stmt
              left (conj left [:set])]
          (reduce conj left (mapv (fn [r] [union (conj r [:set])]) relvars)))

        :difference
        (let [[_ & relvars] stmt]
          (reduce conj left (mapv (fn [r] [difference r]) relvars)))

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
          (conj left [transform #(set/rename-keys % renames)]))))))

(defn to-dataflow [graph relvar]
  (let [ret (to-dataflow* graph relvar relvar)]
    ((fn ! [ret]
       (if (vector? ret)
         (! (to-dataflow* graph ret relvar))
         ret)) ret)))

(defn- depend [graph dep dependent]
  (let [{:keys [dependents
                score]
         :or {dependents #{}
              score 0}
         :as node} (graph dep)
        dependents (conj dependents dependent)]
    ;; todo score
    (assoc graph dep (-> (assoc node :dependents dependents, :score score)
                         (dissoc :linked)))))

(defn- undepend [graph dep dependent]
  (let [{:keys [dependents
                score]
         :or {dependents #{}
              score 0}
         :as node} (graph dep)
        dependents (disj dependents dependent)]
    ;; todo score
    (assoc graph dep
                 (-> (assoc node :dependents dependents, :score score)
                     (dissoc :linked)))))

(defn- add-to-graph* [graph relvar]
  (cond
    (= [] relvar) graph
    (contains? graph (*ids* relvar)) graph
    :else
    (let [{:keys [deps table] :as node} (to-dataflow graph relvar)
          nid (id relvar)
          graph (assoc graph nid (assoc node :relvar relvar :score 0))
          graph (if table (update graph ::tables assoc table nid) graph)
          graph (reduce add-to-graph* graph deps)
          graph (reduce #(depend %1 (id %2) nid) graph deps)]
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
        (update graph ::unlinked set-conj (id relvar))
        (let [nid (id relvar)
              graph (dissoc graph nid)
              graph (if table (update graph ::tables dissoc table) graph)
              graph (reduce #(undepend %1 (id %2) nid) graph deps)
              graph (reduce del-from-graph* graph deps)
              _ (set! *ids* (dissoc *ids* relvar))]
          graph)))))

(defn del-from-graph [graph relvar]
  (binding [*ids* (::ids graph {})
            *idn* (::idn graph 0)]
    (let [graph (del-from-graph* graph relvar)
          graph (assoc graph ::ids *ids* ::idn *idn*)]
      graph)))

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
  [graph edge relvar]
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
          (build [graph edge id]
            (let [{:keys [dependents
                          flow]
                   :as node} (graph id)
                  dependents (sort-dependents dependents)
                  flow-fn (get flow edge flow-noop)
                  graph (reduce #(build %1 id %2) graph dependents)
                  forward (tracked-flow id (forward-fn graph dependents))
                  linked (fn flow [db inserted deleted] (flow-fn db inserted deleted forward))]
              (assoc graph id (assoc node :linked linked))))]
    (build graph edge ((::ids graph {}) relvar relvar))))

(defn relink
  [graph]
  (-> (reduce (fn [graph relvar]
                (reduce (fn [graph dep]
                          (link graph dep relvar))
                        graph
                        (:deps (graph (get-id graph relvar))))) graph (::unlinked graph))
      (dissoc ::unlinked)))

(defn init [graph relvar]
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
          (dirty [graph id]
            (let [{:keys [dependents]} (graph id)]
              dependents))
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
                          provide
                          results]} (graph (id relvar))
                  rs (or results (when provide (provide graph)))]

              (cond
                rs
                (let [graph (link-uninitialised graph (id relvar))
                      f (:init-forward (graph (id relvar)))]
                  (f graph rs nil))

                :else
                (reduce init graph deps))))]
    (init graph relvar)))

(defn- result [graph self left box]
  {:deps [left]
   :flow (flow left (fn [db inserted _ forward]
                      (vswap! box
                              (fn [existing]
                                (cond
                                  existing
                                  (into (set existing) inserted)

                                  (coll? inserted)
                                  inserted

                                  :else (vec inserted))))
                      db))})

(defn gg [db] (::graph (meta db) {}))
(defn- sg [db graph] (vary-meta db assoc ::graph graph))

(defn q [db query]
  (if (keyword? query)
    (query db)
    (let [graph (gg db)]
      (or (:results (graph (get-id graph query)))
          (let [rbox (volatile! nil)
                query (conj query [result rbox])
                graph (add-to-graph graph query)
                _ (init graph query)]
            @rbox)))))

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
           graph (if (:ephemeral opts) graph (update graph ::materialized set-conj relvar))
           relvar (conj relvar [mat])
           graph (add-to-graph graph relvar)
           graph (relink graph)
           graph (init graph relvar)]
       (sg db graph)))))

(defn dematerialize [db relvar]
  (let [graph (gg db)
        graph (update graph ::materialized disj relvar)
        relvar (conj relvar [mat])
        graph (del-from-graph graph relvar)
        graph (relink graph)]
    (sg db graph)))

(defn change-table [db table-key inserts deletes]
  (let [graph (gg db)
        table-id (-> graph ::tables (get table-key))
        {:keys [linked
                relvar]} (graph table-id)]
    (if linked
      (let [data (graph table-key)
            drows (when deletes (if data (into [] (filter data) deletes) (set deletes)))
            nrows (if data (into [] (remove data) inserts) (set inserts))
            ndata (if data (into data nrows) nrows)
            ndata (if drows (reduce disj ndata drows) ndata)

            graph (assoc graph table-key ndata)
            db (assoc db table-key ndata)
            graph (linked graph nrows drows)]
        (sg db graph))
      (if relvar
        (let [graph (link graph nil relvar)]
          (change-table (sg db graph) table-key inserts deletes))
        (let [data (graph table-key)
              drows (when deletes (if data (into [] (filter data) deletes) (set deletes)))
              ndata (if data (into data inserts) (set inserts))
              ndata (if drows (reduce disj ndata drows) ndata)
              graph (assoc graph table-key ndata)
              db (assoc db table-key ndata)]
          (sg db graph))))))

(defn watch [db relvar]
  (let [db (materialize db relvar {:ephemeral true})
        graph (gg db)
        graph (update graph ::watched set-conj (get-id graph relvar))]
    (sg db graph)))

(defn unwatch [db relvar]
  (let [graph (gg db)
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
    (reduce-kv (fn [f k e] (comp f (let [f2 (row-fn e)] #(assoc % k (f2 %))))) identity f-or-set-map)
    f-or-set-map))

(defn- update-where [db table-key f-or-set-map exprs]
  (let [rows (q db (conj [[:table table-key]] (into [:where] exprs)))
        f (update-f-or-set-map-to-fn f-or-set-map)
        new-rows (eduction (map f) rows)]
    (if (seq rows)
      (change-table db table-key new-rows rows)
      db)))

(defn- table-with-opts? [v]
  (if (vector? v)
    (not= 2 (count (head-stmt v)))
    false))

(defn materialized? [db relvar]
  (contains? (::materialized (gg db)) relvar))

(defn transact-error [tx]
  (raise "Unrecognized transact form" {:tx tx}))

(defn- transact* [db tx]
  (cond
    (map? tx) (reduce-kv (fn [db table rows] (change-table db (to-table-key table) rows nil)) db tx)
    (seq? tx) (reduce transact* db tx)
    :else
    (let [[op table & args] tx
          table-key (to-table-key table)]
      (case op
        :insert (change-table db table-key args nil)
        :delete-exact (change-table db table-key nil args)
        :delete (delete-where db table-key args)
        :update (let [[f-or-set-map & exprs] args] (update-where db table-key f-or-set-map exprs))
        :upsert (binding [*upsert-collisions* {}]
                  (let [db2 (change-table db table-key args nil)
                        db-new (reduce-kv (fn [db table-key rows]
                                            (change-table db table-key nil rows)) db *upsert-collisions*)]
                    (if (identical? db-new db2)
                      db2
                      (change-table db-new table-key (remove (*upsert-collisions* table-key #{}) args) nil))))
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
            *foreign-key-violations* {}]
    (let [db (reduce transact* db tx-coll)
          db (cascade db)]
      (doseq [[[relvar references clause] rows] *foreign-key-violations*]
        (when (seq rows)
          (raise "Foreign key violation" {:relvar relvar, :references references, :clause clause, :rows rows})))
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