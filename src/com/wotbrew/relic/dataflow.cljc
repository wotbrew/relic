(ns com.wotbrew.relic.dataflow)

(defn- raise
  ([msg] (throw (ex-info msg {})))
  ([msg map] (throw (ex-info msg map)))
  ([msg map cause] (throw (ex-info msg map cause))))

(def Env [[:table ::Env]])
(def ^:dynamic *env-deps* nil)
(defn- track-env-dep [k]
  (when *env-deps* (set! *env-deps* (conj *env-deps* k))))

(defn row-fn [expr]
  (cond
    (= [] expr) (constantly [])

    (= ::% expr) identity

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
            (= f ::env)
            (let [[k not-found] args]
              (track-env-dep k)
              [(fn [row]
                 (-> row ::env (get k not-found)))])

            (= f ::get) (let [[k not-found] args] [(fn [row] (row k not-found))])

            (= f ::esc) (let [[v] args] [(constantly v)])
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
    (if (= ::* binding)
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
      #{binding}
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

(defn req-col-check [table-key col]
  {:pred [some? col],
   :error [str [::esc col] " required by " [::esc table-key] ", but not found."]})

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

(defn operator [stmt] (nth stmt 0))
(defn left-relvar [relvar] (when (peek relvar) (subvec relvar 0 (dec (count relvar)))))
(defn head-stmt [relvar] (peek relvar))

(defn table-relvar?
  "True if the relvar is a table."
  [relvar]
  (case (count relvar)
    1 (= :table (operator (head-stmt relvar)))
    false))

(defn table*-relvar?
  [relvar]
  (case (count relvar)
    1 (= ::table* (operator (head-stmt relvar)))
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
      (cond
        (table-relvar? relvar) relvar
        (table*-relvar? relvar) relvar))))

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
(defn- add-to-mutable-set [mset v] (vswap! mset conj! v))
(defn- iterable-mut-set [mset] (persistent! @mset))

(defn- fast-key [value] value)

(def ^:dynamic *ids* nil)
(def ^:dynamic *idn* nil)

(defn id [relvar]
  (or (get *ids* relvar)
      (let [n *idn*]
        (set! *ids* (assoc *ids* relvar n))
        (set! *idn* (inc n))
        n)))

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
  on the same output rows."
  [graph self left f]
  (let [xf (map f)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward] (forward db (eduction xf inserted) (eduction xf deleted))))}))

(defn union
  "Set union"
  [graph self left right]
  (let [left (conj left [:set])
        right (conj right [:set])
        [mgetl] (mem left)
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
  "Set intersection"
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
  "Set difference"
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
   :flow (flow left (fn [db inserted deleted forward]
                      (run! on-delete deleted)
                      (run! on-insert inserted)
                      (forward db inserted deleted)))})

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
  "Looks up an index that allows seeking on the exprs, returns a pair [index-relvar, seek-fn, unique-seek-fn (maybe)]. "
  [graph left exprs]
  (let [fns (mapv row-fn exprs)
        path (if (empty? fns) (constantly []) (apply juxt fns))]
    [(conj left (into [:hash] exprs))]
    (fn [idx row] (get-in idx (path row)))))

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
                          db (mset db nidx)]
                      (forward db (iterable-mut-set adds) (iterable-mut-set dels))))
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

                           db (mset db nidx)]
                       (forward db (iterable-mut-set adds) (iterable-mut-set dels)))))}))

(defn left-join [graph self left seekl right seekr]
  (conj
    left
    [join-coll seekl right seekr]
    [expand (fn [{:keys [coll]}] (if (seq coll) coll [nil]))]
    [transform-unsafe (fn [{:keys [left right]}] (->Joined (:row left) right))]))

(defn save-set [graph self left]
  (let [[mget mset] (mem self)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [s (mget db)
                              s (transient s)
                              s (reduce disj! s deleted)
                              s (reduce conj! s inserted)
                              db (mset db (persistent! s))]
                          (forward db inserted deleted))))
     :provide (fn [db] (mget db))}))

(defn- enumerate-nested-map-of-sets [m depth]
  (loop [depth depth
         coll (vals m)]
    (case depth
      0 (eduction cat coll)
      (recur (dec depth) (eduction (mapcat vals) coll)))))

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
                              db (mset db m m)]
                          (forward db inserted deleted))))
     :provide (fn [db] (when-some [m (mget db)] (enumerate-nested-map-of-sets m (count fns))))}))

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
     :provide (fn [db] (when-some [m (mget db)] (enumerate-nested-map-of-sets m (count fns))))}))

(defn- enumerate-nested-map-of-maps [m depth]
  (loop [depth depth
         coll (vals m)]
    (case depth
      0 coll
      (recur (dec depth) (eduction (mapcat vals) coll)))))

(defn save-unique [graph self left fns collision-fn]
  (let [path (if (empty? fns) (constantly []) (apply juxt fns))
        replace-fn (fn [old-row row] (if (nil? old-row) row (collision-fn old-row row)))
        add-row (fn [m row] (update-in m (path row) replace-fn row))
        del-row (fn [m row] (dissoc-in m (path row)))
        [mget mset] (mem self)]
    {:flow (flow left (fn [db inserted deleted forward]
                        (let [m (mget db)
                              m (reduce del-row m deleted)
                              m (reduce add-row m inserted)
                              db (mset db m)]
                          (forward db inserted deleted))))
     :provide (fn [db] (when-some [m (mget db)] (enumerate-nested-map-of-maps m (count fns))))}))

(defn group [graph self left cols binding elfn sfn]
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
              sfn
              #(sfn (nth % 1)))

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
                             (assoc (group->row group) binding (sfn entry)))))
                       changed)

                     inserted
                     (eduction
                       (keep
                         (fn [group]
                           (when-some [entry (nidx group)]
                             (assoc (group->row group) binding (sfn entry)))))
                       changed)]
                 (forward db inserted deleted))))}))

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

(defn fk [graph self left seekl right seekr clause {:keys [cascade]}]
  (let [_ (when (and cascade (not (unwrap-table left)))
            (raise "Cascading :fk constraints are only allowed on table relvars" {:relvar left, :references right, :clause clause}))
        on-insert (fn [{:keys [row coll]}]
                    (if (empty? coll)
                      (bad-fk left right clause row cascade)
                      (good-fk left right clause row cascade)))
        on-delete (fn [{:keys [row]}]
                    (good-fk left right clause row cascade))]
    (conj left
          [join-coll seekl right seekr]
          [runf on-insert on-delete]
          [transform-unsafe :row])))

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
                 (conj Env [:select [::env [select-keys ::env env-deps]]])]
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
        (conj left [left-join seekl right seekr] [transform join-merge])))))

(defn to-dataflow* [graph relvar]
  (let [left (left-relvar relvar)
        stmt (head-stmt relvar)
        operator (operator stmt)]
    (if (fn? operator)
      (let [[op & args] stmt] (apply op graph relvar left args))
      (case operator
        :from
        (let [[_ relvar] stmt] relvar)

        :where
        (let [[_ & exprs] stmt]
          (add-implicit-joins #(conj left [where (where-pred exprs)])))

        :without
        (let [[_ & cols] stmt]
          (conj left [transform (without-fn cols)]))

        :join
        (let [[_ & joins] stmt]
          (reduce (fn [left [right clause]] (add-join left right clause graph)) left (partition 2 joins)))

        :left-join
        (let [[_ & joins] stmt]
          (reduce (fn [left [right clause]] (add-left-join left right clause graph)) left (partition 2 joins)))

        :extend
        (let [[_ & extends] stmt]
          (add-implicit-joins #(conj left [transform (extend-fn extends)])))

        :expand
        (let [[_ & expansions] stmt]
          (add-implicit-joins #(conj left [expand (expand-fn expansions)])))

        :select
        (let [[_ & selections] stmt
              cols (filterv keyword? selections)
              extensions (filterv vector? selections)
              deps (into (set cols) (mapcat extend-form-cols) extensions)
              project-fn #(select-keys % deps)]
          (if (seq extensions)
            (add-implicit-joins #(conj left [transform (comp project-fn (extend-fn extensions))]))
            (conj left [transform project-fn])))

        :set
        (let [[_] stmt]
          (conj left [save-set]))

        :hash
        (let [[_ & exprs] stmt]
          (add-implicit-joins #(conj left [save-hash (mapv row-fn exprs) exprs])))

        :btree
        (let [[_ & exprs] stmt]
          (add-implicit-joins #(conj left [save-btree (mapv row-fn exprs) exprs])))

        :check
        (let [[_ & checks] stmt]
          (add-implicit-joins #(conj left [runf (check-fn left checks) identity])))

        :fk
        (let [[_ right clause opts] stmt]
          (add-implicit-joins
            (fn []
              (let [[left seekl] (find-hash-index graph left (keys clause))
                    [right seekr] (find-hash-index graph right (vals clause))]
                (conj left [fk seekl right seekr clause opts])))))

        :constrain
        (let [[_ & constraints] stmt]
          {:deps (mapv #(conj left %) constraints)})

        :const
        (let [[_ coll] stmt]
          [[provide (constantly coll)]])

        :intersection
        (let [[_ & relvars] stmt]
          (reduce conj left (mapv (fn [r] [intersection r]) relvars)))

        :union
        (let [[_ & relvars] stmt]
          (reduce conj left (mapv (fn [r] [union r]) relvars)))

        :difference
        (let [[_ & relvars] stmt]
          (reduce conj left (mapv (fn [r] [difference r]) relvars)))))))

(defn to-dataflow [graph relvar]
  (let [ret (to-dataflow* graph relvar)]
    (if (vector? ret)
      (to-dataflow graph ret)
      ret)))

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
    (let [{:keys [deps] :as node} (to-dataflow graph relvar)
          nid (id relvar)
          graph (assoc graph nid (assoc node :relvar relvar))
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
    (not (contains? graph ((::ids graph {}) relvar))) graph
    :else
    (let [{:keys [dependents
                  deps]} (graph (id relvar))]
      (if (seq dependents)
        (update graph ::unlinked set-conj (id relvar))
        (let [nid (id relvar)
              graph (dissoc graph nid)
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

(defn- flow-noop [db & _]
  db)

(defn- flow-pass [db inserted deleted forward]
  (forward db inserted deleted))

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
            (if (:linked (graph id))
              graph
              (let [{:keys [dependents
                            flow]
                     :as node} (graph id)
                    dependents (sort-dependents dependents)
                    flow-fn (get flow edge flow-noop)
                    graph (reduce #(build %1 id %2) graph dependents)
                    forward (forward-fn id dependents)
                    linked (fn flow [db inserted deleted] (flow-fn db inserted deleted forward))]
                (assoc graph id (assoc node :linked linked)))))]
    (build graph edge ((::ids graph {}) relvar relvar))))

(defn relink
  [graph]
  (-> (reduce link graph (::unlinked graph))
      (dissoc ::unlinked)))

(defn init [graph relvar]
  (letfn [(id [relvar] ((::ids graph {}) relvar))
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
            (let [{:keys [dependents
                          initialised]
                   :or {initialised #{}}} (graph id)]
              (remove initialised dependents)))
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
                                 flow-fn (flow dep)]
                             (assoc m dep (fn init [db inserted deleted]
                                             (let [db (update db dep update :initialised set-conj uninit)
                                                   db (flow-fn db inserted deleted forward)]
                                               db)))))
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
                      (vreset! box
                               (if (coll? inserted)
                                 inserted
                                 (vec inserted)))
                      db))})

(defn q [db query]
  (let [{::keys [graph] :or {graph {}}} (meta db)]
    (or (:results (graph query))
        (let [rbox (volatile! nil)
              query (conj query [result rbox])
              graph (add-to-graph graph query)
              _ (init graph query)]
          @rbox))))

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

(defn- gg [db] (::graph (meta db) {}))
(defn- sg [db graph] (vary-meta db assoc ::graph graph))

(defn materialize [db relvar]
  (let [graph (gg db)
        graph (update graph ::materialized set-conj relvar)
        relvar (conj relvar [mat])
        graph (add-to-graph graph relvar)
        graph (init graph relvar)]
    (sg db graph)))

(defn dematerialize [db relvar]
  (let [graph (gg db)
        graph (update graph ::materialized disj relvar)
        relvar (conj relvar [mat])
        graph (del-from-graph graph relvar)
        graph (relink graph)]
    (sg db graph)))

;; todo watch
;; todo transact
;; todo tracing