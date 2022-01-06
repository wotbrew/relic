(ns ^:no-doc com.wotbrew.relic.impl.dataflow
  "Core dataflow implementation - the 'innards'. All functions in this namespace
  should be considered private."
  (:require [com.wotbrew.relic.impl.generic-agg-index :as gai]
            [com.wotbrew.relic.impl.util :as u]
            [com.wotbrew.relic.impl.expr :as e]
            [com.wotbrew.relic.impl.relvar :as r]
            [clojure.set :as set]))

(defn where-pred [exprs]
  (case (count exprs)
    0 (constantly true)
    1 (e/row-fn (first exprs))
    (apply every-pred (map e/row-fn exprs))))

(defn without-fn [cols]
  #(apply dissoc % cols))

(defn- overwrite-keys
  [m1 m2 ks]
  (if (nil? m2)
    m1
    (reduce (fn [m k] (let [v (m2 k ::not-found)]
                        (if #?(:clj  (identical? ::not-found v)
                               :cljs (keyword-identical? ::not-found v))
                          m
                          (assoc m k v)))) m1 ks)))

(defn bind-fn [binding]
  (if (keyword? binding)
    (case binding
      (:com.wotbrew.relic/* :*) conj
      #(assoc %1 binding %2))
    #(overwrite-keys %1 %2 binding)))

(defn extend-form-fn [form]
  (cond
    (vector? form)
    (let [[binding expr] form
          expr-fn (e/row-fn expr)
          bind (bind-fn binding)]
      (fn bind-extend [row] (bind row (expr-fn row))))

    :else (throw (ex-info "Not a valid binding form, expected a vector [binding expr]" {}))))

(defn extend-form-cols [form]
  (let [[binding] form]
    (if (keyword? binding)
      (case binding
        (:* :com.wotbrew.relic/*) (u/raise "Cannot use ::rel/* in select expressions yet, use :extend.")
        #{binding})
      (set binding))))

(defn extend-fn [extensions]
  (case (count extensions)
    0 identity
    1 (extend-form-fn (first extensions))
    (apply comp (rseq (mapv extend-form-fn extensions)))))

(defn- expand-form-fn [form]
  (let [[binding expr] form
        expr-fn (e/row-fn expr)
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

(defn req-col-check [relvar col]
  (let [table-key-or-name (if-some [tk (r/unwrap-table-key relvar)] tk "(derived relvar)")]
    {:pred [some? col],
     :error [str [:com.wotbrew.relic/esc col] " required by " [:com.wotbrew.relic/esc table-key-or-name] ", but not found."]}))

(def ^:dynamic *check-violations* nil)

(defn- bad-check [relvar check row]
  (if-some [m *check-violations*]
    (do
      (set! *check-violations* (update m [relvar check] u/set-conj row))
      row)
    (let [{:keys [error]} check
          error-fn (e/row-fn (or error "Check constraint violation"))]
      (u/raise (error-fn row) {:check check, :row row, :relvar relvar}))))

(defn- good-check [relvar check row]
  row)

(defn- check-pred [relvar check]
  (if (map? check)
    (let [{:keys [pred]} check
          pred-fn (e/row-fn pred)]
      (fn [m]
        (let [r (pred-fn m)]
          (if r
            (good-check relvar check m)
            (bad-check relvar check m)))))
    (let [f2 (e/row-fn check)]
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
  (if-some [table-key (r/unwrap-table-key relvar)]
    [(fn mget ([db] (db table-key)) ([db default] (db table-key default)))
     (fn mset [& _] (u/raise "Cannot call mset on table memory"))]
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
                        (let [idx (mget db {})
                              adds (u/mutable-list)
                              dels (u/mutable-list)
                              nidx (reduce (fn [idx row]
                                             (let [new-row (f row)
                                                   s (idx new-row #{})
                                                   ns (disj s row)]
                                               (cond
                                                 (same-size? s ns) idx
                                                 (empty? ns) (do (u/add-to-mutable-list dels new-row)
                                                                 (dissoc idx new-row))
                                                 :else (assoc idx new-row ns)))) idx deleted)

                              nidx (reduce (fn [idx row]
                                             (let [new-row (f row)
                                                   s (idx new-row #{})
                                                   ns (conj s row)]
                                               (if (same-size? s ns)
                                                 idx
                                                 (let [idx (assoc idx new-row ns)]
                                                   (u/add-to-mutable-list adds new-row)
                                                   idx)))) nidx inserted)
                              db (mset db nidx)
                              adds (u/iterable-mut-list adds)
                              dels (u/iterable-mut-list dels)]
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
    {:deps (if (seq left) [left right] [right])
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
  (let [fns (mapv e/row-fn exprs)
        path (if (empty? fns) (constantly []) (apply juxt fns))]
    (if (seq fns)
      (fn [idx row] (get-in idx (path row)))
      (fn [idx _] (when idx (idx nil))))))

(defn- index-seek-fn [index]
  (let [[& exprs] (r/head index)]
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

                          adds (u/mutable-set)
                          dels (u/mutable-set)

                          nidx
                          (reduce
                            (fn [idx row]
                              (let [j (idx row)]
                                (if j
                                  (do (u/add-to-mutable-set dels j)
                                      (dissoc idx row))
                                  idx)))
                            idx
                            deleted)

                          nidx
                          (reduce
                            (fn [idx row]
                              (let [j (idx row)]
                                (when j
                                  (u/add-to-mutable-set dels j))
                                (let [nj (->JoinColl row (seekr ridx row))]
                                  (u/add-to-mutable-set adds nj)
                                  (assoc idx row nj))))
                            nidx
                            inserted)

                          adds (u/iterable-mut-set adds)
                          dels (u/iterable-mut-set dels)

                          db (mset db nidx)]
                      (forward db adds dels)))
             right (fn [db inserted deleted forward]
                     (let [idx (mget db {})
                           lidx (mgetl db {})
                           ridx (mgetr db {})

                           adds (u/mutable-set)
                           dels (u/mutable-set)

                           lrows (into #{} (comp cat (mapcat #(seekl lidx %))) [inserted deleted])

                           nidx
                           (reduce
                             (fn [idx row]
                               (let [j (idx row)
                                     nj (->JoinColl row (seekr ridx row))]
                                 (u/add-to-mutable-set dels j)
                                 (u/add-to-mutable-set adds nj)
                                 (assoc idx row nj)))
                             idx
                             lrows)

                           adds (u/iterable-mut-set adds)
                           dels (u/iterable-mut-set dels)

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
                              deleted (u/realise-coll deleted)
                              inserted (u/realise-coll inserted)
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
        add-row (fn [m row] (update-in m (path row) u/set-conj row))
        del-row (fn [m row] (u/disjoc-in m (path row) row))
        [mget mset] (mem self)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [m (mget db)
                              inserted (u/realise-coll inserted)
                              deleted (u/realise-coll deleted)
                              m (reduce del-row m deleted)
                              m (reduce add-row m inserted)
                              db (mset db m)]
                          (forward db inserted deleted))))
     :provide (fn [db] (when-some [m (not-empty (mget db))]
                         (enumerate-nested-map-of-sets m (count fns))))}))

(defn save-btree [graph self left fns]
  (let [path (if (empty? fns) (constantly []) (apply juxt fns))
        sm (sorted-map)
        add-row (fn [m row] (u/update-in2 m sm (path row) u/set-conj row))
        del-row (fn [m row] (u/disjoc-in m (path row) row))
        [mget mset] (mem self)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [m (mget db sm)
                              deleted (u/realise-coll deleted)
                              inserted (u/realise-coll inserted)
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
      (eduction xf (vals m)))))

(defn save-unique [graph self left fns]
  (let [path (if (empty? fns) (constantly []) (apply juxt fns))
        raise-violation (fn [old-row new-row] (u/raise "Unique constraint violation" {:relvar left, :old-row old-row, :new-row new-row}))
        on-collision (fn on-collision [old-row new-row] (raise-violation old-row new-row))
        replace-fn (fn [old-row row] (cond (nil? old-row) row
                                           (= old-row row) row
                                           :else (on-collision old-row row)))
        add-row (fn [m row] (update-in m (path row) replace-fn row))
        del-row (fn [m row] (u/dissoc-in m (path row)))
        [mget mset] (mem self)]
    {:deps [left]
     :flow (flow left (fn [db inserted deleted forward]
                        (let [m (mget db)
                              deleted (u/realise-coll deleted)
                              inserted (u/realise-coll inserted)
                              m (reduce del-row m deleted)
                              m (reduce add-row m inserted)
                              db (mset db m)]
                          (forward db inserted deleted))))
     :provide (fn [db] (when-some [m (not-empty (mget db))] (enumerate-nested-map-of-maps m (count fns))))}))

(def ^:dynamic *foreign-key-violations* nil)
(def ^:dynamic *foreign-key-cascades* nil)

(defn- bad-fk [left right clause row cascade]
  (when (and (= cascade :delete) *foreign-key-cascades*)
    (set! *foreign-key-cascades* (update *foreign-key-cascades* [left right clause] u/set-conj (dissoc row ::fk))))
  (if *foreign-key-violations*
    (set! *foreign-key-violations* (update *foreign-key-violations* [left right clause] u/set-conj (dissoc row ::fk)))
    (u/raise "Foreign key violation" {:relvar left
                                    :references right
                                    :clause clause
                                    :row (dissoc row ::fk)})))

(defn- good-fk [left right clause row cascade]
  (when (and (= cascade :delete) *foreign-key-cascades*)
    (set! *foreign-key-cascades* (u/disjoc *foreign-key-cascades* [left right clause] (dissoc row ::fk))))
  (when *foreign-key-violations*
    (set! *foreign-key-violations* (u/disjoc *foreign-key-violations* [left right clause] (dissoc row ::fk)))))

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
  (binding [e/*env-deps* #{}
            e/*implicit-joins* #{}]
    (let [relvar (thunk)
          env-deps e/*env-deps*
          joins e/*implicit-joins*

          join-statements
          (mapv (fn [[relvar clause :as jk]]
                  [:join-as-coll relvar clause jk {:unsafe true}]) joins)

          env-join
          (when (seq env-deps)
            [:left-join
             (conj [[:from ::e/Env]] [:select [::e/env [select-keys ::e/env env-deps]]])
             {}])

          without-cols
          (if (seq env-deps)
            [::e/env]
            [])

          without-cols (into without-cols joins)]

      (if (or (seq joins) env-join)
        (let [left (r/left relvar)
              head (r/head relvar)
              left (if env-join (conj left env-join) left)
              left (reduce conj left join-statements)
              left (conj left head)
              left (reduce conj left remainder)]
          (conj left (into [:without-unsafe] without-cols)))
        (reduce conj relvar remainder)))))

(defn- join-merge [{:keys [left, right]}] (conj left right))

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
                      (do (u/add-to-mutable-set changed group)
                          (assoc idx group nset)))))
                (fn [idx row changed]
                  (let [group (group-fn row)
                        [rows eset] (idx group)
                        nrows (u/set-conj rows row)
                        nset (if-some [v (elfn row)] (u/set-conj eset v) eset)]
                    (if (same-size? nrows rows)
                      idx
                      (do (when-not (identical? nset eset)
                            (u/add-to-mutable-set changed group))
                          (assoc idx group [nrows nset]))))))
        deleter (if (= identity elfn)
                  (fn [idx row changed]
                    (let [group (group-fn row)
                          eset (idx group #{})
                          nset (disj eset row)]
                      (if (same-size? eset nset)
                        idx
                        (do (u/add-to-mutable-set changed group)
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
                              (u/add-to-mutable-set changed group))
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

                     changed (u/mutable-set)

                     nidx (reduce #(deleter %1 %2 changed) idx deleted)
                     nidx (reduce #(adder %1 %2 changed) nidx inserted)
                     db (mset db nidx)

                     changed (u/iterable-mut-set changed)

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
                    (update coll (elfn row) u/set-conj row)
                    (update (sorted-map) (elfn row) u/set-conj row)))
        rem-row (fn [coll row]
                  (if coll
                    (u/disjoc coll (elfn row) row)
                    (sorted-map)))

        adder (fn [idx row changed]
                (let [group (group-fn row)
                      ev (idx group)
                      nv (add-row ev row)]
                  (if (identical? ev nv)
                    idx
                    (do
                      (u/add-to-mutable-set changed group)
                      (assoc idx group nv)))))

        deleter (fn [idx row changed]
                  (let [group (group-fn row)
                        ev (idx group)
                        nv (rem-row ev row)]
                    (if (identical? ev nv)
                      idx
                      (do
                        (u/del-from-mutable-set changed group)
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

                     changed (u/mutable-set)

                     nidx (reduce #(deleter %1 %2 changed) idx deleted)
                     nidx (reduce #(adder %1 %2 changed) nidx inserted)
                     db (mset db nidx)

                     changed (u/iterable-mut-set changed)

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
   (let [f (e/row-fn [:if expr :%])]
     {:custom-node (fn [left cols [binding]]
                     (conj left
                           [group cols f]
                           [transform-unsafe (bind-group binding count)]))})))

(defn max-by [expr]
  (let [f (e/row-fn expr)
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
  (let [f (e/row-fn expr)
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
  (comp-complete (max-by expr) (e/row-fn expr)))

(defn min-agg [expr]
  (comp-complete (min-by expr) (e/row-fn expr)))

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
                     deleted (u/realise-coll deleted)
                     inserted (u/realise-coll inserted)
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
                      (u/raise "Cannot bind aggregate to a grouping key"))) aggs)
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

(defn lookup [graph self _ index path]
  (let [path (vec path)
        [iget] (mem index)]
    (when-not (graph (id index))
      (u/raise ":lookup used but index is not materialized" {:index index}))
    (when-not (= (count path) (dec (count (r/head index))))
      (u/raise ":lookup path length must currently match indexed expressions"))
    {:provide (fn [db]
                (when-some [i (iget db)]
                  (get-in i path)))}))

(defn- require-set
  "Certain functions will require that an intermediate relvar is materialized as a set.

  Returns a (potentially new) relvar that ensures mget will return a set of rows at all times."
  [relvar]
  (if (empty? relvar)
    relvar
    (case (r/operator (r/head relvar))
      :set relvar
      (if (r/unwrap-table-key relvar)
        relvar
        (conj relvar [:set])))))

(defn- relvar-or-dual [relvar]
  (if (empty? relvar)
    [[:const [{}]]]
    relvar))

(defn to-dataflow* [graph relvar self]
  (let [relvar (r/to-relvar relvar)
        left (r/left relvar)
        head (r/head relvar)
        operator (r/operator head)]
    (if (fn? operator)
      (let [[op & args] head] (apply op graph self left args))
      (case operator
        :from
        (let [[_ relvar] head
              relvar (if (keyword? relvar)
                       [[:table relvar]]
                       relvar)]
          (conj relvar [pass]))

        :where
        (let [[_ & exprs] head]
          (add-implicit-joins #(conj left [where (where-pred exprs)])))

        :without
        (let [[_ & cols] head]
          (conj left [transform (without-fn cols)]))

        :without-unsafe
        (let [[_ & cols] head]
          (conj left [transform-unsafe (without-fn cols)]))

        :join-as-coll
        (let [[_ right clause binding {:keys [unsafe]}] head
              right (r/to-relvar right)]
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
        (let [[_ & joins] head]
          (reduce (fn [left [right clause]] (add-join left (r/to-relvar right) clause graph)) left (partition-all 2 joins)))

        :left-join
        (let [[_ & joins] head]
          (reduce (fn [left [right clause]] (add-left-join left (r/to-relvar right) clause graph)) left (partition-all 2 joins)))

        :extend
        (let [[_ & extends] head]
          (add-implicit-joins #(conj left [transform (extend-fn extends)])))

        :extend-unsafe
        (let [[_ & extends] head]
          (add-implicit-joins #(conj left [transform-unsafe (extend-fn extends)])))

        :expand
        (let [[_ & expansions] head]
          (add-implicit-joins #(conj left [expand (expand-fn expansions)]) [transform join-merge]))

        :select
        (let [[_ & selections] head
              left (relvar-or-dual left)
              cols (filterv keyword? selections)
              extensions (filterv vector? selections)
              deps (vec (into (set cols) (mapcat extend-form-cols) extensions))
              project (project-fn deps)]
          (if (seq extensions)
            (conj left (into [:extend] extensions) [transform project])
            (conj left [transform project])))

        :select-unsafe
        (let [[_ & selections] head
              left (relvar-or-dual left)
              cols (filterv keyword? selections)
              extensions (filterv vector? selections)
              deps (vec (into (set cols) (mapcat extend-form-cols) extensions))
              project (project-fn deps)]
          (if (seq extensions)
            (add-implicit-joins #(conj left [transform-unsafe (comp project (extend-fn extensions))]))
            (conj left [transform-unsafe project])))

        :set
        (let [[_] head]
          (conj left [save-set]))

        :hash
        (let [[_ & exprs] head]
          (add-implicit-joins #(conj left [save-hash (mapv e/row-fn exprs)])))

        :btree
        (let [[_ & exprs] head]
          (add-implicit-joins #(conj left [save-btree (mapv e/row-fn exprs)])))

        :check
        (let [[_ & checks] head
              left (require-set left)]
          (add-implicit-joins #(conj left [runf (check-fn left checks) identity])))

        :req
        (let [[_ & cols] head]
          (conj left (into [:check] (map #(req-col-check left %)) cols)))

        :fk
        (let [[_ right clause opts] head]
          (add-implicit-joins
            (fn []
              (let [references (r/to-relvar right)
                    origin left
                    _ (when (and (:cascade opts) (not (r/unwrap-table origin)))
                        (u/raise "Cascading :fk constraints are only allowed on table relvars" {:relvar origin, :references references, :clause clause}))
                    [left seekl] (find-hash-index graph left (keys clause) (vals clause))
                    [right seekr] (find-hash-index graph references (vals clause) (keys clause))]
                (conj left [fk seekl right seekr clause origin references opts])))))

        :unique
        (let [[_ & exprs] head]
          (add-implicit-joins #(conj left [save-unique (mapv e/row-fn exprs)])))

        :constrain
        (let [[_ & constraints] head]
          (reduce conj left constraints))

        :const
        (let [[_ coll] head]
          [[provide (constantly coll)]])

        :intersection
        (let [[_ & relvars] head]
          (reduce conj (require-set left) (mapv (fn [r] [intersection (require-set (r/to-relvar r))]) relvars)))

        :union
        (let [[_ & relvars] head
              left (require-set left)]
          (reduce conj left (mapv (fn [r] [union (require-set (r/to-relvar r))]) relvars)))

        :difference
        (let [[_ & relvars] head
              left (require-set left)]
          (reduce conj left (mapv (fn [r] [difference (require-set (r/to-relvar r))]) relvars)))

        :table
        (let [[_ table-key] head]
          {:deps []
           :table table-key
           :flow {nil flow-pass}
           :provide (fn [db] (db table-key))})

        :agg
        (let [[_ cols & aggs] head]
          (conj left [agg cols aggs]))

        :qualify
        (let [[_ namespace] head]
          (conj left [transform #(reduce-kv (fn [m k v] (assoc m (keyword namespace (name k)) v)) {} %)]))

        :rename
        (let [[_ renames] head]
          (conj left [transform #(set/rename-keys % renames)]))

        :lookup
        (let [[_ index & path] head]
          [[lookup index path]])))))

(defn to-dataflow [graph relvar]
  (let [ret (to-dataflow* graph relvar relvar)]
    ((fn ! [ret]
       (if (vector? ret)
         (! (to-dataflow* graph ret relvar))
         ret)) ret)))

(defn dependencies
  "Returns the (table name) dependencies of the relvar, e.g what tables it could be affected by."
  [relvar]
  ((fn rf [s relvar]
     (if (empty? relvar)
       s
       (if-some [table (r/unwrap-table-key relvar)]
         (conj s table)
         (let [head (r/head relvar)
               op (r/operator head)]
           (case op
             :lookup (let [[_ i] head] (dependencies i))
             (let [{:keys [deps]} (to-dataflow {} relvar)]
               (reduce rf s deps)))))))
   #{} relvar))

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
          graph (assoc graph nid (assoc node :relvar relvar))
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
                                            {:adds (reduce u/add-to-mutable-list (or adds (u/mutable-list)) added)
                                             :dels (reduce u/add-to-mutable-list (or dels (u/mutable-list)) deleted)}))]
      (set! *tracking* ntracking)))
  nil)

(defn- tracked-flow [id f]
  (fn tracking-flow [db inserted deleted]
    (track id inserted deleted)
    (f db inserted deleted)))

(defn link
  [graph table-key]
  (letfn [(forward-fn [graph dependents]
            (case (count dependents)
              0 flow-noop
              1 (:linked (graph (first dependents)))
              (let [fns (mapv (comp :linked graph) dependents)]
                (fn flow-fan-out [db inserted deleted]
                  (let [inserted (u/realise-coll inserted)
                        deleted (u/realise-coll deleted)]
                    (reduce
                      (fn flow-step [db f] (f db inserted deleted))
                      db
                      fns))))))
          (build-fn [graph edge id]
            (let [{:keys [dependents
                          flow]
                   :as node} (graph id)
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
            (forward-fn [graph dep dependents]
              (case (count dependents)
                0 flow-noop
                1 (get (:init (graph (first dependents))) dep)
                (let [fns (mapv (comp #(get % dep) :init graph) dependents)]
                  (fn flow-fan-out [db inserted deleted]
                    (let [inserted (u/realise-coll inserted)
                          deleted (u/realise-coll deleted)]
                      (reduce
                        (fn flow-step [db f] (f db inserted deleted))
                        db
                        fns))))))
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
                    dirty (dirty graph uninit)
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
           graph (if (:ephemeral opts) graph (update graph ::materialized u/set-conj relvar))
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
  (let [relvar (r/to-relvar relvar)
        db (materialize db relvar {:ephemeral true})
        graph (gg db)
        graph (update graph ::watched u/set-conj (get-id graph relvar))]
    (sg db graph)))

(defn unwatch [db relvar]
  (let [relvar (r/to-relvar relvar)
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
    (r/unwrap-table-key k-or-relvar)))

(defn- delete-where [db table-key exprs]
  (let [rows (q db (conj [[:table table-key]] (into [:where] exprs)))]
    (if (seq rows)
      (change-table db table-key nil rows)
      db)))

(defn- update-f-or-set-map-to-fn [f-or-set-map]
  (if (map? f-or-set-map)
    (reduce-kv (fn [f k e] (comp f (let [f2 (e/row-fn e)] #(assoc % k (f2 %))))) identity f-or-set-map)
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

(defn index? [relvar] (#{:hash :btree :unique} (r/head-operator relvar)))
(defn unique? [relvar] (= :unique (r/head-operator relvar)))
(defn hash? [relvar] (= :hash (r/head-operator relvar)))
(defn btree? [relvar] (= :btree (r/head-operator relvar)))
(defn from? [relvar] (= :from (r/head-operator relvar)))

(defn find-indexes
  ([db relvar] (find-indexes db identity relvar))
  ([db pred relvar]
   (let [graph (gg db)
         relvar (r/unwrap-from (r/to-relvar relvar))
         id (get-id graph relvar)
         {:keys [dependents]} (graph id)]
     (u/iterable-mut-list
       (reduce
         (fn !
           [lst child-id]
           (let [child-node (graph child-id)
                 relvar (:relvar child-node)]
             (cond
               (and (index? relvar) (pred relvar)) (u/add-to-mutable-list lst relvar)
               (from? relvar) (reduce ! lst (:dependents child-node))
               :else lst)))
         (u/mutable-list)
         dependents)))))

(defn index [db relvar]
  (let [graph (gg db)
        id (get-id graph relvar)
        {:keys [mem]} (graph id)]
    mem))

(defn transact-error [tx]
  (u/raise "Unrecognized transact form" {:tx tx}))

(defn- upsert-base [db table-key f rows]
  (let [unique-indexes (find-indexes db unique? table-key)

        path-fns
        (reduce
          (fn [m unique]
            (let [[_ & exprs] (r/head unique)
                  path (if (seq exprs) (apply juxt (map e/row-fn exprs)) (constantly [nil]))]
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

      (doseq [[[relvar references clause] rows] *foreign-key-violations*]
        (when (seq rows)
          (u/raise "Foreign key violation" {:relvar relvar, :references references, :clause clause, :rows rows})))

      (doseq [[[relvar check] rows] *check-violations*
              :let [graph (gg db)
                    id (get-id graph relvar)
                    idx (if-some [tk (r/unwrap-table-key relvar)]
                          (graph tk #{})
                          (:mem (graph id) #{}))
                    rows (seq (filter idx rows))]]
        (when-some [row (first rows)]
          (let [error (e/row-fn (:error check "Check violation"))]
            (u/raise (error row) {:relvar relvar, :check check, :rows rows}))))

      db)))

(defn- to-table-key-if-table [relvar]
  (if (r/table-relvar? relvar) (to-table-key relvar) relvar))

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
                    [(to-table-key-if-table relvar)
                     {:added (filterv results (u/iterable-mut-list adds))
                      :deleted (filterv (every-pred (complement results) oresults) (u/iterable-mut-list dels))}])]
      {:db db
       :changes (into {} changes)})))