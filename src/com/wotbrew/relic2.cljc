(ns com.wotbrew.relic2
  (:require [clojure.set :as set]))

;; protocols

(defprotocol Index
  :extend-via-metadata true
  (index-row [index row])
  (unindex-row [index row])
  (contains-row? [index row])
  (row-seqable [index]))

(defprotocol SeekN
  :extend-via-metadata true
  (seek-n [index ks]))

(extend-protocol SeekN
  nil
  (seek-n [_ _] nil))

;; utilities

(defn- dissoc-in [m ks]
  (if-let [[k & ks] (seq ks)]
    (if (seq ks)
      (let [v (dissoc-in (get m k) ks)]
        (if (empty? v)
          (dissoc m k)
          (assoc m k v)))
      (dissoc m k))
    m))

(defn- disjoc-in [m ks x]
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

(def ^:private set-conj (fnil conj #{}))

(defn- raise
  ([msg] (throw (ex-info msg {})))
  ([msg map] (throw (ex-info msg map)))
  ([msg map cause] (throw (ex-info msg map cause))))

;; indexes

(def empty-set-index
  (->> {`index-row conj
        `unindex-row disj
        `contains-row? contains?
        `row-seqable identity}
       (with-meta #{})))

(defn map-index [empty keyfns]
  (let [path (apply juxt keyfns)
        depth (count keyfns)]
    (with-meta
      empty
      {`index-row (fn [index row] (update-in index (path row) set-conj row))
       `unindex-row (fn [index row] (disjoc-in index (path row) row))
       `contains-row?
       (fn map-contains-row? [index row] (contains? (get-in index (path row) #{}) row))
       `row-seqable
       (fn enumerate-map
         ([index] (enumerate-map index depth))
         ([index depth]
          (if (= 1 depth)
            (mapcat identity (vals index))
            (for [v (vals index)
                  v2 (enumerate-map v (dec depth))]
              v2))))
       `seek-n
       (fn map-seekn [index ks]
         (let [remaining-depth (- depth (count ks))
               m-or-coll (get-in index ks)]
           (case remaining-depth
             0 m-or-coll
             1 (mapcat identity (vals m-or-coll))
             2 (mapcat vals (vals m-or-coll))
             (if (<= 1 remaining-depth depth)
               ((fn ! [m-or-coll remaining-depth]
                  (case remaining-depth
                    0 m-or-coll
                    1 (mapcat identity (vals m-or-coll))
                    2 (mapcat vals (vals m-or-coll))
                    (mapcat #(! % (depth remaining-depth)) (vals m-or-coll))))
                m-or-coll
                remaining-depth)
               (throw (ex-info "Invalid remaining depth" {}))))))})))

;; statements

(defn- operator [stmt] (nth stmt 0))

(defmulti graph-node
  "Return a map describing the stmt in relation to the left relvar.

  Return keys:

  :empty-index
  An empty implementation of the Index protocol if the statement requires memory for materialization.

  :deps
  A collection of relvars this relvar is dependent on, will normally include 'left' but not always.

  :insert1,, :delete1,
  A function of [st row flow-inserts flow-insert1 flow-deletes flow-delete1], the function should return a new state,
  by calling flow-insert*/flow-delete* functions for new/deleted rows. You do not have to maintain any indexes, the flow functions
  include self-index maintenance, the state should just be threaded through.

  :insert, :delete
  Like :insert1/:delete1 but taking a 'rows' collection instead of a single 'row', otherwise arg order is the same and the same rules apply."
  (fn [left stmt] (operator stmt)))

;; dataflow

(defn base-relvar? [relvar]
  (case (count relvar)
    1 (= :state (operator (peek relvar)))
    false))

(defn- base-mat-fns
  ([relvar]
   (assert (base-relvar? relvar) "Cannot modify derived relvar")
   (let [{:keys [empty-index]} (graph-node [] (peek relvar))]
     {:insert
      (fn base-insert [st rows]
        (let [oidx (st relvar empty-index)
              new-rows (filterv #(not (contains-row? oidx %)) rows)]
          (if (seq new-rows)
            (let [nidx (reduce index-row oidx new-rows)
                  st (assoc st relvar nidx)]
              st)
            st)))
      :insert1
      (fn base-insert1 [st row]
        (let [oidx (st relvar empty-index)]
          (if (contains-row? oidx row)
            st
            (let [nidx (index-row oidx row)
                  st (assoc st relvar nidx)]
              st))))
      :delete
      (fn base-delete [st rows]
        (let [oidx (st relvar empty-index)
              deleted-rows (filterv #(contains-row? oidx %) rows)]
          (if (seq deleted-rows)
            (let [nidx (reduce unindex-row oidx deleted-rows)
                  st (assoc st relvar nidx)]
              st)
            st)))
      :delete1
      (fn base-delete1 [st row]
        (let [oidx (st relvar empty-index)]
          (if-not (contains-row? oidx row)
            st
            (let [nidx (unindex-row oidx row)
                  st (assoc st relvar nidx)]
              st))))}))
  ([g relvar flow]
   (let [{:keys [empty-index]} (g relvar)
         {:keys [flow-inserts-n
                 flow-insert1
                 flow-deletes-n
                 flow-delete1]} flow]
     {:insert
      (fn base-insert [st rows]
        (let [oidx (st relvar empty-index)
              new-rows (filterv #(not (contains-row? oidx %)) rows)]
          (if (seq new-rows)
            (let [nidx (reduce index-row oidx new-rows)
                  st (assoc st relvar nidx)
                  st (vary-meta st assoc relvar nidx)]
              (vary-meta st flow-inserts-n new-rows))
            st)))
      :insert1
      (fn base-insert1 [st row]
        (let [oidx (st relvar empty-index)]
          (if (contains-row? oidx row)
            st
            (let [nidx (index-row oidx row)
                  st (assoc st relvar nidx)
                  st (vary-meta st assoc relvar nidx)]
              (vary-meta st flow-insert1 row)))))
      :delete
      (fn base-delete [st rows]
        (let [oidx (st relvar empty-index)
              deleted-rows (filterv #(contains-row? oidx %) rows)]
          (if (seq deleted-rows)
            (let [nidx (reduce unindex-row oidx deleted-rows)
                  st (assoc st relvar nidx)
                  st (vary-meta st assoc relvar nidx)]
              (vary-meta st flow-deletes-n deleted-rows))
            st)))
      :delete1
      (fn base-delete1 [st row]
        (let [oidx (st relvar empty-index)]
          (if-not (contains-row? oidx row)
            st
            (let [nidx (unindex-row oidx row)
                  st (assoc st relvar nidx)
                  st (vary-meta st assoc relvar nidx)]
              (vary-meta st flow-delete1 row)))))})))

(defn- add-to-graph [g relvars]
  (letfn [(edit-flow [node dependent]
            (let [dependents (:dependents node #{})
                  new-dependents (conj dependents dependent)]
              (if (identical? new-dependents dependents)
                node
                (-> node
                    (assoc :dependents new-dependents)))))
          (depend [g dependent dependency]
            (let [g (add g dependency)]
              (update g dependency edit-flow dependent)))
          (merge-node [a b relvar stmt]
            (cond
              (nil? a) (assoc b :relvar relvar)
              (:mat (meta stmt)) (assoc b :relvar relvar :dependents (:dependents a #{}))
              :else a))
          (add [g relvar]
            (let [left (pop relvar)
                  stmt (peek relvar)
                  {:keys [deps]
                   :as node} (graph-node left stmt)
                  g (update g relvar merge-node node relvar stmt)
                  g (reduce (fn [g dep] (depend g relvar dep)) g deps)]
              g))]
    (reduce add g relvars)))

(defn- derived-mat-fns [node edge flow]
  (let [{:keys [flow-inserts-n
                flow-insert1
                flow-deletes-n
                flow-delete1]} flow
        {:keys [empty-index
                relvar
                insert
                insert1
                delete
                delete1]} node

        insert (get insert edge)
        insert1 (get insert1 edge)
        delete (get delete edge)
        delete1 (get delete1 edge)

        empty-index (or empty-index (when (:mat (meta (peek relvar))) empty-set-index))

        inserted1
        (if empty-index
          (fn inserted1 [st row]
            (let [idx (st relvar empty-index)]
              (if (contains-row? idx row)
                st
                (let [nidx (index-row idx row)
                      st (assoc st relvar nidx)]
                  (flow-insert1 st row)))))
          flow-insert1)
        inserted
        (if empty-index
          (fn insertedn [st rows]
            (let [idx (st relvar empty-index)
                  new-rows (into [] (remove #(contains-row? idx %)) rows)]
              (if (seq new-rows)
                (-> st
                    (assoc relvar (reduce index-row idx new-rows))
                    (flow-inserts-n new-rows))
                st)))
          flow-inserts-n)

        deleted1
        (if empty-index
          (fn deleted1 [st row]
            (let [idx (st relvar empty-index)]
              (if (contains-row? idx row)
                (let [nidx (unindex-row idx row)
                      st (assoc st relvar nidx)]
                  (flow-delete1 st row))
                st)))
          flow-delete1)
        deleted
        (if empty-index
          (fn deleted [st rows]
            (let [idx (st relvar empty-index)
                  del-rows (into [] (filter #(contains-row? idx %) rows))]
              (if (seq del-rows)
                (-> st
                    (assoc relvar (reduce unindex-row idx del-rows))
                    (flow-deletes-n del-rows))
                st)))
          flow-deletes-n)]
    {:insert1
     (cond
       insert1 (fn [st row] (insert1 st row inserted inserted1 deleted deleted1))
       insert (fn [st row] (insert st [row] inserted inserted1 deleted deleted1))
       :else (raise "No :insert/:insert1 defined for node/edge" {:node node, :edge edge}))
     :insert
     (cond
       insert (fn [st rows] (insert st rows inserted inserted1 deleted deleted1))
       insert1 (fn [st rows] (reduce #(insert1 %1 %2 inserted inserted1 deleted deleted1) st rows))
       :else (raise "No :insert/:insert1 defined for node/edge" {:node node, :edge edge}))
     :delete1
     (cond
       delete1 (fn [st row] (delete1 st row inserted inserted1 deleted deleted1))
       delete (fn [st row] (delete st [row] inserted inserted1 deleted deleted1))
       :else (raise "No :insert/:insert1 defined for node/edge" {:node node, :edge edge}))
     :delete
     (cond
       delete (fn [st rows] (delete st rows inserted inserted1 deleted deleted1))
       delete1 (fn [st rows] (reduce #(delete1 %1 %2 inserted inserted1 deleted deleted1) st rows))
       :else (raise "No :insert/:insert1 defined for node/edge" {:node node, :edge edge}))}))

(defn add-mat-fns [g relvars]

  (let [og g
        g (add-to-graph og relvars)
        wrap-insertn
        (fn wrap-insertn
          [{:keys [insert insert1]}]
          (fn [st rows]
            (case (count rows)
              0 st
              1 (insert1 st (nth rows 0))
              (insert st rows)))
          insert)

        wrap-deleten
        (fn wrap-deleten
          [{:keys [delete delete1]}]
          (fn [st rows]
            (case (count rows)
              0 st
              1 (delete1 st (nth rows 0))
              (delete st rows)))
          delete)

        visited (volatile! #{})

        rf (fn rf [g relvar edge]

             (assert (g relvar) "Should only request inserter for relvars in dataflow graph")
             (when (nil? edge)
               (when-not (base-relvar? relvar)
                 (raise "Can only modify base relvars" {:relvar relvar})))
             (if (contains? @visited relvar)
               g
               (let [{:keys [dependents] :as node} (g relvar)
                     g (reduce #(rf %1 %2 relvar) g dependents)
                     flow-mat-fns (mapv #((:mat-fns (g %)) relvar) dependents)

                     flow-inserters-n (mapv :insert flow-mat-fns)
                     flow-deleters-n (mapv :delete flow-mat-fns)

                     flow-inserts-n
                     (case (count flow-inserters-n)
                       0 (fn [st _] st)
                       1 (first flow-inserters-n)
                       (fn [st rows]
                         (reduce (fn [st f] (f st rows)) st flow-inserters-n)))

                     flow-deletes-n
                     (case (count flow-deleters-n)
                       0 (fn [st _] st)
                       1 (first flow-deleters-n)
                       (fn [st rows]
                         (reduce (fn [st f] (f st rows)) st flow-deleters-n)))

                     flow-inserters-1 (mapv :insert1 flow-mat-fns)

                     flow-insert1
                     (case (count flow-inserters-1)
                       0 (fn [st _] st)
                       1 (first flow-inserters-1)
                       (fn [st rows]
                         (reduce (fn [st f] (f st rows)) st flow-inserters-1)))

                     flow-deleters-1 (mapv :delete1 flow-mat-fns)

                     flow-delete1
                     (case (count flow-deleters-1)
                       0 (fn [st _] st)
                       1 (first flow-deleters-1)
                       (fn [st rows]
                         (reduce (fn [st f] (f st rows)) st flow-deleters-1)))

                     flow
                     {:flow-inserts-n flow-inserts-n
                      :flow-insert1 flow-insert1
                      :flow-deletes-n flow-deletes-n
                      :flow-delete1 flow-delete1}

                     mat-fns (if (nil? edge) (base-mat-fns g relvar flow) (derived-mat-fns node edge flow))
                     mat-fns (assoc mat-fns :insert (wrap-insertn mat-fns) :delete (wrap-deleten mat-fns))]
                 (assoc-in g [relvar :mat-fns edge] mat-fns))))

        bases
        (reduce (fn ! [acc relvar]
                  (if (base-relvar? relvar)
                    (conj acc relvar)
                    (let [{:keys [deps]} (g relvar)]
                      (reduce ! acc deps))))
                [] relvars)]
    (reduce #(rf %1 %2 nil) g bases)))

(defn- dataflow-transactor [g]
  (let [mat-fns (memoize
                  (fn [relvar]
                    (or (get-in g [relvar :mat-fns nil])
                        (base-mat-fns relvar))))
        insert-n (memoize (comp :insert mat-fns))
        delete-n (memoize (comp :delete mat-fns))]
    (fn transact
      [st tx]
      (cond
        (map? tx) (reduce-kv (fn [st relvar rows] ((insert-n relvar) st rows)) st tx)
        (seq? tx) (reduce transact st tx)
        (nil? tx) st
        :else
        (let [[op relvar & rows] tx]
          (case op
            :insert ((insert-n relvar) st rows)
            :delete ((delete-n relvar) st rows)))))))

(defn- mat-head [relvar]
  (case (count relvar)
    0 (raise "Invalid relvar, no statements" {:relvar relvar})
    (update relvar (dec (count relvar)) vary-meta assoc :mat true)))

(defn- init [m relvar]
  (let [g (::graph m {})
        {:keys [deps
                dependents]} (g relvar)
        m (reduce init m deps)]
    (if (contains? m relvar)
      (let [rows (row-seqable (m relvar))]
        (reduce (fn [m dependent]
                  (let [{:keys [mat-fns]} (g dependent)
                        {:keys [insert]} (mat-fns relvar)]
                    (insert m rows))) m dependents))
      m)))

(defn materialize [st & relvars]
  (let [m (meta st)
        relvars (map mat-head relvars)
        g (add-mat-fns (::graph m {}) relvars)
        transactor (dataflow-transactor g)
        m (assoc m ::transactor transactor, ::graph g)
        m (reduce init m relvars)]
    (with-meta st m)))

(defn transact [st & tx]
  (reduce (or (::transactor (meta st)) (dataflow-transactor {})) st tx))

(defn query [st relvar]
  (some-> (or (get st relvar)
              (get (meta st) relvar)
              (let [st (materialize st relvar)]
                (get (meta st) relvar)))))

(defrecord Escape [x])

(defn esc
  "Escapes `x` so it is not treated differently in an expression, useful for 'quoting' keywords as they
  would normally be interpreted as columns."
  [x]
  (->Escape x))

(defn- expr-row-fn [expr]
  (cond
    (= [] expr) (constantly [])

    (instance? Escape expr) (constantly (.-x ^Escape expr))

    (identical? ::% expr) identity

    (vector? expr)
    (let [[f & args] expr
          [f args]
          (cond
            (qualified-symbol? f) [@(requiring-resolve f) args]
            (symbol? f) [@(resolve f) args]
            (= f :and) [(apply every-pred (map expr-row-fn args))]
            (= f :or) [(apply some-fn (map expr-row-fn args))]
            (= f :not) [not args]
            :else [f args])
          args (map expr-row-fn args)]
      (case (count args)
        0 f
        1 (let [[a] args] (comp f a))
        2 (let [[a b] args] #(f (a %) (b %)))
        3 (let [[a b c] args] #(f (a %) (b %) (c %)))
        4 (let [[a b c d] args] #(f (a %) (b %) (c %) (d %)))
        5 (let [[a b c d e] args] #(f (a %) (b %) (c %) (d %) (e %)))
        (let [get-args (apply juxt args)] #(apply f (get-args %)))))

    (keyword? expr) expr

    (qualified-symbol? expr) @(requiring-resolve expr)

    (symbol? expr) @(resolve expr)

    (fn? expr) expr

    :else (constantly expr)))

(defn- assoc-if-not-nil [m k v]
  (if (nil? v)
    m
    (assoc m k v)))

(defn- select-keys-if-not-nil [m keyseq]
  (reduce #(assoc-if-not-nil %1 %2 (%2 m)) {} keyseq))

(defn- extend-form-fn [form]
  ;;todo sep forms for nil behaviour
  (cond
    (vector? form)
    (let [[binding ?sep expr] form
          expr (if ?sep expr ?sep)
          expr-fn (expr-row-fn expr)]
      (if (keyword? binding)
        #(assoc % binding (expr-fn %))
        #(merge % (select-keys (expr-fn %) binding))))

    :else (throw (ex-info "Not a valid binding form, expected a vector [binding :<- expr]" {}))))

(defn- extend-form-cols [form]
  (let [[binding] form]
    (if (keyword? binding)
      #{binding}
      (set binding))))

(defn- agg-form-fn [form]
  (let [[binding _sep expr] form
        ;;todo sep forms for nil behaviour
        expr-fn
        (cond
          (vector? expr)
          (let [[f & args] expr
                [f args]
                (cond
                  (qualified-symbol? f) [@(requiring-resolve f) args]
                  (symbol? f) [@(resolve f) args]
                  :else [f args])
                args (map expr-row-fn args)]
            (case (count args)
              0 f
              1 (let [[a] args] (fn [rows] (f (keep a rows))))
              (throw (ex-info "Not a valid agg function, only arity 0 or 1 functions are supported." {}))))
          (qualified-symbol? expr) @(requiring-resolve expr)
          (symbol? expr) @(resolve expr)
          (fn? expr) expr
          :else (throw (ex-info "Not a valid agg form, expected a function/symbol/vector" {})))]
    (if (keyword? binding)
      (fn [m rows]
        (assoc m binding (expr-fn rows)))
      (fn [m rows]
        (merge m (set/project (set (expr-fn rows)) binding))))))

(defn- aggs-fn [forms]
  (let [fs (mapv agg-form-fn forms)]
    (fn [group rows] (reduce (fn [m f] (f m rows)) group fs))))

(defn expand-form-xf [form]
  ;; todo sep forms for filter/nil behaviour
  (let [[binding _sep expr] form
        expr-fn (expr-row-fn expr)]
    (if (keyword? binding)
      (mapcat
        (fn [row]
          (for [v (expr-fn row)]
            (assoc row binding v))))
      (mapcat
        (fn [row]
          (for [v (expr-fn row)]
            (merge row (select-keys v binding))))))))

(defn- pass-through-insert [st rows inserted inserted1 deleted deleted1] (inserted st rows))
(defn- pass-through-delete [st rows inserted inserted1 deleted deleted1] (deleted st rows))

(defn- transform-insert [f]
  (fn [st rows inserted inserted1 deleted deleted1] (inserted st (mapv f rows))))

(defn- transform-delete [f]
  (fn [st rows inserted inserted1 deleted deleted1] (deleted st (mapv f rows))))

(defn- mat-noop [st rows inserted inserted1 deleted deleted1] st)

(defmethod graph-node :state
  [_ _]
  {:empty-index empty-set-index})

(defmethod graph-node :from
  [_ [_ relvar]]
  {:deps [relvar]
   :insert {relvar pass-through-insert}
   :delete {relvar pass-through-delete}})

(defmethod graph-node :project
  [left [_ & cols]]
  (let [cols (vec (set cols))
        f (fn [row] (select-keys row cols))]
    {:deps [left]
     :insert {left (transform-insert f)}
     :delete {left (transform-delete f)}}))

(defmethod graph-node :project-away
  [left [_ & cols]]
  (let [cols (vec (set cols))
        f (fn [_ row] (apply dissoc row cols))]
    {:deps [left]
     :insert {left (transform-insert f)}
     :delete {left (transform-delete f)}}))

(defmethod graph-node :union
  [left [_ right]]
  {:deps [(mat-head left) (mat-head right)]
   :insert {left pass-through-insert
            right pass-through-insert}
   :delete {left (fn [st rows inserted inserted1 deleted deleted1]
                   (let [idx2 (st right empty-set-index)
                         del-rows (remove #(contains-row? idx2 %) rows)]
                     (deleted st del-rows)))
            right (fn [st rows inserted inserted1 deleted deleted1]
                    (let [idx2 (st left empty-set-index)
                          del-rows (remove #(contains-row? idx2 %) rows)]
                      (deleted st del-rows)))}})

(defmethod graph-node :intersection
  [left [_ right]]
  {:deps [(mat-head left) (mat-head right)]
   :insert {left (fn [st rows inserted inserted1 deleted deleted1]
                   (let [idx2 (st right empty-set-index)
                         add-rows (filter #(contains-row? idx2 %) rows)]
                     (inserted st add-rows)))
            right (fn [st rows inserted inserted1 deleted deleted1]
                    (let [idx2 (st left empty-set-index)
                          add-rows (filter #(contains-row? idx2 %) rows)]
                      (inserted st add-rows)))}
   :delete {left pass-through-delete
            right pass-through-delete}})

(defmethod graph-node :difference
  [left [_ right]]
  {:deps [left right]
   :insert {left (fn [st rows inserted inserted1 deleted deleted1]
                   (let [idx2 (st right empty-set-index)
                         add-rows (remove #(contains-row? idx2 %) rows)]
                     (inserted st add-rows)))
            right (fn [st rows inserted inserted1 deleted deleted1]
                   (let [idx2 (st left empty-set-index)
                         del-rows (filter #(contains-row? idx2 %) rows)]
                     (deleted st del-rows)))}
   :delete {left (fn [st rows inserted inserted1 deleted deleted1]
                   (let [idx2 (st right empty-set-index)
                         del-rows (remove #(contains-row? idx2 %) rows)]
                     (deleted st del-rows)))
            right mat-noop}})

(defmethod graph-node :where
  [left [_ & exprs]]
  (let [expr-preds (mapv expr-row-fn exprs)
        pred-fn (apply every-pred expr-preds)]
    {:deps [left]
     :insert {left (fn [st rows inserted inserted1 deleted deleted1] (inserted st (filterv pred-fn rows)))}
     :delete {left (fn [st rows inserted inserted1 deleted deleted1] (deleted st (filterv pred-fn rows)))}}))

(defmethod graph-node :extend
  [left [_ & extensions]]
  (let [ext-fns (mapv extend-form-fn extensions)
        ext-fn (apply comp (reverse ext-fns))]
    {:deps [left]
     :insert {left (transform-insert ext-fn)}
     :delete {left (transform-delete ext-fn)}}))

(defmethod graph-node :expand
  [left [_ & expansions]]
  (let [exp-fns (mapv expand-form-xf expansions)
        exp-xf (apply comp (reverse exp-fns))]
    {:deps [left]
     :insert {left (fn [st rows inserted inserted1 deleted deleted1] (inserted st (into [] exp-xf rows)))}
     :delete {left (fn [st rows inserted inserted1 deleted deleted1] (deleted st (into [] exp-xf rows)))}}))

(defmethod graph-node :select
  [left [_ & selections]]
  (let [[cols exts] ((juxt filter remove) keyword? selections)
        cols (set (concat cols (mapcat extend-form-cols exts)))
        select-fn (apply comp #(select-keys % cols) (map extend-form-fn (reverse exts)))]
    {:deps [left]
     :insert (transform-insert select-fn)
     :delete (transform-delete select-fn)}))

(defmethod graph-node :hash
  [left [_ & exprs]]
  (let [expr-fns (mapv expr-row-fn exprs)]
    {:empty-index (map-index {} expr-fns)
     :deps [left]
     :insert {left pass-through-insert}
     :delete {left pass-through-delete}}))

(defmethod graph-node :btree
  [left [_ & exprs]]
  (let [expr-fns (mapv expr-row-fn exprs)]
    {:empty-index (map-index (sorted-map) expr-fns)
     :deps [left]
     :insert {left pass-through-insert}
     :delete {left pass-through-delete}}))

(defmethod graph-node :join
  [left [_ right clause]]
  (let [left-exprs (keys clause)
        left (conj left (into [:hash] left-exprs))
        right-path-fn (apply juxt (map expr-row-fn left-exprs))

        right-exprs (vals clause)
        right (conj right (into [:hash] right-exprs))
        left-path-fn (apply juxt (map expr-row-fn right-exprs))
        join-row merge]
    {:deps [left right]
     :insert {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx (st right)
                           matches (for [row rows
                                         :let [path (right-path-fn row)]
                                         match (seek-n idx path)]
                                     (join-row row match))]
                       (inserted st matches)))
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx (st left)
                            matches (for [row rows
                                          :let [path (left-path-fn row)]
                                          match (seek-n idx path)]
                                      (join-row row match))]
                        (inserted st matches)))}
     :delete {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx (st right)
                           matches (for [row rows
                                         :let [path (right-path-fn row)]
                                         match (seek-n idx path)]
                                     (join-row row match))]
                       (deleted st matches)))
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx (st left)
                            matches (for [row rows
                                          :let [path (left-path-fn row)]
                                          match (seek-n idx path)]
                                      (join-row row match))]
                        (deleted st matches)))}}))

(defmethod graph-node :left-join
  [left [_ right clause]]
  (let [left-exprs (keys clause)
        left (conj left (into [:hash] left-exprs))
        right-path-fn (apply juxt (map expr-row-fn left-exprs))

        right-exprs (vals clause)
        right (conj right (into [:hash] right-exprs))
        left-path-fn (apply juxt (map expr-row-fn right-exprs))
        join-row merge]
    {:deps [left right]
     :insert {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx (st right)
                           pairs (for [row rows
                                       :let [path (right-path-fn row)
                                             matches (seek-n idx path)]]
                                   (if (seq matches)
                                     {:delete row
                                      :inserts (mapv #(join-row row %) matches)}
                                     {:inserts [row]}))]
                       (-> st
                           (deleted (keep :delete pairs))
                           (inserted (mapcat :inserts pairs)))))
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx (st left)
                            pairs (for [row rows
                                        :let [path (left-path-fn row)
                                              matches (seek-n idx path)]]
                                    ;; todo deal with resultant merged map being same as original map
                                    {:deletes matches
                                     :inserts (mapv #(join-row % row) matches)})]
                        (-> st
                            (deleted (mapcat :deletes pairs))
                            (inserted (mapcat :inserts pairs)))))}
     :delete {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx (st right)
                           pairs (for [row rows
                                       :let [path (right-path-fn row)
                                             matches (seek-n idx path)]]
                                   (if (seq matches)
                                     {:insert row
                                      :deletes (mapv #(join-row row %) matches)}
                                     {:deletes [row]}))]
                       (-> st
                           (deleted (mapcat :deletes pairs))
                           (inserted (keep :insert pairs)))))
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx (st left)
                            pairs (for [row rows
                                        :let [path (left-path-fn row)
                                              matches (seek-n idx path)]]
                                    ;; todo deal with resultant merged map being same as original map
                                    {:inserted matches
                                     :deletes (mapv #(join-row % row) matches)})]
                        (-> st
                            (deleted (mapcat :deletes pairs))
                            (inserted (mapcat :inserts pairs)))))}}))