(ns com.wotbrew.relic
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

(defn- index-by [k coll]
  (reduce #(assoc % (k %2) %2) {} coll))

(defn- juxt2 [& args]
  (if (empty? args) (constantly []) (apply juxt args)))

(defn- dissoc-in [m ks]
  (if-let [[k & ks] (seq ks)]
    (if (seq ks)
      (let [v (dissoc-in (get m k) ks)]
        (if (empty? v)
          (dissoc m k)
          (assoc m k v)))
      (dissoc m k))
    m))

(defn- disjoc [m k x]
  (let [ns (disj (get m k) x)]
    (if (empty? ns)
      (dissoc m k)
      (assoc m k ns))))

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

(def map-index0
  (vary-meta
    empty-set-index
    assoc
    `seek-n (fn map-seekn [index ks] (if (seq ks) #{} index))))

(defn map-index1 [empty keyfn]
  (let [map-contains-row? (fn map-contains-row? [index row] (contains? (get index (keyfn row) #{}) row))]
    (with-meta
      empty
      {`index-row (fn [index row] (update index (keyfn row) set-conj row))
       `unindex-row (fn [index row] (disjoc index (keyfn row) row))
       `contains-row? map-contains-row?
       `row-seqable (fn enumerate-map [index] (mapcat identity (vals index)))
       `seek-n
       (fn map-seekn [index ks]
         (if (not= 1 (count ks))
           #{}
           (get index (first ks))))})))

(defn map-index [empty keyfns]
  (case (count keyfns)
    0 map-index0
    1 (map-index1 empty (first keyfns))
    (let [path (apply juxt2 keyfns)
          depth (count keyfns)
          map-contains-row? (fn map-contains-row? [index row] (contains? (get-in index (path row) #{}) row))]
      (with-meta
        empty
        {`index-row (fn [index row] (update-in index (path row) set-conj row))
         `unindex-row (fn [index row] (disjoc-in index (path row) row))
         `contains-row? map-contains-row?
         `row-seqable
         (fn enumerate-map
           ([index] (enumerate-map index depth))
           ([index depth]
            (cond
              (= 0 depth) (mapcat identity (vals index))
              (= 1 depth) (mapcat identity (vals index))
              :else
              (for [v (vals index)
                    v2 (enumerate-map v (dec depth))]
                v2))))
         `seek-n
         (fn map-seekn [index ks]
           (let [remaining-depth (- depth (count ks))
                 m-or-coll (if (seq ks) (get-in index ks) (get index nil))]
             (case remaining-depth
               0 m-or-coll
               1 (mapcat identity (vals m-or-coll))
               (if (<= 1 remaining-depth depth)
                 ((fn ! [m-or-coll remaining-depth]
                    (case remaining-depth
                      0 m-or-coll
                      1 (mapcat identity (vals m-or-coll))
                      (mapcat #(! % (dec remaining-depth)) (vals m-or-coll))))
                  m-or-coll
                  remaining-depth)
                 (throw (ex-info "Invalid remaining depth" {}))))))}))))

(defn map-unique-index1 [empty keyfn collision-fn]
  (let [map-contains-row? (fn map-contains-row? [index row]
                            (if-some [k (keyfn row)]
                              (contains? index k)
                              (contains? (index nil #{}) row)))
        replace (fn [a b] (if a (collision-fn a b) b))]
    (with-meta
      empty
      {`index-row (fn [index row]
                    (if-some [k (keyfn row)]
                      (update index k replace row)
                      (update index nil set-conj row)))
       `unindex-row (fn [index row]
                      (if-some [k (keyfn row)]
                        (dissoc index k)
                        (disjoc index nil row)))
       `contains-row? map-contains-row?
       `row-seqable (fn enumerate-map [index] (vals index))
       `seek-n
       (fn map-seekn [index ks]
         (let [k (first ks)]
           (case (count ks)
             0 (concat (vals (dissoc index nil)) (index nil))
             1 (when-some [res (get index k)]
                 (if (nil? k)
                   res
                   [res]))
             #{})))})))

(defn map-unique-index
  ([empty keyfns] (map-unique-index empty keyfns (fn [& _] (raise "Unique key violation"))))
  ([empty keyfns collision-fn]
   (case (count keyfns)
     1 (map-unique-index1 empty (first keyfns) collision-fn)
     (let [path (apply juxt2 keyfns)
           depth (count keyfns)
           map-contains-row? (fn map-contains-row? [index row] (= (get-in index (path row) #{}) row))
           replace (fn [a b] (if a (collision-fn a b) b))]
       (with-meta
         empty
         {`index-row (fn [index row] (update-in index (path row) replace row))
          `unindex-row (fn [index row] (dissoc-in index (path row)))
          `contains-row? map-contains-row?
          `row-seqable
          (fn enumerate-map
            ([index] (enumerate-map index depth))
            ([index depth]
             (cond
               (= 0 depth) (vals index)
               (= 1 depth) (vals index)
               :else
               (for [v (vals index)
                     v2 (enumerate-map v (dec depth))]
                 v2))))
          `seek-n
          (fn map-seekn [index ks]
            (let [remaining-depth (- depth (count ks))
                  m-or-coll (if (seq ks) (get-in index ks) (get index nil))]
              (case remaining-depth
                0 (when (some? m-or-coll) [m-or-coll])
                1 (vals m-or-coll)
                (if (<= 1 remaining-depth depth)
                  ((fn ! [m-or-coll remaining-depth]
                     (case remaining-depth
                       0 (when (some? m-or-coll) [m-or-coll])
                       1 (vals m-or-coll)
                       (mapcat #(! % (dec remaining-depth)) (vals m-or-coll))))
                   m-or-coll
                   remaining-depth)
                  (throw (ex-info "Invalid remaining depth" {}))))))})))))

(defrecord AggIndexNode [size rm cache a b reduced combined results])

(defn- agg-index [key-fn reduce-fn combine-fn complete-fn merge-fn cache-size]
  (letfn [(combine-results [tree1 tree2]
            (combine-reduced (:results tree1) (:results tree2)))
          (combine-reduced [combined reduced]
            (cond
              (nil? combined) reduced
              (nil? reduced) combined
              :else (delay (combine-fn @combined @reduced))))

          (push-cache [tree new-cache reduced]
            (let [{:keys [rm, size, a, b, cache] :or {size 0}} tree]
              (if (seq cache)
                (let [grow-a (< (:size a 0) (:size b 0))
                      branch-id (if grow-a :a :b)
                      a (if grow-a (push-cache a cache reduced) a)
                      b (if grow-a b (push-cache b cache reduced))
                      rm (persistent! (reduce-kv (fn [m k v] (assoc! m k branch-id)) (transient (or rm {})) cache))
                      combined (combine-results a b)]
                  (->AggIndexNode
                    (+ size cache-size)
                    rm
                    new-cache
                    a
                    b
                    reduced
                    combined
                    (combine-reduced reduced combined)))
                (let [combined (combine-results a b)]
                  (->AggIndexNode
                    (+ size cache-size)
                    rm
                    new-cache
                    a
                    b
                    reduced
                    combined
                    (combine-reduced reduced combined))))))

          (add [tree row]
            (let [{:keys [size, rm, cache, a, b, reduced, combined] :or {size 0, cache {}}} tree]
              (cond
                (contains? rm row) tree
                (contains? cache row) tree

                (< (count cache) cache-size)
                (let [new-cache (assoc cache row row)
                      reduced (delay (reduce-fn (vals new-cache)))]
                  (->AggIndexNode
                    (inc size)
                    rm
                    new-cache
                    a
                    b
                    reduced
                    combined
                    (combine-reduced combined reduced)))

                :else
                (let [grow-a (< (:size a 0) (:size b 0))
                      branch (if grow-a a b)
                      branch-id (if grow-a :a :b)
                      fit-cache (= 0 (mod (:size branch 0) cache-size))]
                  (if fit-cache
                    (let [branch (push-cache branch cache reduced)
                          rm (persistent! (reduce-kv (fn [m k v] (assoc! m k branch-id)) (transient (or rm {})) cache))

                          new-cache (array-map row row)
                          reduced (delay (reduce-fn (vals new-cache)))
                          a (if grow-a branch a)
                          b (if grow-a b branch)
                          combined (combine-results a b)]
                      (->AggIndexNode
                        (inc size)
                        rm
                        new-cache
                        a
                        b
                        reduced
                        combined
                        (combine-reduced combined reduced)))
                    (let [branch (add tree row)
                          rm (assoc rm row branch-id)
                          a (if grow-a branch a)
                          b (if grow-a b branch)
                          combined (combine-results a b)]
                      (->AggIndexNode
                        (inc size)
                        rm
                        cache
                        a
                        b
                        reduced
                        combined
                        (combine-reduced combined reduced))))))))

          (shrink [tree]
            (if (<= (:size tree 0) 0)
              nil
              tree))

          (del [tree row]
            (let [{:keys [size
                          rm
                          cache
                          a
                          b
                          combined
                          reduced]} tree]
              (cond
                (contains? cache row)
                (let [new-cache (dissoc cache row)
                      reduced (delay (reduce-fn (vals new-cache)))]
                  (shrink
                    (->AggIndexNode
                      (dec size)
                      rm
                      new-cache
                      a
                      b
                      reduced
                      combined
                      (combine-reduced combined reduced))))

                (contains? rm row)
                (let [branch-id (rm row)
                      shrink-a (= :a branch-id)
                      new-rm (dissoc rm row)
                      a (if shrink-a (del a row) a)
                      b (if shrink-a b (del b row))
                      combined (combine-results a b)]
                  (shrink
                    (->AggIndexNode
                      (dec size)
                      new-rm
                      cache
                      a
                      b
                      reduced
                      combined
                      (combine-reduced combined reduced))))

                :else tree)))]
    (with-meta
      {}
      {`index-row (fn [index row]
                    (let [k (key-fn row)
                          tree (index k)
                          tree (add tree row)
                          irow (delay (merge-fn k (complete-fn @(:results tree))))
                          tree (assoc tree :indexed-row irow)]
                      (assoc index k tree)))
       `unindex-row (fn [index row]
                      (let [k (key-fn row)
                            tree (index k)
                            tree (del tree row)]
                        (if (nil? tree)
                          (dissoc index k)
                          (assoc index k (assoc tree :indexed-row (delay (merge-fn k (complete-fn @(:results tree)))))))))
       `contains-row? (fn [index row]
                        (let [k (key-fn row)
                              {:keys [rm cache]} (index k)]
                          (or (contains? rm row)
                              (contains? cache row))))
       `row-seqable (fn [index] (mapcat (comp keys :rowmap) (vals index)))})))

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

(defn unwrap-base [relvar]
  (cond
    (base-relvar? relvar) relvar
    (= :from (operator (peek relvar))) (let [[_ relvar] (peek relvar)] (unwrap-base relvar))
    (= :state (operator (peek relvar))) [(peek relvar)]
    :else nil))

(defn- base-mat-fns
  [g relvar flow]
  (let [{:keys [empty-index]} (if g (g relvar) (graph-node [] (peek relvar)))
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
                 st (vary-meta st assoc relvar nidx)
                 st (vary-meta st flow-inserts-n new-rows)]
             st)
           st)))
     :insert1
     (fn base-insert1 [st row]
       (let [oidx (st relvar empty-index)]
         (if (contains-row? oidx row)
           st
           (let [nidx (index-row oidx row)
                 st (assoc st relvar nidx)
                 st (vary-meta st assoc relvar nidx)
                 st (vary-meta st flow-insert1 row)]
             st))))
     :delete
     (fn base-delete [st rows]
       (let [oidx (st relvar empty-index)
             deleted-rows (filterv #(contains-row? oidx %) rows)]
         (if (seq deleted-rows)
           (let [nidx (reduce unindex-row oidx deleted-rows)
                 st (assoc st relvar nidx)
                 st (vary-meta st assoc relvar nidx)
                 st (vary-meta st flow-deletes-n deleted-rows)]
             st)
           st)))
     :delete1
     (fn base-delete1 [st row]
       (let [oidx (st relvar empty-index)]
         (if-not (contains-row? oidx row)
           st
           (let [nidx (unindex-row oidx row)
                 st (assoc st relvar nidx)
                 st (vary-meta st assoc relvar nidx)
                 st (vary-meta st flow-delete1 row)]
             st))))}))

(defn- mat-noop
  ([st rows] st)
  ([st rows inserted inserted1 deleted deleted1] st))

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
                  {:keys [deps, implicit]
                   :as node} (graph-node left stmt)
                  contains (contains? g relvar)
                  g (update g relvar merge-node node relvar stmt)]
              (if contains
                g
                (let [g (reduce (fn [g dep] (depend g relvar dep)) g deps)
                      g (reduce add g implicit)]
                  g))))]
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
                  del-rows (filterv #(contains-row? idx %) rows)]
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
       :else mat-noop)
     :insert
     (cond
       insert (fn [st rows] (insert st rows inserted inserted1 deleted deleted1))
       insert1 (fn [st rows] (reduce #(insert1 %1 %2 inserted inserted1 deleted deleted1) st rows))
       :else mat-noop)
     :delete1
     (cond
       delete1 (fn [st row] (delete1 st row inserted inserted1 deleted deleted1))
       delete (fn [st row] (delete st [row] inserted inserted1 deleted deleted1))
       :else mat-noop)
     :delete
     (cond
       delete (fn [st rows] (delete st rows inserted inserted1 deleted deleted1))
       delete1 (fn [st rows] (reduce #(delete1 %1 %2 inserted inserted1 deleted deleted1) st rows))
       :else mat-noop)}))

(def ^:private ^:dynamic *added* nil)
(def ^:private ^:dynamic *deleted* nil)

(defn- record-added [relvar rows]
  (when (and *added* (*added* relvar)) (set! *added* (update *added* relvar into rows))))

(defn- record-deleted [relvar rows]
  (when (and *deleted* (*deleted* relvar)) (set! *deleted* (update *deleted* relvar into rows))))

(defn- record-added1 [relvar row]
  (when (and *added* (*added* relvar)) (set! *added* (update *added* relvar conj row))))

(defn- record-deleted1 [relvar row]
  (when (and *deleted* (*deleted* relvar)) (set! *deleted* (update *deleted* relvar conj row))))

(defn- with-added-hook [relvar f]
  (fn [st rows] (record-added relvar rows) (f st rows)))

(defn- with-deleted-hook [relvar f]
  (fn [st rows] (record-deleted relvar rows) (f st rows)))

(defn- with-added-hook1 [relvar f]
  (fn [st row] (record-added1 relvar row) (f st row)))

(defn- with-deleted-hook1 [relvar f]
  (fn [st row] (record-deleted1 relvar row) (f st row)))

(defn add-mat-fns [g relvars]
  (let [og g
        g (add-to-graph og relvars)

        wrap-insertn
        (fn wrap-insertn
          [{:keys [insert insert1]}]
          (fn [st rows]
            (case (count rows)
              0 st
              1 (insert1 st (first rows))
              (insert st rows))))

        wrap-deleten
        (fn wrap-deleten
          [{:keys [delete delete1]}]
          (fn [st rows]
            (case (count rows)
              0 st
              1 (delete1 st (first rows))
              (delete st rows))))

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
                       0 (fn [st rows] st)
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
                     {:flow-inserts-n (with-added-hook relvar flow-inserts-n)
                      :flow-insert1 (with-added-hook1 relvar flow-insert1)
                      :flow-deletes-n (with-deleted-hook relvar flow-deletes-n)
                      :flow-delete1 (with-deleted-hook1 relvar flow-delete1)}

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
                    (assert (base-relvar? relvar) "Can only modify :state relvars")
                    (or (get-in g [relvar :mat-fns nil])
                        (-> {}
                            (add-mat-fns [relvar])
                            (get-in [relvar :mat-fns nil])))))
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
                dependents]} (g relvar)]
    (let [m (reduce init m deps)
          rows (delay (row-seqable (m relvar empty-set-index)))]
      (reduce (fn [m dependent]
                (let [{:keys [mat-fns]} (g dependent)
                      {:keys [insert]} (mat-fns relvar)
                      init-key [::init dependent relvar]]
                  (if (contains? m init-key)
                    m
                    (-> m
                        (assoc init-key true)
                        (insert @rows))))) m dependents))))

(defn materialize [st & relvars]
  (let [m (meta st)
        relvars (map mat-head relvars)
        g (add-mat-fns (::graph m {}) relvars)
        transactor (dataflow-transactor g)
        m (assoc m ::transactor transactor, ::graph g)
        m (reduce init m relvars)]
    (with-meta st m)))

(defn transact [st & tx]
  (if-some [transactor (::transactor (meta st))]
    (reduce transactor st tx)
    (apply transact (vary-meta st assoc ::transactor (dataflow-transactor {})) tx)))

(defn- materialized-relation [st relvar]
  (or (get st relvar)
      (get (meta st) relvar)))

(defn index [st relvar]
  (or (materialized-relation st relvar)
      (let [st (materialize st relvar)]
        (get (meta st) relvar))))

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
            (= f :if) (let [[c t e] (map expr-row-fn args)]
                        [(if e
                           (fn [row]
                             (if (c row)
                               (t row)
                               (e row)))
                           (fn [row]
                             (if (c row)
                               (t row))))])
            :else [f args])
          args (map expr-row-fn args)]
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

    (qualified-symbol? expr) @(requiring-resolve expr)

    (symbol? expr) @(resolve expr)

    (fn? expr) expr

    :else (constantly expr)))

(defn q
  ([st relvar-or-binds]
   (if (map? relvar-or-binds)
     (let [relvars (keep (fn [q] (if (map? q) (:q q) q)) (vals relvar-or-binds))
           missing (remove #(materialized-relation st %) relvars)
           st (apply materialize st missing)]
       (reduce-kv
         (fn [m k qr]
           (if (map? q)
             (assoc m k (q st (:q qr) qr))
             (assoc m k (q st qr))))
         {}
         relvar-or-binds))
     (some-> (index st relvar-or-binds) row-seqable)))
  ([st relvar opts]
   (let [rs (some-> (index st relvar) row-seqable)
         {:keys [sort
                 xf]
          into-coll :into} opts

         sort-asc-fn
         (cond
           (nil? sort) nil
           (keyword? sort) sort
           :else (apply juxt2 (map expr-row-fn sort)))

         rs (if sort-asc-fn
              (sort-by sort-asc-fn rs)
              rs)

         rs (cond
             into-coll (if xf (into into-coll xf rs) (into into-coll rs))
             xf (sequence xf rs)
             :else rs)]
     rs)))

(defn what-if [st relvar & tx]
  (q (apply transact st tx) relvar))

(defn watch [st & relvars]
  (let [st (vary-meta st update ::watched (fnil into #{}) relvars)]
    (apply materialize st relvars)))

(defn track-transact [st & tx]
  (binding [*added* (zipmap (::watched (meta st)) (repeat #{}))
            *deleted* (zipmap (::watched (meta st)) (repeat #{}))]
    (let [ost st
          st (apply transact st tx)
          added (for [[relvar added] *added*
                      :let [idx (index st relvar)]]
                  [relvar {:added (filterv #(contains-row? idx %) added)}])
          deleted (for [[relvar deleted] *deleted*
                        :let [oidx (index ost relvar)
                              idx (index st relvar)]]
                    [relvar {:deleted (filterv (every-pred #(not (contains-row? idx %))
                                                           #(contains-row? oidx %)) deleted)}])]
      {:result st
       :changes (merge-with merge (into {} added) (into {} deleted))})))


(defn- assoc-if-not-nil [m k v]
  (if (nil? v)
    m
    (assoc m k v)))

(defn- select-keys-if-not-nil [m keyseq]
  (reduce #(assoc-if-not-nil %1 %2 (%2 m)) {} keyseq))

(defn- extend-form-fn [form]
  (cond
    (vector? form)
    (let [[binding expr] form
          expr-fn (expr-row-fn expr)]
      (if (keyword? binding)
        #(assoc % binding (expr-fn %))
        #(merge % (select-keys (expr-fn %) binding))))

    :else (throw (ex-info "Not a valid binding form, expected a vector [binding expr]" {}))))

(defn- extend-form-cols [form]
  (let [[binding] form]
    (if (keyword? binding)
      #{binding}
      (set binding))))

(defn expand-form-xf [form]
  (let [[binding expr] form
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

(defn- pass-through-insert1 [st row inserted inserted1 deleted deleted1] (inserted1 st row))
(defn- pass-through-delete1 [st row inserted inserted1 deleted deleted1] (deleted1 st row))

(defn- transform-insert [f]
  (fn [st rows inserted inserted1 deleted deleted1]
    (inserted st (mapv f rows))))

(defn- transform-delete [f]
  (fn [st rows inserted inserted1 deleted deleted1]
    (deleted st (mapv f rows))))

(defn- transform-insert1 [f]
  (fn [st row inserted inserted1 deleted deleted1]
    (inserted1 st (f row))))

(defn- transform-delete1 [f]
  (fn [st row inserted inserted1 deleted deleted1]
    (deleted1 st (f row))))

(defmulti col-data* (fn [_ stmt] (operator stmt)))

(defn col-data [relvar]
  (if (empty? relvar)
    []
    (let [stmt (peek relvar)
          left (pop relvar)]
      (col-data* left stmt))))

(defn state-deps [relvar]
  (if-some [state (unwrap-base relvar)]
    #{state}
    (let [left (pop relvar)
          stmt (peek relvar)
          {:keys [deps]} (graph-node left stmt)]
      (set (mapcat state-deps deps)))))

(defn known-keys [relvar]
  (map :k (col-data relvar)))

(defmethod col-data* :default [_ _]
  {})

(defmethod graph-node :state
  [left [_ _ {:keys [pk]} :as stmt]]
  (if (seq left)
    (graph-node left [:from [stmt]])
    {:empty-index (if pk (map-unique-index {} (mapv expr-row-fn pk)) empty-set-index)}))

(defmethod col-data* :state
  [_ [_ _ {:keys [req]} :as stmt]]
  (for [k req] {:k k}))

(defmethod graph-node :from
  [_ [_ relvar]]
  {:deps [relvar]
   :insert {relvar pass-through-insert}
   :insert1 {relvar pass-through-insert1}
   :delete {relvar pass-through-delete}
   :delete1 {relvar pass-through-delete1}})

(defmethod col-data* :from
  [_ [_ relvar]]
  (col-data relvar))

(defmethod graph-node :project
  [left [_ & cols]]
  (let [cols (vec (set cols))
        f (fn [row] (select-keys row cols))]
    {:deps [left]
     :insert {left (transform-insert f)}
     :insert1 {left (transform-insert1 f)}
     :delete {left (transform-delete f)}
     :delete1 {left (transform-delete1 f)}}))

(defmethod col-data* :project
  [left [_ & cols]]
  (filter (comp (set cols) :k) (col-data left)))

(defmethod graph-node :project-away
  [left [_ & cols]]
  (let [cols (vec (set cols))
        f (fn [row] (apply dissoc row cols))]
    {:deps [left]
     :insert {left (transform-insert f)}
     :insert1 {left (transform-insert1 f)}
     :delete {left (transform-delete f)}
     :delete1 {left (transform-delete1 f)}}))

(defmethod col-data* :project-away
  [left [_ & cols]]
  (remove (comp (set cols) :k) (col-data left)))

(defmethod graph-node :union
  [left [_ right]]
  (let [left (mat-head left)
        right (mat-head right)]
    {:deps [left right]
     :insert {left pass-through-insert
              right pass-through-insert}
     :insert1 {left pass-through-insert1
               right pass-through-insert1}
     :delete {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx2 (st right empty-set-index)
                           del-rows (remove #(contains-row? idx2 %) rows)]
                       (deleted st del-rows)))
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx2 (st left empty-set-index)
                            del-rows (remove #(contains-row? idx2 %) rows)]
                        (deleted st del-rows)))}
     :delete1 {left (fn [st row inserted inserted1 deleted deleted1]
                      (let [idx2 (st right empty-set-index)]
                        (if (contains-row? idx2 row)
                          st
                          (deleted1 st row))))
               right (fn [st row inserted inserted1 deleted deleted1]
                       (let [idx2 (st left empty-set-index)]
                         (if (contains-row? idx2 row)
                           st
                           (deleted1 st row))))}}))

(defmethod col-data* :union
  [left [_ right]]
  (let [left-idx (index-by :k (col-data left))
        right-idx (index-by :k (col-data right))]
    (concat
      (for [[k col] left-idx]
        (if (right-idx k)
          (if (= col (right-idx k))
            ;; todo looser check than = ?
            col
            {:k k})
          col))
      (for [[k col] right-idx
            :when (not (left-idx k))]
        col))))

(defmethod graph-node :intersection
  [left [_ right]]
  (let [left (mat-head left)
        right (mat-head right)]
    {:deps [left right]
     :insert {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx2 (st right empty-set-index)
                           add-rows (filter #(contains-row? idx2 %) rows)]
                       (inserted st add-rows)))
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx2 (st left empty-set-index)
                            add-rows (filter #(contains-row? idx2 %) rows)]
                        (inserted st add-rows)))}
     :insert1 {left (fn [st row inserted inserted1 deleted deleted1]
                      (let [idx2 (st right empty-set-index)]
                        (if (contains-row? idx2 row)
                          (inserted1 st row)
                          st)))
               right (fn [st row inserted inserted1 deleted deleted1]
                       (let [idx2 (st left empty-set-index)]
                         (if (contains-row? idx2 row)
                           (inserted1 st row)
                           st)))}
     :delete {left pass-through-delete
              right pass-through-delete}
     :delete1 {left pass-through-delete1
               right pass-through-delete1}}))

(defmethod col-data* :intersection
  [left [_ _]]
  (col-data left))

(defmethod graph-node :difference
  [left [_ right]]
  (let [left (mat-head left)
        right (mat-head right)]
    {:deps [left right]
     :insert {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx2 (st right empty-set-index)
                           add-rows (remove #(contains-row? idx2 %) rows)]
                       (inserted st add-rows)))
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx2 (st left empty-set-index)
                            del-rows (filter #(contains-row? idx2 %) rows)]
                        (deleted st del-rows)))}
     :insert1 {left (fn [st row inserted inserted1 deleted deleted1]
                      (let [idx2 (st right empty-set-index)]
                        (if (contains-row? idx2 row)
                          st
                          (inserted1 st row))))
               right (fn [st row inserted inserted1 deleted deleted1]
                       (let [idx2 (st left empty-set-index)]
                         (if (contains-row? idx2 row)
                           (deleted1 st row)
                           st)))}
     :delete {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx2 (st right empty-set-index)
                           del-rows (remove #(contains-row? idx2 %) rows)]
                       (deleted st del-rows)))
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx2 (st left empty-set-index)
                            add-rows (filter #(contains-row? idx2 %) rows)]
                        (inserted st add-rows)))}
     :delete1 {left (fn [st row inserted inserted1 deleted deleted1]
                      (let [idx2 (st right empty-set-index)]
                        (if (contains-row? idx2 row)
                          st
                          (deleted1 st row))))
               right (fn [st row inserted inserted1 deleted deleted1]
                       (let [idx2 (st left empty-set-index)]
                         (if (contains-row? idx2 row)
                           (inserted1 st row)
                           st)))}}))

(defmethod col-data* :difference
  [left [_ _]]
  (col-data left))

(defmethod graph-node :where
  [left [_ & exprs]]
  (let [expr-preds (mapv expr-row-fn exprs)
        pred-fn (apply every-pred expr-preds)]
    {:deps [left]
     :insert {left (fn [st rows inserted inserted1 deleted deleted1]
                     (inserted st (filterv pred-fn rows)))}
     :insert1 {left (fn [st row inserted inserted1 deleted deleted1]
                      (if (pred-fn row)
                        (inserted1 st row)
                        st))}
     :delete {left (fn [st rows inserted inserted1 deleted deleted1] (deleted st (filterv pred-fn rows)))}
     :delete1 {left (fn [st row inserted inserted1 deleted deleted1] (if (pred-fn row) (deleted1 st row) st))}}))

(defmethod col-data* :where
  [left _]
  (col-data left))

(defrecord JoinColl [relvar clause])
(defn join-coll [relvar clause] (->JoinColl relvar clause))
(defn- join-expr? [expr] (instance? JoinColl expr))
(defn- extend-expr [[_ _ expr]] expr)
(defn- join-ext? [extension] (join-expr? (extend-expr extension)))

(defrecord JoinFirst [relvar clause])
(defn join-first [relvar clause] (->JoinFirst relvar clause))
(defn- join-first-expr? [expr] (instance? JoinFirst expr))
(defn- join-first-ext? [extension] (join-first-expr? (extend-expr extension)))

(defmethod graph-node ::extend*
  [left [_ & extensions]]
  (let [f (apply comp (map extend-form-fn (reverse extensions)))]
    {:deps [left]
     :insert {left (transform-insert f)}
     :insert1 {left (transform-insert1 f)}
     :delete {left (transform-delete f)}
     :delete1 {left (transform-delete1 f)}}))

(defmethod graph-node :extend
  [left [_ & extensions]]
  (->> (for [extensions (partition-by (fn [ext] (cond
                                                  (join-ext? ext) :join
                                                  (join-first-ext? ext) :join1
                                                  :else :std)) extensions)]
         (cond
           (join-ext? (first extensions))
           (for [[binding {:keys [relvar clause]}] extensions
                 :let [_ (assert (keyword? binding) "only keyword bindings accepted for join-as-coll")]]
             [:join-as-coll relvar clause binding])

           (join-first-ext? (first extensions))
           (for [[binding {:keys [relvar clause]}] extensions
                 :let [_ (assert (keyword? binding) "only keyword bindings accepted for join-as-coll")]
                 stmt [[:join-as-coll relvar clause binding]
                       (into [::extend*] (for [[binding] extensions] [binding [first binding]]))]]
             stmt)

           :else [(into [::extend*] extensions)]))
       (transduce cat conj left)
       ((fn [relvar]
          (graph-node (pop relvar) (peek relvar))))))

(defmethod col-data* :extend
  [left [_ & extensions]]
  (let [ext-keys (set (mapcat extend-form-cols extensions))]
    (concat
      (for [col (col-data left)
            :when (not (ext-keys (:k col)))]
        col)
      (for [k ext-keys]
        {:k k}))))

(defmethod graph-node :qualify
  [left [_ namespace]]
  (let [namespace (name namespace)
        f #(reduce-kv (fn [m k v] (assoc m (keyword namespace (name k)) v)) {} %)]
    {:deps [left]
     :insert {left (transform-insert f)}
     :insert1 {left (transform-insert1 f)}
     :delete {left (transform-delete f)}
     :delete1 {left (transform-delete1 f)}}))

(defmethod col-data* :qualify
  [left [_ namespace] _]
  (for [col (col-data left)]
    (assoc col :k (keyword namespace (name (:k col))))))

(defmethod graph-node :rename
  [left [_ renames]]
  (let [extensions (for [[from to] renames] [to from])
        away (keys renames)
        dep (conj left (into [:extend] extensions) (into [:project-away] away))]
    {:deps [dep]
     :insert {dep pass-through-insert}
     :insert1 {dep pass-through-insert1}
     :delete {dep pass-through-delete}
     :delete1 {dep pass-through-delete1}}))

(defmethod col-data* :rename
  [left [_ renames]]
  (for [col (col-data left)
        :let [nk (get renames (:k col))]]
    (if nk
      (assoc col :k nk)
      col)))

(defmethod graph-node :expand
  [left [_ & expansions]]
  (let [exp-fns (mapv expand-form-xf expansions)
        exp-xf (apply comp (reverse exp-fns))]
    {:deps [left]
     :insert {left (fn [st rows inserted inserted1 deleted deleted1] (inserted st (into [] exp-xf rows)))}
     :delete {left (fn [st rows inserted inserted1 deleted deleted1] (deleted st (into [] exp-xf rows)))}}))

(defmethod col-data* :expand
  [left [_ & expansions]]
  (let [exp-keys (set (for [[binding] expansions
                            k (if (vector? binding) binding [binding])]
                        k))]
    (concat
      (for [col (col-data left)
            :when (not (exp-keys (:k col)))]
        col)
      (for [k exp-keys]
        {:k k}))))

(defmethod graph-node :select
  [left [_ & selections]]
  (let [[cols exts] ((juxt filter remove) keyword? selections)
        cols (set (concat cols (mapcat extend-form-cols exts)))
        select-fn (apply comp #(select-keys % cols) (map extend-form-fn (reverse exts)))]
    {:deps [left]
     :insert {left (transform-insert select-fn)}
     :insert1 {left (transform-insert1 select-fn)}
     :delete {left (transform-delete select-fn)}
     :delete1 {left (transform-delete1 select-fn)}}))

(defmethod col-data* :select
  [left [_ & selections]]
  (let [[cols exts] ((juxt filter remove) keyword? selections)
        cols (set (concat cols (mapcat extend-form-cols exts)))]
    (concat
      (for [col (col-data left)
            :when (not (cols (:k col)))]
        col)
      (for [k cols]
        {:k k}))))

(defmethod graph-node :hash
  [left [_ & exprs]]
  (let [expr-fns (mapv expr-row-fn exprs)]
    {:empty-index (map-index {} expr-fns)
     :deps [left]
     :insert {left pass-through-insert}
     :insert1 {left pass-through-insert1}
     :delete {left pass-through-delete}
     :delete1 {left pass-through-delete1}}))

(defmethod graph-node :unique
  [left [_ & exprs]]
  (let [expr-fns (mapv expr-row-fn exprs)]
    {:empty-index (map-unique-index {} expr-fns)
     :deps [left]
     :insert {left pass-through-insert}
     :insert1 {left pass-through-insert1}
     :delete {left pass-through-delete}
     :delete1 {left pass-through-delete1}}))

(defmethod col-data* :hash
  [left _]
  (col-data left))

(defmethod graph-node :btree
  [left [_ & exprs]]
  (let [expr-fns (mapv expr-row-fn exprs)]
    {:empty-index (map-index (sorted-map) expr-fns)
     :deps [left]
     :insert {left pass-through-insert}
     :insert1 {left pass-through-insert1}
     :delete {left pass-through-delete}
     :delete1 {left pass-through-delete1}}))

(defmethod col-data* :btree
  [left _]
  (col-data left))

(defmethod graph-node :join
  [left [_ right clause]]
  (let [left-exprs (keys clause)
        left (conj left (into [:hash] left-exprs))
        right-path-fn (apply juxt2 (map expr-row-fn left-exprs))

        right-exprs (vals clause)
        right (conj right (into [:hash] right-exprs))
        left-path-fn (apply juxt2 (map expr-row-fn right-exprs))
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
                                      (join-row match row))]
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
                                      (join-row match row))]
                        (deleted st matches)))}}))

(defmethod graph-node :fk
  [left [_ right clause]]
  (let [left-base (unwrap-base left)
        _ (when-not left-base (raise "FK can only depend on base relvar"))
        left-exprs (keys clause)
        left (conj left-base (into [:hash] left-exprs))
        right-path-fn (apply juxt2 (map expr-row-fn left-exprs))

        right-exprs (vals clause)
        right (conj right (into [:hash] right-exprs))
        left-path-fn (apply juxt2 (map expr-row-fn right-exprs))]
    {:deps [left right]
     :insert {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx (st right)
                           matches (for [row rows
                                         :let [path (right-path-fn row)]
                                         match (seek-n idx path)]
                                     match)]
                       (when (empty? matches) (raise "FK violation (left)"))
                       st))
              right mat-noop}
     :delete {left mat-noop
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx (st left)
                            matches (for [row rows
                                          :let [path (left-path-fn row)]
                                          match (seek-n idx path)]
                                      match)]
                        (if (seq matches)
                          (raise "FK violation (right)")
                          st)))}}))

(defmethod col-data* :join
  [left [_ right]]
  (let [left-cols (col-data left)
        right-cols (col-data right)
        right-idx (reduce #(assoc %1 (:k %2) %2) {} right-cols)]
    (concat
      (remove (comp right-idx :k) left-cols)
      ;; todo switch :opt if could-be-right
      right-cols)))

(defmethod graph-node ::join-as-coll
  [left [_ right clause k]]
  (let [left-exprs (keys clause)
        left (conj left (into [:hash] left-exprs))
        right-path-fn (apply juxt2 (map expr-row-fn left-exprs))

        right-exprs (vals clause)
        right (conj right (into [:hash] right-exprs))
        left-path-fn (apply juxt2 (map expr-row-fn right-exprs))
        join-matches (fn [m rows] (assoc m k (set rows)))]
    {:deps [left right]
     :insert {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx (st right)
                           matches (for [row rows
                                         :let [path (right-path-fn row)
                                               matches (seek-n idx path)]]
                                     (join-matches row matches))]
                       (inserted st matches)))
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx (st left)
                            right-idx (st right)
                            nrows (group-by left-path-fn rows)
                            lrows (for [row rows
                                        :let [path (left-path-fn row)]
                                        match (seek-n idx path)]
                                    match)
                            deletes (set (for [row lrows
                                               :let [path (right-path-fn row)
                                                     matches (set (seek-n right-idx path))]]
                                           (join-matches row (set/difference matches (set (nrows path))))))
                            inserts (set (for [row lrows
                                               :let [path (right-path-fn row)
                                                     matches (seek-n right-idx path)]]
                                           (join-matches row matches)))]
                        (-> st
                            (deleted deletes)
                            (inserted inserts))))}
     :delete {left (fn [st rows inserted inserted1 deleted deleted1]
                     (let [idx (st right)
                           matches (for [row rows
                                         :let [path (right-path-fn row)
                                               matches (seek-n idx path)]]
                                     (join-matches row matches))]
                       (deleted st matches)))
              right (fn [st rows inserted inserted1 deleted deleted1]
                      (let [idx (st left)
                            right-idx (st right)
                            nrows (group-by left-path-fn rows)
                            lrows (for [row rows
                                        :let [path (left-path-fn row)]
                                        match (seek-n idx path)]
                                    match)
                            deletes (set (for [row lrows
                                               :let [path (right-path-fn row)
                                                     matches (seek-n right-idx path)]]
                                           (join-matches row (set/union (set (nrows path)) (set matches)))))
                            inserts (set (for [row lrows
                                               :let [path (right-path-fn row)
                                                     matches (seek-n right-idx path)]]
                                           (join-matches row (set matches))))]
                        (-> st
                            (deleted deletes)
                            (inserted inserts))))}}))

(defmethod graph-node :left-join
  [left [_ right clause]]
  (let [join-as-coll (conj left [::join-as-coll right clause ::left-join])
        join-row merge]
    {:deps [join-as-coll]
     :insert {join-as-coll (fn [st rows inserted inserted1 deleted deleted1]
                             (->> (for [row rows
                                        :let [rrows (::left-join row)
                                              row (dissoc row ::left-join)]
                                        nrows (if (empty? rrows)
                                                [row]
                                                (map #(join-row row %) rrows))]
                                    nrows)
                                  (inserted st)))}
     :delete {join-as-coll (fn [st rows inserted inserted1 deleted deleted1]
                             (->> (for [row rows
                                        :let [rrows (::left-join row)
                                              row (dissoc row ::left-join)]
                                        nrows (if (empty? rrows)
                                                [row]
                                                (map #(join-row row %) rrows))]
                                    nrows)
                                  (deleted st)))}}))

(defmethod col-data* :left-join
  [left [_ right clause]]
  [left [_ right]]
  (let [left-cols (col-data left)
        right-cols (col-data right)
        right-idx (reduce #(assoc %1 (:k %2) %2) {} right-cols)]
    (concat
      (remove (comp right-idx :k) left-cols)
      ;; todo flag optionality
      right-cols)))

;; todo :agg custom indexes
;; sometimes it'll be more efficient to multi-index and join
;; for count, count-distinct, group etc

(defn row-count []
  {:combiner +
   :reducer count})

(defn greatest-by [expr]
  (let [f (expr-row-fn expr)
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

(defn least-by [expr]
  (let [f (expr-row-fn expr)
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

(defn- comp-complete [agg f]
  (let [{:keys [complete]} agg]
    (if complete
      (assoc agg :complete (comp f complete))
      (assoc agg :complete f))))

(defn greatest [expr]
  (comp-complete (greatest-by expr) (expr-row-fn expr)))

(defn least [expr]
  (comp-complete (least-by expr) (expr-row-fn expr)))

(defn sum [& exprs]
  (case (count exprs)
    0 {:combiner (constantly 0) :reducer (constantly 0)}
    1
    (let [expr (first exprs)
          f (expr-row-fn expr)
          xf (keep f)]
      {:combiner +'
       :reducer #(transduce xf +' %)})
    (let [fns (map expr-row-fn exprs)
          nums (apply juxt fns)
          xf (comp (mapcat nums) (remove nil?))]
      {:combiner +'
       :reducer #(transduce xf +' %)})))

(defn set-concat [& exprs]
  (let [f (apply juxt (map expr-row-fn exprs))
        xf (comp (mapcat f) (remove nil?))]
    {:combiner set/union
     :reducer (fn [rows] (into #{} xf rows))}))

(defn distinct-count [& exprs]
  (let [agg (apply set-concat exprs)]
    (comp-complete agg count)))

(defn- agg-expr-agg [expr]
  (cond
    (map? expr) expr

    (#{count 'count `count} expr) (row-count)

    (fn? expr) (expr)

    (vector? expr)
    (let [[f & args] expr]
      (if (#{count 'count `count} f)
        (row-count)
        (apply f args)))

    :else (throw (ex-info "Not a valid agg form, expected a agg function or vector" {:expr expr}))))

(defn- agg-form-fn [form]
  (let [[binding expr] form {:keys [combiner reducer complete]} (agg-expr-agg expr)]
    (assert (keyword? binding) "Only keyword bindings allowed for agg forms")
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

(defn- maybe-deref [x] (some-> x deref))

(defmethod graph-node :agg
  [left [_ cols & aggs]]
  (let [cols (vec (set cols))
        key-fn #(select-keys % cols)
        {:keys [reducer combiner complete merger]} (agg-fns aggs)
        complete (or complete identity)
        merge-fn (or merger merge)
        empty-idx (agg-index key-fn reducer combiner complete merge-fn 32)
        idx-key [::agg-index left cols aggs]]
    {:deps [left]
     :empty-index empty-set-index
     :insert {left (fn [st rows insert insert1 delete delete1]
                     (let [idx (st idx-key empty-idx)
                           ks (set (map key-fn rows))
                           old-rows (keep (comp maybe-deref :indexed-row idx) ks)
                           nidx (reduce index-row idx rows)
                           new-rows (keep (comp maybe-deref :indexed-row nidx) ks)
                           st (assoc st idx-key nidx)]
                       (-> st
                           (delete old-rows)
                           (insert new-rows))))}
     :delete {left (fn [st rows insert insert1 delete delete1]
                     (let [idx (st idx-key empty-idx)
                           ks (set (map key-fn rows))
                           old-rows (keep (comp maybe-deref :indexed-row idx) ks)
                           nidx (reduce unindex-row idx rows)
                           new-rows (keep (comp maybe-deref :indexed-row nidx) ks)
                           st (assoc st idx-key nidx)]
                       (-> st
                           (delete old-rows)
                           (insert new-rows))))}}))

(defmethod col-data* :agg
  [left [_ cols & aggs]]
  (let [agg-keys (set (for [[binding] aggs] binding))
        left-cols (filter (comp (set cols) :k) (col-data left))]
    (concat
      (remove agg-keys left-cols)
      (for [agg agg-keys]
        {:k agg}))))

(defn ed [relvar]
  #?(:clj ((requiring-resolve 'com.wotbrew.relic.ed/ed) relvar)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library!")))

(defn ed-state [st]
  #?(:clj ((requiring-resolve 'com.wotbrew.relic.ed/set-state) st)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library?!")))

(defn ed-transact [& tx]
  #?(:clj (apply (requiring-resolve 'com.wotbrew.relic.ed/transact) tx)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library?!")))