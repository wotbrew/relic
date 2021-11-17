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
                              (= (index k) row)
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
(defn- left-relvar [relvar] (pop relvar))
(defn- head-stmt [relvar] (peek relvar))

(defmulti dataflow-node
  "Return a map describing the stmt in relation to the left relvar.

  Return keys:

  :empty-index
  An empty implementation of the Index protocol if the statement requires memory for materialization.

  :deps
  A collection of relvars this relvar is dependent on, will normally include 'left' but not always.

  :insert1,, :delete1,
  A function of [db row flow-inserts flow-insert1 flow-deletes flow-delete1], the function should return a new state,
  by calling flow-insert*/flow-delete* functions for new/deleted rows. You do not have to maintain any indexes, the flow functions
  include self-index maintenance, the state should just be threaded through.

  :insert, :delete
  Like :insert1/:delete1 but taking a 'rows' collection instead of a single 'row', otherwise arg order is the same and the same rules apply."
  (fn [left stmt] (operator stmt)))

;; dataflow

(defn table-relvar? [relvar]
  (case (count relvar)
    1 (= :table (operator (head-stmt relvar)))
    false))

(defn unwrap-table [relvar]
  (cond
    (table-relvar? relvar) relvar
    (= :from (operator (head-stmt relvar))) (let [[_ relvar] (head-stmt relvar)] (unwrap-table relvar))
    (= :table (operator (head-stmt relvar))) [(head-stmt relvar)]
    :else nil))

(defn- base-mat-fns
  [g relvar flow]
  (let [[_ table-key :as stmt] (head-stmt relvar)
        {:keys [empty-index]} (if g (g relvar) (dataflow-node [] stmt))
        {:keys [flow-inserts-n
                flow-insert1
                flow-deletes-n
                flow-delete1]} flow]
    {:insert
     (fn base-insert [db rows]
       (let [oidx (db table-key empty-index)
             new-rows (filterv #(not (contains-row? oidx %)) rows)]
         (if (seq new-rows)
           (let [nidx (reduce index-row oidx new-rows)
                 db (assoc db table-key nidx)
                 db (vary-meta db assoc relvar nidx)
                 db (vary-meta db flow-inserts-n new-rows)]
             db)
           db)))
     :insert1
     (fn base-insert1 [db row]
       (let [oidx (db table-key empty-index)]
         (if (contains-row? oidx row)
           db
           (let [nidx (index-row oidx row)
                 db (assoc db table-key nidx)
                 db (vary-meta db assoc relvar nidx)
                 db (vary-meta db flow-insert1 row)]
             db))))
     :delete
     (fn base-delete [db rows]
       (let [oidx (db table-key empty-index)
             deleted-rows (filterv #(contains-row? oidx %) rows)]
         (if (seq deleted-rows)
           (let [nidx (reduce unindex-row oidx deleted-rows)
                 db (assoc db table-key nidx)
                 db (vary-meta db assoc relvar nidx)
                 db (vary-meta db flow-deletes-n deleted-rows)]
             db)
           db)))
     :delete1
     (fn base-delete1 [db row]
       (let [oidx (db table-key empty-index)]
         (if-not (contains-row? oidx row)
           db
           (let [nidx (unindex-row oidx row)
                 db (assoc db table-key nidx)
                 db (vary-meta db assoc relvar nidx)
                 db (vary-meta db flow-delete1 row)]
             db))))}))

(defn- mat-noop
  ([db rows] db)
  ([db rows inserted inserted1 deleted deleted1] db))

(defn- delete-from-graph [g relvar]
  (letfn [(orphan? [g relvar]
            (let [{:keys [dependents]} (g relvar)]
              (empty? dependents)))
          (materialized? [g relvar] (contains? (::materialized g) relvar))
          (watched? [g relvar] (contains? (::watched g) relvar))
          (delete-if-safe [g relvar]
            (cond
              (not (orphan? g relvar)) g
              (watched? g relvar) g
              (materialized? g relvar) g
              (table-relvar? relvar) g
              :else
              (let [{:keys [deps]} (g relvar)
                    g (dissoc g relvar)
                    g (reduce (fn [g dep] (update-in g [dep :dependents] disj relvar)) g deps)]
                (reduce delete-if-safe g deps))))]
    (delete-if-safe g relvar)))

(def Env [[:table ::Env]])
(def ^:dynamic ^:private *env-deps* nil)
(defn- track-env-dep [k] (when *env-deps* (set! *env-deps* (conj *env-deps* k))))

(defn- add-to-graph [g relvar]
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
          (merge-node [a b relvar]
            (cond
              (nil? a) (assoc b :relvar relvar)
              (:mat (meta (head-stmt relvar))) (assoc b :relvar relvar
                                                        :mat true
                                                        :dependents (:dependents a #{}))
              :else a))

          (create-node [relvar]
            (let [left (left-relvar relvar)
                  stmt (head-stmt relvar)

                  [node env-deps]
                  (if (::env-satisfied (meta stmt))
                    [(dataflow-node left stmt)]
                    (binding [*env-deps* #{}]
                      [(dataflow-node left stmt) *env-deps*]))]
              (if (seq env-deps)
                (dataflow-node (conj (or left [])
                                     [:left-join (conj Env [:extend [::env [select-keys ::env env-deps]]])]
                                     (vary-meta stmt assoc ::env-satisfied true))
                               [:project-away ::env])
                node)))

          (add [g relvar]
            (let [{:keys [deps, implicit]
                   :as node} (create-node relvar)
                  contains (contains? g relvar)
                  g (update g relvar merge-node node relvar)]
              (if contains
                g
                (let [g (reduce (fn [g dep] (depend g relvar dep)) g deps)
                      g (reduce add g implicit)]
                  g))))]
    (add g relvar)))

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

        empty-index (or empty-index (when (:mat (meta (head-stmt relvar))) empty-set-index))

        inserted1
        (if empty-index
          (fn inserted1 [db row]
            (let [idx (db relvar empty-index)]
              (if (contains-row? idx row)
                db
                (let [nidx (index-row idx row)
                      db (assoc db relvar nidx)]
                  (flow-insert1 db row)))))
          flow-insert1)
        inserted
        (if empty-index
          (fn insertedn [db rows]
            (let [idx (db relvar empty-index)
                  new-rows (into [] (remove #(contains-row? idx %)) rows)]
              (if (seq new-rows)
                (-> db
                    (assoc relvar (reduce index-row idx new-rows))
                    (flow-inserts-n new-rows))
                db)))
          flow-inserts-n)

        deleted1
        (if empty-index
          (fn deleted1 [db row]
            (let [idx (db relvar empty-index)]
              (if (contains-row? idx row)
                (let [nidx (unindex-row idx row)
                      db (assoc db relvar nidx)]
                  (flow-delete1 db row))
                db)))
          flow-delete1)
        deleted
        (if empty-index
          (fn deleted [db rows]
            (let [idx (db relvar empty-index)
                  del-rows (filterv #(contains-row? idx %) rows)]
              (if (seq del-rows)
                (-> db
                    (assoc relvar (reduce unindex-row idx del-rows))
                    (flow-deletes-n del-rows))
                db)))
          flow-deletes-n)]
    {:insert1
     (cond
       insert1 (fn [db row] (insert1 db row inserted inserted1 deleted deleted1))
       insert (fn [db row] (insert db [row] inserted inserted1 deleted deleted1))
       :else mat-noop)
     :insert
     (cond
       insert (fn [db rows] (insert db rows inserted inserted1 deleted deleted1))
       insert1 (fn [db rows] (reduce #(insert1 %1 %2 inserted inserted1 deleted deleted1) db rows))
       :else mat-noop)
     :delete1
     (cond
       delete1 (fn [db row] (delete1 db row inserted inserted1 deleted deleted1))
       delete (fn [db row] (delete db [row] inserted inserted1 deleted deleted1))
       :else mat-noop)
     :delete
     (cond
       delete (fn [db rows] (delete db rows inserted inserted1 deleted deleted1))
       delete1 (fn [db rows] (reduce #(delete1 %1 %2 inserted inserted1 deleted deleted1) db rows))
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
  (fn [db rows] (record-added relvar rows) (f db rows)))

(defn- with-deleted-hook [relvar f]
  (fn [db rows] (record-deleted relvar rows) (f db rows)))

(defn- with-added-hook1 [relvar f]
  (fn [db row] (record-added1 relvar row) (f db row)))

(defn- with-deleted-hook1 [relvar f]
  (fn [db row] (record-deleted1 relvar row) (f db row)))

(defn add-mat-fns [g relvars]
  (let [og g
        g (reduce add-to-graph og relvars)

        wrap-insertn
        (fn wrap-insertn
          [{:keys [insert insert1]}]
          (fn [db rows]
            (case (count rows)
              0 db
              1 (insert1 db (first rows))
              (insert db rows))))

        wrap-deleten
        (fn wrap-deleten
          [{:keys [delete delete1]}]
          (fn [db rows]
            (case (count rows)
              0 db
              1 (delete1 db (first rows))
              (delete db rows))))

        visited (volatile! #{})

        rf (fn rf [g relvar edge]
             (assert (g relvar) "Should only request inserter for relvars in dataflow graph")
             (if (contains? @visited relvar)
               g
               (let [{:keys [dependents] :as node} (g relvar)
                     g (reduce #(rf %1 %2 relvar) g dependents)
                     flow-mat-fns (mapv #((:mat-fns (g %)) relvar) dependents)

                     flow-inserters-n (mapv :insert flow-mat-fns)
                     flow-deleters-n (mapv :delete flow-mat-fns)

                     flow-inserts-n
                     (case (count flow-inserters-n)
                       0 (fn [db rows] db)
                       1 (first flow-inserters-n)
                       (fn [db rows]
                         (reduce (fn [db f] (f db rows)) db flow-inserters-n)))

                     flow-deletes-n
                     (case (count flow-deleters-n)
                       0 (fn [db _] db)
                       1 (first flow-deleters-n)
                       (fn [db rows]
                         (reduce (fn [db f] (f db rows)) db flow-deleters-n)))

                     flow-inserters-1 (mapv :insert1 flow-mat-fns)

                     flow-insert1
                     (case (count flow-inserters-1)
                       0 (fn [db _] db)
                       1 (first flow-inserters-1)
                       (fn [db row]
                         (reduce (fn [db f] (f db row)) db flow-inserters-1)))

                     flow-deleters-1 (mapv :delete1 flow-mat-fns)

                     flow-delete1
                     (case (count flow-deleters-1)
                       0 (fn [db _] db)
                       1 (first flow-deleters-1)
                       (fn [db row]
                         (reduce (fn [db f] (f db row)) db flow-deleters-1)))

                     flow
                     {:flow-inserts-n (with-added-hook relvar flow-inserts-n)
                      :flow-insert1 (with-added-hook1 relvar flow-insert1)
                      :flow-deletes-n (with-deleted-hook relvar flow-deletes-n)
                      :flow-delete1 (with-deleted-hook1 relvar flow-delete1)}

                     mat-fns (if (nil? edge)
                               (if (table-relvar? relvar)
                                 (base-mat-fns g relvar flow)
                                 (derived-mat-fns node edge flow))
                               (derived-mat-fns node edge flow))
                     mat-fns (assoc mat-fns :insert (wrap-insertn mat-fns) :delete (wrap-deleten mat-fns))]
                 (assoc-in g [relvar :mat-fns edge] mat-fns))))

        bases
        (reduce (fn ! [acc relvar]
                  (let [{:keys [deps]} (g relvar)]
                    (if (empty? deps)
                      (conj acc relvar)
                      (reduce ! acc deps))))
                [] relvars)]
    (reduce #(rf %1 %2 nil) g bases)))

(declare materialize q expr-row-fn)

(defn- dataflow-transactor [g]
  (let [insert-n
        (fn insert-n
          [relvar db rows retry]
          (let [{:keys [insert]} (-> db meta ::graph (get relvar) :mat-fns (get nil))]
            (if insert
              (insert db rows)
              (if retry
                (raise "Could not create mat-fns for table" {:relvar relvar})
                (insert-n relvar (materialize db relvar) rows true)))))
        delete-n
        (fn delete-n
          [relvar db rows retry]
          (let [{:keys [delete]} (-> db meta ::graph (get relvar) :mat-fns (get nil))]
            (if delete
              (delete db rows)
              (if retry
                (raise "Could not create mat-fns for table" {:relvar relvar})
                (delete-n relvar (materialize db relvar) rows true)))))
        update-where
        (fn update-where
          [relvar db f-or-set-map exprs]
          (let [f (if (map? f-or-set-map)
                    (reduce-kv (fn [f k e] (comp f (let [f2 (expr-row-fn e)] #(assoc % k (f2 %))))) identity f-or-set-map)
                    f-or-set-map)
                matched-rows (q db (conj relvar (into [:where] exprs)))
                new-rows (mapv f matched-rows)
                db (delete-n relvar db matched-rows false)
                db (insert-n relvar db new-rows false)]
            db))
        delete-where
        (fn delete-where
          [relvar db exprs]
          (let [rows (q db (conj relvar (into [:where] exprs)))]
            (delete-n relvar db rows false)))]
    (fn transact
      [db tx]
      (cond
        (map? tx) (reduce-kv (fn [db relvar rows] (insert-n relvar db rows false)) db tx)
        (seq? tx) (reduce transact db tx)
        (nil? tx) db
        :else
        (let [[op relvar & args] tx]
          (case op
            :insert (insert-n relvar db args false)
            :delete-exact (delete-n relvar db args false)
            :delete (delete-where relvar db args)
            :update (let [[f-or-set-map & exprs] args] (update-where relvar db f-or-set-map exprs))))))))

(defn- mat-head [relvar]
  (case (count relvar)
    0 (raise "Invalid relvar, no statements" {:relvar relvar})
    (update relvar (dec (count relvar)) vary-meta assoc :mat true)))

(defn- init [m relvar]
  (let [g (::graph m {})
        {:keys [deps
                dependents
                provide]} (g relvar)]
    (let [m (reduce init m deps)
          m (cond
              (seq deps) m
              (contains? m relvar) m
              provide (assoc m relvar (provide m))
              :else m)
          rows (delay (row-seqable (m relvar empty-set-index)))]
      (reduce (fn [m dependent]
                (let [{:keys [mat-fns, mat, provide]} (g dependent)
                      {:keys [insert]} (mat-fns relvar)
                      init-key [::init dependent relvar mat]
                      f (if provide #(insert % (provide %)) #(insert % @rows))]
                  (cond
                    (not mat) (f m)
                    (contains? m init-key) m
                    :else
                    (-> m
                        (assoc init-key true)
                        (f))))) m dependents))))

(declare transact)

(defn- materialize* [db opts relvars]
  (let [m (meta db)
        relvars (map mat-head relvars)
        g (::graph m {})
        g (add-mat-fns g relvars)
        g (if (:ephemeral opts) g (update g ::materialized (fnil into #{}) relvars))
        transactor (dataflow-transactor g)
        m (assoc m ::transactor transactor, ::graph g)
        m (reduce init m relvars)
        db (with-meta db m)]
    db))

(defn- dematerialize* [db opts relvars]
  (let [m (meta db)
        g (::graph m {})
        g (if (:ephemeral opts) g (update g ::materialized (fnil set/difference #{}) (set relvars)))
        g (reduce delete-from-graph g relvars)
        g (add-mat-fns g (::materialized g))
        transactor (dataflow-transactor g)
        m (assoc m ::transactor transactor, ::graph g)
        m (reduce (fn [m relvar]
                    (if (and (contains? m relvar) (not (contains? g relvar)))
                      (dissoc m relvar)
                      m)) m relvars)
        db (with-meta db m)]
    db))

(defn materialize [db & relvars] (materialize* db {} relvars))
(defn dematerialize [db & relvars] (dematerialize* db {} relvars))

(defn transact [db & tx]
  (if-some [transactor (::transactor (meta db))]
    (reduce transactor db tx)
    (apply transact (vary-meta db assoc ::transactor (dataflow-transactor {})) tx)))

(defn- materialized-relation [db relvar]
  (or (get db relvar)
      (get (meta db) relvar)))

(defn index [db relvar]
  (or (materialized-relation db relvar)
      (let [db (materialize db relvar)]
        (get (meta db) relvar))))

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

    (= ::% expr) identity

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
            (= f ::env)
            (let [[k not-found] args]
              (track-env-dep k)
              [(fn [row] (-> row ::env (get k not-found)))])
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

    (map? expr)
    (if (seq expr)
      (apply every-pred (map (fn [[k v]] (let [kf (expr-row-fn k)] #(= v (kf %)))) expr))
      (constantly true))

    :else (constantly expr)))

(defn- add-implicit-expr-joins [relvar exprs]
  (binding [*env-deps* #{}]
    (let [expr-fns (mapv expr-row-fn exprs)]
      (if (seq *env-deps*)
        [(conj relvar [:left-join (conj Env [:extend [:env [select-keys :env *env-deps*]]])]) expr-fns true]
        [relvar expr-fns]))))

(defn q
  ([db relvar-or-binds]
   (if (map? relvar-or-binds)
     (let [relvars (keep (fn [q] (if (map? q) (:q q) q)) (vals relvar-or-binds))
           missing (remove #(materialized-relation db %) relvars)
           db (apply materialize db missing)]
       (reduce-kv
         (fn [m k qr]
           (if (map? q)
             (assoc m k (q db (:q qr) qr))
             (assoc m k (q db qr))))
         {}
         relvar-or-binds))
     (some-> (index db relvar-or-binds) row-seqable)))
  ([db relvar opts]
   (let [{:keys [sort
                 rsort
                 xf]
          into-coll :into} opts

         sort* (or sort rsort)
         sort-exprs (if (keyword? sort*) [sort*] sort*)
         [relvar sort-fns drop-env] (if sort* (add-implicit-expr-joins relvar sort-exprs) [relvar])
         rs (some-> (index db relvar) row-seqable)
         sort-fn (when sort-fns (apply juxt2 sort-fns))

         rs (cond
              sort (sort-by sort-fn rs)
              rsort (sort-by sort-fn > rs)
              :else rs)

         xf (if drop-env
              (comp (map #(dissoc % ::env)) (or xf identity))
              xf)

         rs (cond
             into-coll (if xf (into into-coll xf rs) (into into-coll rs))
             xf (sequence xf rs)
             :else rs)]
     rs)))

(defn what-if [db relvar & tx]
  (q (apply transact db tx) relvar))

(defn watch [db & relvars]
  (let [db (vary-meta db update ::watched (fnil into #{}) relvars)]
    (materialize* db {:ephemeral true} relvars)))

(defn unwatch [db & relvars]
  (let [db (vary-meta db update ::watched (fnil set/difference #{}) (set relvars))]
    (dematerialize* db {:ephemeral true} relvars)))

(defn track-transact [db & tx]
  (binding [*added* (zipmap (::watched (meta db)) (repeat #{}))
            *deleted* (zipmap (::watched (meta db)) (repeat #{}))]
    (let [ost db
          db (apply transact db tx)
          added (for [[relvar added] *added*
                      :let [idx (index db relvar)]]
                  [relvar {:added (filterv #(contains-row? idx %) added)}])
          deleted (for [[relvar deleted] *deleted*
                        :let [oidx (index ost relvar)
                              idx (index db relvar)]]
                    [relvar {:deleted (filterv (every-pred #(not (contains-row? idx %))
                                                           #(contains-row? oidx %)) deleted)}])]
      {:result db
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

(defn- pass-through-insert [db rows inserted inserted1 deleted deleted1] (inserted db rows))
(defn- pass-through-delete [db rows inserted inserted1 deleted deleted1] (deleted db rows))

(defn- pass-through-insert1 [db row inserted inserted1 deleted deleted1] (inserted1 db row))
(defn- pass-through-delete1 [db row inserted inserted1 deleted deleted1] (deleted1 db row))

(defn- transform-insert [f]
  (fn [db rows inserted inserted1 deleted deleted1]
    (inserted db (mapv f rows))))

(defn- transform-delete [f]
  (fn [db rows inserted inserted1 deleted deleted1]
    (deleted db (mapv f rows))))

(defn- transform-insert1 [f]
  (fn [db row inserted inserted1 deleted deleted1]
    (inserted1 db (f row))))

(defn- transform-delete1 [f]
  (fn [db row inserted inserted1 deleted deleted1]
    (deleted1 db (f row))))

(defmulti columns* (fn [_ stmt] (operator stmt)))

(defn columns [relvar]
  (if (empty? relvar)
    []
    (let [stmt (head-stmt relvar)
          left (left-relvar relvar)]
      (columns* left stmt))))

(defn state-deps [relvar]
  (if-some [state (unwrap-table relvar)]
    #{state}
    (let [left (left-relvar relvar)
          stmt (head-stmt relvar)
          {:keys [deps]} (dataflow-node left stmt)]
      (set (mapcat state-deps deps)))))

(defn col-keys [relvar]
  (map :k (columns relvar)))

(defmethod columns* :default [_ _]
  {})

(defmethod dataflow-node :table
  [left [_ _ {:keys [pk]} :as stmt]]
  (if (seq left)
    (dataflow-node left [:from [stmt]])
    {:empty-index (if pk (map-unique-index {} (mapv expr-row-fn pk)) empty-set-index)}))

(defmethod columns* :table
  [_ [_ _ {:keys [req]} :as stmt]]
  (for [k req] {:k k}))

(defmethod dataflow-node :from
  [_ [_ relvar]]
  {:deps [relvar]
   :insert {relvar pass-through-insert}
   :insert1 {relvar pass-through-insert1}
   :delete {relvar pass-through-delete}
   :delete1 {relvar pass-through-delete1}})

(defmethod columns* :from
  [_ [_ relvar]]
  (columns relvar))

(defmethod dataflow-node :project
  [left [_ & cols]]
  (let [cols (vec (set cols))
        f (fn [row] (select-keys row cols))]
    {:deps [left]
     :insert {left (transform-insert f)}
     :insert1 {left (transform-insert1 f)}
     :delete {left (transform-delete f)}
     :delete1 {left (transform-delete1 f)}}))

(defmethod columns* :project
  [left [_ & cols]]
  (filter (comp (set cols) :k) (columns left)))

(defmethod dataflow-node :project-away
  [left [_ & cols]]
  (let [cols (vec (set cols))
        f (fn [row] (apply dissoc row cols))]
    {:deps [left]
     :insert {left (transform-insert f)}
     :insert1 {left (transform-insert1 f)}
     :delete {left (transform-delete f)}
     :delete1 {left (transform-delete1 f)}}))

(defmethod columns* :project-away
  [left [_ & cols]]
  (remove (comp (set cols) :k) (columns left)))

(defmethod dataflow-node :union
  [left [_ right]]
  (let [left (mat-head left)
        right (mat-head right)]
    {:deps [left right]
     :insert {left pass-through-insert
              right pass-through-insert}
     :insert1 {left pass-through-insert1
               right pass-through-insert1}
     :delete {left (fn [db rows inserted inserted1 deleted deleted1]
                     (let [idx2 (db right empty-set-index)
                           del-rows (remove #(contains-row? idx2 %) rows)]
                       (deleted db del-rows)))
              right (fn [db rows inserted inserted1 deleted deleted1]
                      (let [idx2 (db left empty-set-index)
                            del-rows (remove #(contains-row? idx2 %) rows)]
                        (deleted db del-rows)))}
     :delete1 {left (fn [db row inserted inserted1 deleted deleted1]
                      (let [idx2 (db right empty-set-index)]
                        (if (contains-row? idx2 row)
                          db
                          (deleted1 db row))))
               right (fn [db row inserted inserted1 deleted deleted1]
                       (let [idx2 (db left empty-set-index)]
                         (if (contains-row? idx2 row)
                           db
                           (deleted1 db row))))}}))

(defmethod columns* :union
  [left [_ right]]
  (let [left-idx (index-by :k (columns left))
        right-idx (index-by :k (columns right))]
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

(defmethod dataflow-node :intersection
  [left [_ right]]
  (let [left (mat-head left)
        right (mat-head right)]
    {:deps [left right]
     :insert {left (fn [db rows inserted inserted1 deleted deleted1]
                     (let [idx2 (db right empty-set-index)
                           add-rows (filter #(contains-row? idx2 %) rows)]
                       (inserted db add-rows)))
              right (fn [db rows inserted inserted1 deleted deleted1]
                      (let [idx2 (db left empty-set-index)
                            add-rows (filter #(contains-row? idx2 %) rows)]
                        (inserted db add-rows)))}
     :insert1 {left (fn [db row inserted inserted1 deleted deleted1]
                      (let [idx2 (db right empty-set-index)]
                        (if (contains-row? idx2 row)
                          (inserted1 db row)
                          db)))
               right (fn [db row inserted inserted1 deleted deleted1]
                       (let [idx2 (db left empty-set-index)]
                         (if (contains-row? idx2 row)
                           (inserted1 db row)
                           db)))}
     :delete {left pass-through-delete
              right pass-through-delete}
     :delete1 {left pass-through-delete1
               right pass-through-delete1}}))

(defmethod columns* :intersection
  [left [_ _]]
  (columns left))

(defmethod dataflow-node :difference
  [left [_ right]]
  (let [left (mat-head left)
        right (mat-head right)]
    {:deps [left right]
     :insert {left (fn [db rows inserted inserted1 deleted deleted1]
                     (let [idx2 (db right empty-set-index)
                           add-rows (remove #(contains-row? idx2 %) rows)]
                       (inserted db add-rows)))
              right (fn [db rows inserted inserted1 deleted deleted1]
                      (let [idx2 (db left empty-set-index)
                            del-rows (filter #(contains-row? idx2 %) rows)]
                        (deleted db del-rows)))}
     :insert1 {left (fn [db row inserted inserted1 deleted deleted1]
                      (let [idx2 (db right empty-set-index)]
                        (if (contains-row? idx2 row)
                          db
                          (inserted1 db row))))
               right (fn [db row inserted inserted1 deleted deleted1]
                       (let [idx2 (db left empty-set-index)]
                         (if (contains-row? idx2 row)
                           (deleted1 db row)
                           db)))}
     :delete {left (fn [db rows inserted inserted1 deleted deleted1]
                     (let [idx2 (db right empty-set-index)
                           del-rows (remove #(contains-row? idx2 %) rows)]
                       (deleted db del-rows)))
              right (fn [db rows inserted inserted1 deleted deleted1]
                      (let [idx2 (db left empty-set-index)
                            add-rows (filter #(contains-row? idx2 %) rows)]
                        (inserted db add-rows)))}
     :delete1 {left (fn [db row inserted inserted1 deleted deleted1]
                      (let [idx2 (db right empty-set-index)]
                        (if (contains-row? idx2 row)
                          db
                          (deleted1 db row))))
               right (fn [db row inserted inserted1 deleted deleted1]
                       (let [idx2 (db left empty-set-index)]
                         (if (contains-row? idx2 row)
                           (inserted1 db row)
                           db)))}}))

(defmethod columns* :difference
  [left [_ _]]
  (columns left))

(defmethod dataflow-node ::index-lookup
  [_ [_ relvar m]]
  (let [exprs (set (keys m))
        path (map m exprs)
        expr-fns (mapv expr-row-fn exprs)
        hash-index (conj relvar (into [:hash] exprs))
        path-fn (apply juxt expr-fns)]
    {:deps [hash-index]
     :insert {hash-index (fn [db rows inserted inserted1 deleted deleted1]
                           (let [rows (filter #(= path (path-fn %)) rows)]
                             (inserted db rows)))}
     :delete {hash-index (fn [db rows inserted inserted1 deleted deleted1]
                           (let [rows (filter #(= path (path-fn %)) rows)]
                             (deleted db rows)))}
     :provide (fn [db]
                (let [idx (db hash-index)]
                  (when idx
                    (seek-n idx path))))}))

(defmethod dataflow-node :where
  [left [_ & exprs]]
  (let [index-pred (when (map? (first exprs)) (first exprs))
        exprs (if index-pred (rest exprs) exprs)
        expr-preds (mapv expr-row-fn exprs)
        pred-fn (if (seq exprs) (apply every-pred expr-preds) (constantly true))
        left (if index-pred
               [[::index-lookup left index-pred]]
               left)]
    {:deps [left]
     :insert {left (fn [db rows inserted inserted1 deleted deleted1]
                     (inserted db (filterv pred-fn rows)))}
     :insert1 {left (fn [db row inserted inserted1 deleted deleted1]
                      (if (pred-fn row)
                        (inserted1 db row)
                        db))}
     :delete {left (fn [db rows inserted inserted1 deleted deleted1] (deleted db (filterv pred-fn rows)))}
     :delete1 {left (fn [db row inserted inserted1 deleted deleted1] (if (pred-fn row) (deleted1 db row) db))}}))

(defmethod columns* :where
  [left _]
  (columns left))

;; todo reformulate in terms of implicit-joins
(defrecord JoinColl [relvar clause])
(defn join-coll [relvar clause] (->JoinColl relvar clause))
(defn- join-expr? [expr] (instance? JoinColl expr))
(defn- extend-expr [[_ _ expr]] expr)
(defn- join-ext? [extension] (join-expr? (extend-expr extension)))

(defrecord JoinFirst [relvar clause])
(defn join-first [relvar clause] (->JoinFirst relvar clause))
(defn- join-first-expr? [expr] (instance? JoinFirst expr))
(defn- join-first-ext? [extension] (join-first-expr? (extend-expr extension)))

(defmethod dataflow-node ::extend*
  [left [_ & extensions]]
  (let [extend-fns (mapv extend-form-fn extensions)
        f (apply comp (rseq extend-fns))]
    {:deps [left]
     :insert {left (transform-insert f)}
     :insert1 {left (transform-insert1 f)}
     :delete {left (transform-delete f)}
     :delete1 {left (transform-delete1 f)}}))

(defmethod dataflow-node :extend
  [left [_ & extensions]]
  (->> (for [extensions (partition-by (fn [ext] (cond
                                                  (join-ext? ext) :join
                                                  (join-first-ext? ext) :join1
                                                  :else :std)) extensions)]
         (cond
           (join-ext? (first extensions))
           (for [[binding {:keys [relvar clause]}] extensions
                 :let [_ (assert (keyword? binding) "only keyword bindings accepted for join-as-coll")]]
             [::join-as-coll relvar clause binding])

           (join-first-ext? (first extensions))
           (for [[binding {:keys [relvar clause]}] extensions
                 :let [_ (assert (keyword? binding) "only keyword bindings accepted for join-as-coll")]
                 stmt [[::join-as-coll relvar clause binding]
                       (into [::extend*] (for [[binding] extensions] [binding [first binding]]))]]
             stmt)

           :else [(into [::extend*] extensions)]))
       (transduce cat conj left)
       ((fn [relvar]
          (dataflow-node (left-relvar relvar) (head-stmt relvar))))))

(defmethod columns* :extend
  [left [_ & extensions]]
  (let [ext-keys (set (mapcat extend-form-cols extensions))]
    (concat
      (for [col (columns left)
            :when (not (ext-keys (:k col)))]
        col)
      (for [k ext-keys]
        {:k k}))))

(defmethod dataflow-node :qualify
  [left [_ namespace]]
  (let [namespace (name namespace)
        f #(reduce-kv (fn [m k v] (assoc m (keyword namespace (name k)) v)) {} %)]
    {:deps [left]
     :insert {left (transform-insert f)}
     :insert1 {left (transform-insert1 f)}
     :delete {left (transform-delete f)}
     :delete1 {left (transform-delete1 f)}}))

(defmethod columns* :qualify
  [left [_ namespace] _]
  (for [col (columns left)]
    (assoc col :k (keyword namespace (name (:k col))))))

(defmethod dataflow-node :rename
  [left [_ renames]]
  (let [extensions (for [[from to] renames] [to from])
        away (keys renames)
        dep (conj left (into [:extend] extensions) (into [:project-away] away))]
    {:deps [dep]
     :insert {dep pass-through-insert}
     :insert1 {dep pass-through-insert1}
     :delete {dep pass-through-delete}
     :delete1 {dep pass-through-delete1}}))

(defmethod columns* :rename
  [left [_ renames]]
  (for [col (columns left)
        :let [nk (get renames (:k col))]]
    (if nk
      (assoc col :k nk)
      col)))

(defmethod dataflow-node :expand
  [left [_ & expansions]]
  (let [exp-xforms (mapv expand-form-xf expansions)
        exp-xf (apply comp (rseq exp-xforms))]
    {:deps [left]
     :insert {left (fn [db rows inserted inserted1 deleted deleted1] (inserted db (into [] exp-xf rows)))}
     :delete {left (fn [db rows inserted inserted1 deleted deleted1] (deleted db (into [] exp-xf rows)))}}))

(defmethod columns* :expand
  [left [_ & expansions]]
  (let [exp-keys (set (for [[binding] expansions
                            k (if (vector? binding) binding [binding])]
                        k))]
    (concat
      (for [col (columns left)
            :when (not (exp-keys (:k col)))]
        col)
      (for [k exp-keys]
        {:k k}))))

(defmethod dataflow-node :select
  [left [_ & selections]]
  (let [[cols exts] ((juxt filter remove) keyword? selections)
        cols (set (concat cols (mapcat extend-form-cols exts)))
        ext-fns (mapv extend-form-fn exts)
        select-fn (apply comp #(select-keys % cols) (rseq ext-fns))]
    {:deps [left]
     :insert {left (transform-insert select-fn)}
     :insert1 {left (transform-insert1 select-fn)}
     :delete {left (transform-delete select-fn)}
     :delete1 {left (transform-delete1 select-fn)}}))

(defmethod columns* :select
  [left [_ & selections]]
  (let [[cols exts] ((juxt filter remove) keyword? selections)
        cols (set (concat cols (mapcat extend-form-cols exts)))]
    (concat
      (for [col (columns left)
            :when (not (cols (:k col)))]
        col)
      (for [k cols]
        {:k k}))))

(defmethod dataflow-node :hash
  [left [_ & exprs]]
  (let [expr-fns (mapv expr-row-fn exprs)]
    {:empty-index (map-index {} expr-fns)
     :deps [left]
     :insert {left pass-through-insert}
     :insert1 {left pass-through-insert1}
     :delete {left pass-through-delete}
     :delete1 {left pass-through-delete1}}))

(defmethod dataflow-node :unique
  [left [_ & exprs]]
  (let [expr-fns (mapv expr-row-fn exprs)]
    {:empty-index (map-unique-index {} expr-fns)
     :deps [left]
     :insert {left pass-through-insert}
     :insert1 {left pass-through-insert1}
     :delete {left pass-through-delete}
     :delete1 {left pass-through-delete1}}))

(defmethod columns* :hash
  [left _]
  (columns left))

(defmethod dataflow-node :btree
  [left [_ & exprs]]
  (let [expr-fns (mapv expr-row-fn exprs)]
    {:empty-index (map-index (sorted-map) expr-fns)
     :deps [left]
     :insert {left pass-through-insert}
     :insert1 {left pass-through-insert1}
     :delete {left pass-through-delete}
     :delete1 {left pass-through-delete1}}))

(defmethod columns* :btree
  [left _]
  (columns left))

(defmethod dataflow-node :join
  [left [_ right clause]]
  (let [left-exprs (set (keys clause))
        left (conj left (into [:hash] left-exprs))
        right-path-fn (apply juxt2 (map expr-row-fn left-exprs))

        right-exprs (map clause left-exprs)
        right (conj right (into [:hash] right-exprs))
        left-path-fn (apply juxt2 (map expr-row-fn right-exprs))
        join-row merge]
    {:deps [left right]
     :insert {left (fn [db rows inserted inserted1 deleted deleted1]
                     (let [idx (db right)
                           matches (for [row rows
                                         :let [path (right-path-fn row)]
                                         match (seek-n idx path)]
                                     (join-row row match))]
                       (inserted db matches)))
              right (fn [db rows inserted inserted1 deleted deleted1]
                      (let [idx (db left)
                            matches (for [row rows
                                          :let [path (left-path-fn row)]
                                          match (seek-n idx path)]
                                      (join-row match row))]
                        (inserted db matches)))}
     :delete {left (fn [db rows inserted inserted1 deleted deleted1]
                     (let [idx (db right)
                           matches (for [row rows
                                         :let [path (right-path-fn row)]
                                         match (seek-n idx path)]
                                     (join-row row match))]
                       (deleted db matches)))
              right (fn [db rows inserted inserted1 deleted deleted1]
                      (let [idx (db left)
                            matches (for [row rows
                                          :let [path (left-path-fn row)]
                                          match (seek-n idx path)]
                                      (join-row match row))]
                        (deleted db matches)))}}))

(defmethod columns* :join
  [left [_ right]]
  (let [left-cols (columns left)
        right-cols (columns right)
        right-idx (reduce #(assoc %1 (:k %2) %2) {} right-cols)]
    (concat
      (remove (comp right-idx :k) left-cols)
      ;; todo switch :opt if could-be-right
      right-cols)))


(defmethod dataflow-node ::join-as-coll
  [left [_ right clause k]]
  (let [left-exprs (set (keys clause))
        right-path-fn (apply juxt2 (map expr-row-fn left-exprs))

        right-exprs (map clause left-exprs)
        right (conj right (into [:hash] right-exprs))
        left-path-fn (apply juxt2 (map expr-row-fn right-exprs))
        join-matches (fn [m rows] (assoc m k (set rows)))
        mem-key [[::join-as-coll-memory left right clause k]]]
    {:deps [left right]
     :insert {left (fn [db rows inserted inserted1 deleted deleted1]
                     (let [idx (db right)
                           mem (db mem-key {})
                           matches (for [row rows
                                         :let [path (right-path-fn row)
                                               matches (seek-n idx path)]]
                                     (join-matches row matches))
                           mem2 (reduce (fn [mem row] (update mem (right-path-fn row) set-conj row)) mem matches)]
                       (-> db
                           (assoc mem-key mem2)
                           (inserted matches))))
              right (fn [db rows inserted inserted1 deleted deleted1]
                      (let [right-idx (db right)
                            mem (db mem-key {})
                            old-rows (into #{} (comp (map left-path-fn) (mapcat mem)) rows)
                            deletes old-rows
                            inserts (vec (for [row old-rows
                                               :let [path (right-path-fn row)
                                                     matches (seek-n right-idx path)]]
                                           (join-matches row matches)))
                            mem2 (reduce (fn [mem row] (disjoc mem (right-path-fn row) row)) mem old-rows)
                            mem2 (reduce (fn [mem row] (update mem (right-path-fn row) set-conj row)) mem2 inserts)]
                        (-> db
                            (assoc mem-key mem2)
                            (deleted deletes)
                            (inserted inserts))))}
     :delete {left (fn [db rows inserted inserted1 deleted deleted1]
                     (let [idx (db right)
                           mem (db mem-key {})
                           matches (for [row rows
                                         :let [path (right-path-fn row)
                                               matches (seek-n idx path)]]
                                     (join-matches row matches))
                           mem2 (reduce (fn [mem row] (disjoc mem (right-path-fn row) row)) mem matches)]
                       (-> db
                           (assoc mem-key mem2)
                           (deleted matches))))
              right (fn [db rows inserted inserted1 deleted deleted1]
                      (let [right-idx (db right)
                            mem (db mem-key {})
                            old-rows (into #{} (comp (map left-path-fn) (mapcat mem)) rows)
                            deletes old-rows
                            inserts (vec (for [row old-rows
                                               :let [path (right-path-fn row)
                                                     matches (seek-n right-idx path)]]
                                           (join-matches row matches)))
                            mem2 (reduce (fn [mem row] (disjoc mem (right-path-fn row) row)) mem old-rows)
                            mem2 (reduce (fn [mem row] (update mem (right-path-fn row) set-conj row)) mem2 inserts)]
                        (-> db
                            (assoc mem-key mem2)
                            (deleted deletes)
                            (inserted inserts))))}}))

(defmethod dataflow-node :left-join
  [left [_ right clause]]
  (let [join-as-coll (conj left [::join-as-coll right clause ::left-join])
        join-row merge
        xf (fn [rf]
             (fn
               ([acc] (rf acc))
               ([acc row]
                (let [rrows (::left-join row)
                      row (dissoc row ::left-join)]
                  (if (empty? rrows)
                    (rf acc row)
                    (reduce (fn [acc rrow] (rf acc (join-row row rrow))) acc rrows))))))]
    {:deps [join-as-coll]
     :insert {join-as-coll (fn [db rows inserted inserted1 deleted deleted1]
                             (->> (into [] xf rows)
                                  (inserted db)))}
     :delete {join-as-coll (fn [db rows inserted inserted1 deleted deleted1]
                             (->> (into [] xf rows)
                                  (deleted db)))}}))

(defmethod columns* :left-join
  [left [_ right clause]]
  [left [_ right]]
  (let [left-cols (columns left)
        right-cols (columns right)
        right-idx (reduce #(assoc %1 (:k %2) %2) {} right-cols)]
    (concat
      (remove (comp right-idx :k) left-cols)
      ;; todo flag optionality
      right-cols)))

(defn- row-count-node [left [_ cols [binding]]]
  (let [left (mat-head left)
        key-fn #(select-keys % cols)
        idx-key (conj left (into [::row-count-index cols]))]
    {:deps [left]
     :insert {left (fn [db rows inserted inserted1 deleted deleted1]
                     (let [idx (db idx-key {})
                           ks (vec (set (map key-fn rows)))
                           old-counts (mapv (comp count idx) ks)
                           nidx (reduce (fn [nidx row] (update nidx (key-fn row) set-conj row)) idx rows)
                           db (assoc db idx-key nidx)
                           new-counts (mapv (comp count nidx) ks)

                           del-rows
                           (loop [acc []
                                  i 0]
                             (if (< i (count old-counts))
                               (let [n1 (nth old-counts i)
                                     n2 (nth new-counts i)]
                                 (if (not= n1 n2)
                                   (if (= 0 n1)
                                     (recur acc (inc i))
                                     (recur (conj acc (assoc (nth ks i) binding n1)) (inc i)))
                                   (recur acc (inc i))))
                               acc))
                           add-rows
                           (loop [acc []
                                  i 0]
                             (if (< i (count old-counts))
                               (let [n1 (nth old-counts i)
                                     n2 (nth new-counts i)]
                                 (if (not= n1 n2)
                                   (recur (conj acc (assoc (nth ks i) binding n2)) (inc i))
                                   (recur acc (inc i))))
                               acc))]

                       (-> db
                           (deleted del-rows)
                           (inserted add-rows))))}
     :insert1 {left (fn [db row inserted inserted1 deleted deleted1]
                      (let [idx (db idx-key {})
                            k (key-fn row)
                            s (idx k #{})
                            oc (count s)
                            ns (conj s row)
                            nc (count ns)]
                        (if (= nc oc)
                          db
                          (-> db
                              (assoc idx-key (assoc idx k ns))
                              (cond-> (not= 0 oc) (deleted1 (assoc k binding oc)))
                              (inserted1 (assoc k binding nc))))))}
     :delete {left (fn [db rows inserted inserted1 deleted deleted1]
                     (let [idx (db idx-key {})
                           ks (vec (set (map key-fn rows)))
                           old-counts (mapv (comp count idx) ks)
                           nidx (reduce (fn [nidx row] (disjoc nidx (key-fn row) row)) idx rows)
                           db (assoc db idx-key nidx)
                           new-counts (mapv (comp count nidx) ks)

                           del-rows
                           (loop [acc []
                                  i 0]
                             (if (< i (count old-counts))
                               (let [n1 (nth old-counts i)
                                     n2 (nth new-counts i)]
                                 (if (not= n1 n2)
                                   (conj acc (assoc (nth ks i) binding n1))
                                   (recur acc (inc i))))
                               acc))
                           add-rows
                           (loop [acc []
                                  i 0]
                             (if (< i (count old-counts))
                               (let [n1 (nth old-counts i)
                                     n2 (nth new-counts i)]
                                 (if (not= n1 n2)
                                   (if (= 0 n2)
                                     (recur acc (inc i))
                                     (recur (conj acc (assoc (nth ks i) binding n2)) (inc i)))
                                   (recur acc (inc i))))
                               acc))]

                       (-> db
                           (deleted del-rows)
                           (inserted add-rows))))}
     :delete1 {left (fn [db row inserted inserted1 deleted deleted1]
                      (let [idx (db idx-key {})
                            k (key-fn row)
                            s (idx k #{})
                            oc (count s)
                            ns (conj s row)
                            nc (count ns)]
                        (cond
                          (= nc oc) db
                          (= 0 nc) (-> db
                                       (assoc idx-key (dissoc idx k))
                                       (cond-> (not= 0 oc) (deleted1 (assoc k binding oc))))
                          :else
                          (-> db
                              (assoc idx-key (assoc idx k ns))
                              (cond-> (not= 0 oc) (deleted1 (assoc k binding oc)))
                              (inserted1 (assoc k binding nc))))))}}))

(defn row-count []
  {:combiner +
   :reducer count
   :custom-node row-count-node})

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

(defn any [expr]
  (let [f (expr-row-fn expr)]
    {:reducer #(some f %)
     :combiner #(and %1 %2)
     :complete boolean}))

(defn not-any [expr]
  (let [f (expr-row-fn expr)]
    {:reducer #(not-any? f %)
     :combiner #(and %1 %2)
     :complete boolean}))

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
  (let [[binding expr] form
        {:keys [combiner reducer complete]} (agg-expr-agg expr)]
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

(defn- special-agg [left stmt [_ expr]]
  (when-some [n (:custom-node (agg-expr-agg expr))]
    (n left stmt)))

(defmethod dataflow-node :agg
  [left [_ cols & aggs :as stmt]]
  (or
    (when (= 1 (count aggs)) (special-agg left stmt (first aggs)))
    (let [cols (vec (set cols))
          key-fn #(select-keys % cols)
          {:keys [reducer combiner complete merger]} (agg-fns aggs)
          complete (or complete identity)
          merge-fn (or merger merge)
          empty-idx (agg-index key-fn reducer combiner complete merge-fn 32)
          idx-key [::agg-index left cols aggs]]
      {:deps [left]
       :insert {left (fn [db rows insert insert1 delete delete1]
                       (let [idx (db idx-key empty-idx)
                             ks (set (map key-fn rows))
                             old-rows (keep (comp maybe-deref :indexed-row idx) ks)
                             nidx (reduce index-row idx rows)
                             new-rows (keep (comp maybe-deref :indexed-row nidx) ks)
                             db (assoc db idx-key nidx)]
                         (-> db
                             (delete old-rows)
                             (insert new-rows))))}
       :delete {left (fn [db rows insert insert1 delete delete1]
                       (let [idx (db idx-key empty-idx)
                             ks (set (map key-fn rows))
                             old-rows (keep (comp maybe-deref :indexed-row idx) ks)
                             nidx (reduce unindex-row idx rows)
                             new-rows (keep (comp maybe-deref :indexed-row nidx) ks)
                             db (assoc db idx-key nidx)]
                         (-> db
                             (delete old-rows)
                             (insert new-rows))))}})))

(defmethod columns* :agg
  [left [_ cols & aggs]]
  (let [agg-keys (set (for [[binding] aggs] binding))
        left-cols (filter (comp (set cols) :k) (columns left))]
    (concat
      (remove agg-keys left-cols)
      (for [agg agg-keys]
        {:k agg}))))

(defn ed [relvar]
  #?(:clj ((requiring-resolve 'com.wotbrew.relic.ed/ed) relvar)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library!")))

(defn ed-set [db]
  #?(:clj ((requiring-resolve 'com.wotbrew.relic.ed/set-state) db)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library?!")))

(defn ed-transact [& tx]
  #?(:clj (apply (requiring-resolve 'com.wotbrew.relic.ed/transact) tx)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library?!")))

(defn- bad-fk [left relvar clause row]
  (raise "Foreign key violation" {:relvar left
                                  :references relvar
                                  :clause clause
                                  :row (dissoc row ::fk)}))

(defn- good-fk [left relvar clause row])

(defmethod dataflow-node ::fk
  [left [_ relvar clause]]
  (let [dep (conj left [::join-as-coll relvar clause ::fk])
        check-fk (fn [{::keys [fk] :as row}]
                   (if (empty? fk)
                     (bad-fk left relvar clause row)
                     (good-fk left relvar clause row)))]
    {:deps [dep]
     :insert1 {dep (fn [db row insert insert1 delete delete1]
                     (check-fk row)
                     db)}
     :delete1 {dep mat-noop}}))

(defn- check->pred [relvar check]
  (if (map? check)
    (let [{:keys [pred, error]} check
          pred-fn (expr-row-fn pred)
          error-fn (expr-row-fn (or error "Check constraint violation"))]
      (fn [m]
        (let [r (pred-fn m)]
          (if r
            m
            (raise (error-fn m) {:expr pred, :check check, :row m, :relvar relvar}))))))
  (let [f2 (expr-row-fn check)]
    (fn [m]
      (let [r (f2 m)]
        (if r
          m
          (raise "Check constraint violation" {:expr check, :check check, :row m, :relvar relvar}))))))

(defmethod dataflow-node ::check
  [left [_ & checks]]
  (let [f (apply comp (map (partial check->pred left) checks))]
    {:deps [left]
     :insert1 {left (transform-insert1 f)}
     :delete1 {left (transform-delete1 f)}}))

;; spec / malli checks can require extension of this multi
(defmulti constraint->relvars (fn [k constraint] k))

(defmethod constraint->relvars :default [_ _] nil)

(defmethod constraint->relvars :unique [_ {:keys [relvar unique]}]
  (for [k unique]
    (if (keyword? k)
      (conj relvar [:unique k])
      (conj relvar (into [:unique] k)))))

(defmethod constraint->relvars :fk [_ {:keys [relvar fk]}]
  (for [[right clause] fk]
    (conj relvar [::fk right clause])))

(defmethod constraint->relvars :check [_ {:keys [relvar check]}]
  [(conj relvar (into [::check] check))])

(defn constrain
  "Applies constraints to the state.

  constraints are maps (or collections of maps) with the following keys:

  :relvar The relvar that constraints apply to, any kind of relvar is acceptable.

  :unique a vector of unique expressions / sets of expressions, e.g :unique [[:a :b]] would create a composite unique key across :a :b.

  :fk a map of {relvar join-clause} to establish relationships to other relvars, e.g it holds that a :join is always possible across the fk

  :check
  A vector of checks, each check is a relic expr, or a map of

    :pred (relic expr) e.g [< 0 :age 130]
    :error (relic expr returning a string message) e.g [str \"not a valid age: \" :age] and \"not a valid age\" would be acceptable."
  [db & constraints]
  (let [get-relvars
        (fn cr [constraint]
          (if-not (map? constraint)
            (mapcat cr constraint)
            (concat
              (for [k (keys constraint)
                    relvar (constraint->relvars k constraint)]
                relvar))))]
    (->> (get-relvars constraints)
         (apply materialize db))))

(defmethod dataflow-node :const
  [left [_ relation]]
  (let [relation (into empty-set-index relation)]
    {:provide (fn [db] relation)}))

(defn get-env [db] (first (q db Env)))
(defn with-env [db env] (transact db [:delete Env] [:insert Env {::env env}]))
(defn update-env [db f & args] (with-env db (apply f (get-env db) args)))