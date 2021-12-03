(ns ^{:author "Dan Stone <wotbrew@gmail.com>"}
  com.wotbrew.relic
  "Functional relational programming for clojure.

  Quick hints:

  a relic database is a map.

  - put data in with `transact`, see also `what-if`.
  - get data out with `q`, see also `index`.
  - go faster maybe with `materialize`, see also `dematerialize`
  - track changes with `track-transact`, `watch` and `unwatch`.

  I like to alias :as rel. @wotbrew"
  (:require [clojure.set :as set]))

;; protocols

(defprotocol Index
  "Index protocol for all structures that hold rows, implementation detail - do not change."
  :extend-via-metadata true
  (index-row [index row])
  (unindex-row [index row])
  (contains-row? [index row])
  (row-seqable [index]))

(defprotocol SeekN
  "Index extension to seek rows using some (partial) path vector, e.g {:foo {:bar [row1, row2...]}} can be seeked using paths [:foo] or [:foo :bar].

  Implementation detail, not considered public."
  :extend-via-metadata true
  (seek-n [index ks]))

(extend-protocol SeekN
  nil
  (seek-n [_ _] nil))

;; utilities

(defn- index-by
  "Returns (k elem) to elem for each element in coll. Duplicate elements are dropped."
  [k coll]
  (reduce #(assoc % (k %2) %2) {} coll))

(defn- juxt2
  "Version of juxt that accepts 0 fns (result fn will yield the empty vector)."
  [& args]
  (if (empty? args) (constantly []) (apply juxt args)))

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

(def ^:private set-conj (fnil conj #{}))

(defn- raise
  ([msg] (throw (ex-info msg {})))
  ([msg map] (throw (ex-info msg map)))
  ([msg map cause] (throw (ex-info msg map cause))))

;; indexes

(def ^:private empty-set-index
  (->> {`index-row conj
        `unindex-row disj
        `contains-row? contains?
        `row-seqable identity
        `seek-n (fn map-seekn [index ks] (if (seq ks) #{} index))}
       (with-meta #{})))

(defn- map-index1
  [empty keyfn]
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

(defn- update-in2
  ([m empty ks f & args]
   (let [up (fn up [m ks f args]
              (let [[k & ks] ks]
                (if ks
                  (assoc m k (up (get m k empty) ks f args))
                  (assoc m k (apply f (get m k) args)))))]
     (up m ks f args))))

(defn- map-index
  [empty keyfns]
  (case (count keyfns)
    0 empty-set-index
    1 (map-index1 empty (first keyfns))
    (let [path (apply juxt2 keyfns)
          depth (count keyfns)
          map-contains-row? (fn map-contains-row? [index row] (contains? (get-in index (path row) #{}) row))]
      (with-meta
        empty
        {`index-row (fn [index row] (update-in2 index empty (path row) set-conj row))
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

(defn- map-unique-index1 [empty keyfn collision-fn]
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

(defn- map-unique-index
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
         {`index-row (fn [index row] (update-in2 index empty (path row) replace row))
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

;; worth the custom type I think, might specialise further for java vs js
(defrecord AggIndexNode [size rm cache a b reduced combined results])

(defn- agg-index
  "The general aggregation index using a branching tree and combine-reduce. Hopefully will be a last resort
  as most aggs should have specialised indexes."
  [key-fn reduce-fn combine-fn complete-fn merge-fn cache-size]
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
                      a (if grow-a (push-cache a cache (:reduced tree)) a)
                      b (if grow-a b (push-cache b cache (:reduced tree)))
                      rm (persistent! (reduce-kv (fn [m k _] (assoc! m k branch-id)) (transient (or rm {})) cache))
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
                          rm (persistent! (reduce-kv (fn [m k _] (assoc! m k branch-id)) (transient (or rm {})) cache))

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
                    (let [branch (add branch row)
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
(defn- left-relvar [relvar] (when (peek relvar) (pop relvar)))
(defn- head-stmt [relvar] (peek relvar))

(defn table-relvar?
  "True if the relvar is a table."
  [relvar]
  (case (count relvar)
    1 (= :table (operator (head-stmt relvar)))
    false))

(defn- table*-relvar?
  [relvar]
  (case (count relvar)
    1 (= ::table* (operator (head-stmt relvar)))
    false))

(defn- unwrap-from [relvar]
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

;; --
;; Env tracking
;; ::rel/env forms get special casing to introduce implicit joins, as otherwise programming
;; with things like time would be a real pain in the bum.

(def Env [[:table ::Env]])
(def ^:dynamic ^:private *env-deps* nil)
(defn- track-env-dep [k]
  (when *env-deps* (set! *env-deps* (conj *env-deps* k))))

;; --
;; dataflow

(defmulti dataflow-node
  "Return a map describing how to flow data through this relational node.

  ADVANCED, I wouldn't recommend extending this. Better think of it as an implementation detail.

  Return keys:

  :empty-index
  An empty implementation of the Index protocol if the statement requires memory for materialization.

  :deps
  A collection of relvars this relvar is dependent on, will normally include 'left' but not always.

  :extras
  A collection of keys that should be removed with this node when dematerializing.

  :insert1,, :delete1,
  A function of [db row flow-inserts flow-insert1 flow-deletes flow-delete1], the function should return a new state,
  by calling flow-insert*/flow-delete* functions for new/deleted rows. You do not have to maintain any indexes, the flow functions
  include self-index maintenance, the state should just be threaded through.

  :insert, :delete
  Like :insert1/:delete1 but taking a 'rows' collection instead of a single 'row', otherwise arg order is the same and the same rules apply.

  :provide "
  (fn [_left stmt] (operator stmt)))

(defn- mat-noop
  ([db _rows] db)
  ([db _rows _inserted _inserted1 _deleted _deleted1] db))

;; --
;; dataflow graph functions
;; stored under ::graph key in metadata

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
              :else
              (let [{:keys [deps]} (g relvar)
                    g (dissoc g relvar)
                    g (reduce (fn [g dep] (update-in g [dep :dependents] disj relvar)) g deps)
                    g (if (table*-relvar? relvar)
                        (let [[_ table-key] (head-stmt relvar)]
                          (dissoc g table-key))
                        g)]
                (reduce delete-if-safe g deps))))]
    (delete-if-safe g relvar)))

(defn- mat? [relvar]
  (:mat (meta (head-stmt relvar)) false))

(defn- add-to-graph [g relvar]
  (letfn [(flag-dirty [g relvar dependent]
            (let [node (g relvar)
                  node (assoc node :dirty (conj (:dirty node #{}) dependent))
                  g (assoc g relvar node)]
              (cond
                (:empty-index node) g
                (:mat node) g
                :else (reduce #(flag-dirty %1 %2 relvar) g (:deps node)))))

          (depend [g dependent dependency]
            (let [g (add g dependency)
                  n (g dependency)
                  n2 (update n :dependents set-conj dependent)]
              (-> (assoc g dependency n2)
                  (flag-dirty dependency dependent))))

          (create-node [relvar]
            (let [left (left-relvar relvar)
                  stmt (head-stmt relvar)

                  [node env-deps]
                  (if (::env-satisfied (meta stmt))
                    [(dataflow-node left stmt)]
                    (binding [*env-deps* #{}]
                      (let [node (dataflow-node left stmt)]
                        [node *env-deps*])))]
              (if (seq env-deps)
                (dataflow-node (conj (or left [])
                                     [:left-join (conj Env [:extend [::env [select-keys ::env env-deps]]])]
                                     (vary-meta stmt assoc ::env-satisfied true))
                               [:without ::env])
                node)))

          (add [g relvar]
            (if (empty? relvar)
              g
              (let [{:keys [deps]
                     :as node} (create-node relvar)
                    new-node (assoc node :mat (mat? relvar), :relvar relvar)
                    old-node (g relvar)
                    now-mat (when old-node (not= (:mat old-node false) (:mat new-node)))]
                (if (or (nil? old-node) now-mat)
                  (let [g (assoc g relvar (if old-node
                                            (assoc old-node :mat true)
                                            new-node))
                        g (reduce (fn [g dep] (depend g relvar dep)) g deps)
                        g (if (table*-relvar? relvar)
                            (let [[_ table-key] (head-stmt relvar)]
                              (assoc g table-key {}))
                            g)]
                    g)
                  g))))]
    (add g relvar)))

;; --
;; state tracking vars
;; for e.g reactive UI's or hooking into other signal graphs or callback systems
;; we want a way to watch for change without comparing databases or relations.

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

;; --
;; materialized function dynamic composition
;; constructs function chains that realise dataflow graphs
;; function changes are stored against graph nodes


(def ^:dynamic *trace* false)

(defn- trace
  [& msg]
  (when *trace*
    (apply println msg)))

(defn- sys-time []
  #?(:clj (System/nanoTime)
     :cljs (.now (.-performance js/window))))

(defn- timing-str [sys-time]
  #?(:clj (format "%.2fms" (* 1e-6 sys-time))
     :cljs (str (.toFixed sys-time 2) "ms")))

(defn- traced-flow [k edge relvar f]
  (fn traced-flow
    ([db rows]
     (if *trace*
       (do (println (str (name k) ":") relvar)
           (println "  from" edge)
           (let [enter-time (sys-time)
                 ret (f db rows)
                 exit-time (sys-time)
                 elapsed-time (- exit-time enter-time)]
             (println "  took" (timing-str elapsed-time))
             ret))
       (f db rows)))
    ([db rows inserted inserted1 deleted deleted1]
     (if *trace*
       (do (println (str (name k) ":") relvar)
           (println "  from" edge)
           (let [enter-time (sys-time)
                 ret (f db rows inserted inserted1 deleted deleted1)
                 exit-time (sys-time)
                 elapsed-time (- exit-time enter-time)]
             (println "  took" (timing-str elapsed-time))
             ret))
       (f db rows inserted inserted1 deleted deleted1)))))

(defn- derived-mat-fns [node edge flow]
  (let [{:keys [flow-inserts-n
                flow-insert1
                flow-deletes-n
                flow-delete1]} flow
        {:keys [empty-index
                mat
                relvar
                insert
                insert1
                delete
                delete1]} node

        insert (get insert edge)
        insert1 (get insert1 edge)
        delete (get delete edge)
        delete1 (get delete1 edge)

        empty-index (or empty-index (when mat empty-set-index))

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
        (cond
          (set? empty-index)
          (fn insertedn-set [db rows]
            (let [idx (db relvar empty-index)
                  new-rows (into [] (remove idx) rows)]
              (if (seq new-rows)
                (-> db
                    (assoc relvar (into idx new-rows))
                    (flow-inserts-n new-rows))
                db)))

          empty-index
          (fn insertedn [db rows]
            (let [idx (db relvar empty-index)
                  new-rows (into [] (remove #(contains-row? idx %)) rows)]
              (if (seq new-rows)
                (-> db
                    (assoc relvar (reduce index-row idx new-rows))
                    (flow-inserts-n new-rows))
                db)))
          :else
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
          flow-deletes-n)

        tr (fn [k f] (traced-flow k edge relvar f))]
    {:insert1
     (tr :insert1
         (cond
           insert1 (fn [db row] (insert1 db row inserted inserted1 deleted deleted1))
           insert (fn [db row] (insert db [row] inserted inserted1 deleted deleted1))
           :else mat-noop))
     :insert
     (tr :insert
         (cond
           insert (fn [db rows] (insert db rows inserted inserted1 deleted deleted1))
           insert1 (fn [db rows] (reduce #(insert1 %1 %2 inserted inserted1 deleted deleted1) db rows))
           :else mat-noop))
     :delete1
     (tr :delete1
         (cond
           delete1 (fn [db row] (delete1 db row inserted inserted1 deleted deleted1))
           delete (fn [db row] (delete db [row] inserted inserted1 deleted deleted1))
           :else mat-noop))
     :delete
     (tr :delete
         (cond
           delete (fn [db rows] (delete db rows inserted inserted1 deleted deleted1))
           delete1 (fn [db rows] (reduce #(delete1 %1 %2 inserted inserted1 deleted deleted1) db rows))
           :else mat-noop))

     :inserted flow-inserts-n
     :deleted flow-deletes-n
     :inserted1 flow-insert1
     :deleted1 flow-delete1}))

(defn- dependent-sort [relvar]
  (case (operator (head-stmt relvar))
    :hash -1
    :btree 0
    (+ 1 (count relvar))))

(defn- get-flow [g relvar dependents]
  (let [flow-mat-fns (mapv #((:mat-fns (g %)) relvar) dependents)

        flow-inserters-n (mapv :insert flow-mat-fns)
        flow-deleters-n (mapv :delete flow-mat-fns)

        flow-inserts-n
        (case (count flow-inserters-n)
          0 (fn [db _] db)
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
          (fn [db row] (flow-inserts-n db [row])))

        flow-deleters-1 (mapv :delete1 flow-mat-fns)

        flow-delete1
        (case (count flow-deleters-1)
          0 (fn [db _] db)
          1 (first flow-deleters-1)
          (fn [db row] (flow-deletes-n db [row])))]
    {:flow-inserts-n (with-added-hook relvar flow-inserts-n)
     :flow-insert1 (with-added-hook1 relvar flow-insert1)
     :flow-deletes-n (with-deleted-hook relvar flow-deletes-n)
     :flow-delete1 (with-deleted-hook1 relvar flow-delete1)}))

(defn- table-mat-fns
  [g table-key flow]
  (let [{:keys [flow-inserts-n
                flow-insert1
                flow-deletes-n
                flow-delete1]} flow
        empty-idx empty-set-index]
    {:insert
     (fn base-insert [db rows]
       (let [idx (db table-key empty-idx)
             added-rows (into [] (remove idx) rows)]
         (if (seq added-rows)
           (let [db* (::state (meta db) {})
                 nidx (into idx rows)
                 db (assoc db table-key nidx)
                 db* (assoc db* table-key nidx)
                 db* (flow-inserts-n db* added-rows)
                 db (vary-meta db assoc ::state db*)]
             db)
           db)))
     :insert1
     (fn base-insert1 [db row]
       (let [idx (db table-key empty-idx)]
         (if-not (contains? idx row)
           (let [db* (::state (meta db) {})
                 nidx (conj idx row)
                 db (assoc db table-key nidx)
                 db* (assoc db* table-key nidx)
                 db* (flow-insert1 db* row)
                 db (vary-meta db assoc ::state db*)]
             db)
           db)))
     :delete
     (fn base-delete [db rows]
       (let [idx (db table-key empty-idx)
             deleted-rows (filterv idx rows)]
         (if (seq deleted-rows)
           (let [db* (::state (meta db) {})
                 nidx (reduce disj idx rows)
                 db (assoc db table-key nidx)
                 db* (assoc db* table-key nidx)
                 db* (flow-deletes-n db* deleted-rows)
                 db (vary-meta db assoc ::state db*)]
             db)
           db)))
     :delete1
     (fn base-delete1 [db row]
       (let [idx (db table-key empty-idx)]
         (if (contains? idx row)
           (let [db* (::state (meta db) {})
                 nidx (disj idx row)
                 db (assoc db table-key nidx)
                 db* (assoc db* table-key nidx)
                 db* (flow-delete1 db* row)
                 db (vary-meta db assoc ::state db*)]
             db)
           db)))}))

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
             (if (contains? @visited relvar)
               g
               (let [{:keys [dependents] :as node} (g relvar)
                     dependents (sort-by dependent-sort dependents)
                     g (reduce #(rf %1 %2 relvar) g dependents)
                     flow (get-flow g relvar dependents)
                     mat-fns (derived-mat-fns node edge flow)
                     mat-fns (assoc mat-fns :insert (wrap-insertn mat-fns) :delete (wrap-deleten mat-fns))
                     g (assoc-in g [relvar :mat-fns edge] mat-fns)
                     g (if (table*-relvar? relvar)
                         (let [[_ table-key] (head-stmt relvar)]
                           (assoc-in g [table-key :mat-fns] (table-mat-fns g table-key (get-flow g nil [relvar]))))
                         g)]
                 g)))

        bases
        (reduce (fn ! [acc relvar]
                  (let [{:keys [deps]} (g relvar)]
                    (if (empty? deps)
                      (conj acc relvar)
                      (reduce ! acc deps))))
                [] relvars)]
    (reduce #(rf %1 %2 nil) g bases)))

(declare materialize q expr-row-fn)

;; --
;; create a transactor from a dataflow graph
;; stored under ::transactor in metadata
;; right now doesn't use the graph, and references mat function changes on demand
;; but knowing the graph might be useful to optimise the transactor in the future

(def ^:private ^:dynamic *upsert-collisions* nil)

(defn- dataflow-transactor [_g]
  (let [as-table-key (fn [relvar] (if (keyword? relvar) relvar (let [[_ table-key] (head-stmt relvar)] table-key)))
        as-relvar (fn [relvar] (if (keyword? relvar) [[:table relvar]] relvar))
        insert-n
        (fn insert-n
          [relvar db rows retry]
          (let [{:keys [insert]} (-> db meta ::graph (get (as-table-key relvar)) :mat-fns)]
            (if insert
              (insert db rows)
              (if retry
                (raise "Could not create mat-fns for table" {:relvar (as-relvar relvar)})
                (insert-n relvar (materialize db (as-relvar relvar)) rows true)))))
        delete-n
        (fn delete-n
          [relvar db rows retry]
          (let [{:keys [delete]} (-> db meta ::graph (get (as-table-key relvar)) :mat-fns)]
            (if delete
              (delete db rows)
              (if retry
                (raise "Could not create mat-fns for table" {:relvar (as-relvar relvar)})
                (delete-n relvar (materialize db (as-relvar relvar)) rows true)))))
        update-where
        (fn update-where
          [relvar db f-or-set-map exprs]
          (let [f (if (map? f-or-set-map)
                    (reduce-kv (fn [f k e] (comp f (let [f2 (expr-row-fn e)] #(assoc % k (f2 %))))) identity f-or-set-map)
                    f-or-set-map)
                matched-rows (q db (conj (as-relvar relvar) (into [:where] exprs)))
                new-rows (mapv f matched-rows)
                db (delete-n relvar db matched-rows false)
                db (insert-n relvar db new-rows false)]
            db))
        delete-where
        (fn delete-where
          [relvar db exprs]
          (let [rows (q db (conj (as-relvar relvar) (into [:where] exprs)))]
            (delete-n relvar db rows false)))
        upsert
        (fn [relvar db rows]
          (binding [*upsert-collisions* {}]
            (let [db2 (insert-n relvar db rows false)
                  db-new (reduce-kv (fn [db table-key rows] (delete-n table-key db rows false)) db *upsert-collisions*)]
              (if (identical? db-new db)
                db2
                (insert-n relvar db-new (remove (get *upsert-collisions* (as-table-key relvar) #{}) rows) false)))))]
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
            :update (let [[f-or-set-map & exprs] args] (update-where relvar db f-or-set-map exprs))
            :upsert (upsert relvar db args)))))))

(defn- mat-head [relvar]
  (case (count relvar)
    0 (raise "Invalid relvar, no statements" {:relvar relvar})
    (update relvar (dec (count relvar)) vary-meta assoc :mat true)))

;; --
;; initialisation
;; the bane of my existence
;; this function is responsible for triggering insert flow on materialization

(declare assoc-if-not-nil)

(defn- find-roots [m relvar]
  (let [g (::graph m {})
        db (::state m {})
        {:keys [deps, provide]} (g relvar)]
    (cond
      (contains? db relvar) [relvar]
      provide [relvar]
      :else (mapcat #(find-roots m %) deps))))

(defn- init [m relvars]
  (letfn [(clear-dirty [g relvar]
            (let [{:keys [dirty] :as n} (g relvar)
                  n (dissoc n :dirty)
                  g (assoc g relvar n)]
              (reduce clear-dirty g dirty)))

          (flow [m relvar dependent inserts deletes]
            (let [g (::graph m)
                  {:keys [dirty, pass, mat-fns, mat]} (g dependent)]
              (cond

                (and (= identity pass) (seq dirty) (not mat))
                (let [_ (trace "pass: " relvar (count dirty))
                      m (reduce #(flow %1 dependent %2 inserts deletes) m dirty)]
                  m)

                :else
                (let [db (::state m {})
                      {:keys [insert delete]} (mat-fns relvar)
                      db (if (seq deletes) (delete db deletes) db)
                      db (if (seq inserts) (insert db inserts) db)]
                  (assoc m ::state db, ::graph g)))))

          (initialise-root [m relvar]
            (trace "Initialising root" relvar)
            (let [g (::graph m {})
                  db (::state m {})
                  {:keys [provide, dirty, empty-index, mat]} (g relvar)
                  dirty (when (seq dirty) (sort-by dependent-sort dirty))]
              (cond
                (contains? db relvar)
                (let [_ (trace "init: contains" relvar)
                      data (row-seqable (db relvar))
                      m (reduce #(flow %1 relvar %2 data nil) m dirty)]
                  m)
                provide
                (let [_ (trace "init: provide" relvar)
                      data (provide db)
                      empty-index (or empty-index (when mat empty-set-index))
                      empty-index-type-compatible (and (set? data) (set? empty-index))
                      db (cond
                           empty-index-type-compatible (assoc db relvar (with-meta data (meta empty-set-index)))
                           empty-index (assoc db relvar (reduce index-row empty-index data))
                           :else db)
                      m (assoc m ::state db)
                      m (reduce #(flow %1 relvar %2 data nil) m dirty)]
                  m)
                :else
                (raise "Should never get here, should have root relvar" {}))))]

    (let [roots (set (mapcat (partial find-roots m) relvars))
          ;;
          m (reduce initialise-root m roots)
          g (::graph m {})
          g (reduce clear-dirty g roots)]
      (assoc m ::graph g))))

(declare transact)

;; --
;; materialize/dematerialize api

(defn- simple-table-relvar? [relvar]
  (or (table*-relvar? relvar)
      (case (count relvar)
        1 (let [stmt (head-stmt relvar)]
            (and (= 2 (count stmt)) (= :table (operator stmt))))
        false)))

(defn- mat-head-if-not-table [relvar]
  (if (simple-table-relvar? relvar)
    relvar
    (mat-head relvar)))

(defn- materialize* [db opts relvars]
  (let [m (meta db)
        relvars (map mat-head-if-not-table relvars)
        g (::graph m {})
        g (add-mat-fns g relvars)
        g (if (:ephemeral opts) g (update g ::materialized (fnil into #{}) relvars))
        transactor (dataflow-transactor g)
        m (assoc m ::transactor transactor, ::graph g)
        m (init m relvars)
        db (with-meta db m)]
    db))

(defn- dematerialize* [db opts relvars]
  (let [m (meta db)
        og (::graph m {})
        g (if (:ephemeral opts) og (update og ::materialized (fnil set/difference #{}) (set relvars)))
        g (reduce delete-from-graph g relvars)
        g (add-mat-fns g (::materialized g))
        transactor (dataflow-transactor g)
        m (assoc m ::transactor transactor, ::graph g)
        st (::state m {})
        st (reduce (fn ! [st relvar]
                     (if (not (contains? g relvar))
                       (let [{:keys [deps extras]} (og relvar)]
                         (-> (apply dissoc st relvar (concat (for [dep (cons nil deps)
                                                                   init-key [[::init relvar dep]
                                                                             [::init relvar dep]]]
                                                               init-key)
                                                             extras))
                             (as-> st (reduce ! st deps))))
                       st)) st relvars)
        m (assoc m ::state st)
        db (with-meta db m)]
    db))

(defn materialize [db & relvars] (materialize* db {} relvars))
(defn dematerialize [db & relvars] (dematerialize* db {} relvars))

;; --
;; transact api

(def ^:private ^:dynamic *foreign-key-violations* nil)
(def ^:private ^:dynamic *foreign-key-cascades* nil)

(defn transact
  "Return a new relic database, with the transaction applied.

  Accepts transactional commands as args (tx)

  You modify relic databases by submitting commands to tables, a table can be referenced by name, or by table relvar.

  Commands:

  Insert with :insert vectors
  [:insert table row1, row2 ..]

  Delete by predicates with :delete vectors
  [:delete table expr1 expr2 ..]
  e.g [:delete Customer [< :age 42]]

  Update rows with :update vectors.
  [:update table fn-or-map expr1 expr2 .. ]
  e.g [:update Customer {:age inc} [< :age 42]]

  You can use a map as terser multi table insert form:
  {table [row1, row2 ...], ...}

  ---

  Note:
  As relic stores its state and dataflow graph in metadata, all modifications to the database must be made using
  relic transact/track-transact - all bets are off otherwise.

  --

  See also track-transact, what-if."
  [db & tx]
  (if-some [transactor (::transactor (meta db))]
    (binding [*foreign-key-violations* {}
              *foreign-key-cascades* {}]
      (let [db (reduce transactor db tx)
            cascade-tx (reduce-kv (fn [acc [relvar _references _clause] rows] (conj acc (into [:delete-exact (unwrap-table relvar)] rows))) [] *foreign-key-cascades*)
            db (reduce transactor db cascade-tx)]
        (doseq [[[relvar references clause] rows] *foreign-key-violations*]
          (when (seq rows)
            (raise "Foreign key violation" {:relvar relvar, :references references, :clause clause, :rows rows})))
        db))
    (apply transact (vary-meta db assoc ::transactor (dataflow-transactor {})) tx)))

(defn- materialized-relation [db relvar]
  (or (cond
        (keyword? relvar) (db relvar)
        (table-relvar? relvar)
        (let [[_ table-key] (head-stmt relvar)]
          (db table-key)))
      ((::state (meta db) {}) relvar)))

;; --
;; query is based of index lookup
;; in certain cases, e.g for :hash or :btree
;; it might be useful for library users to have raw index access.

(defn index
  "Returns the raw index storing rows for relvar.

  Normally a set, but if the last statement in the relvar is an index statement, you will get a specialised
  datastructure, this can form the bases of using materialized relic indexes in other high-performance work on your data.


  :hash will yield nested maps (path being the expressions in the hash e.g [:hash :a :b :c]
  will yield an index {(a ?row) {(b ?row) {(:c ?row) #{?row}}}


  :btree is the same as hash but gives you a sorted map instead.

  :unique will give you an index where the keys map to exactly one row, so [:unique :a :b :c]
  will yield an index {(a ?row) {(b ?row) {(:c ?row) ?row}}}"
  [db relvar]
  (or (materialized-relation db relvar)
      (when-not (keyword? relvar)
        (let [db (materialize db relvar)]
          ((::state (meta db) {}) relvar)))))

;; --
;; relic expr to function

(defn- expr-row-fn [expr]
  (cond
    (= [] expr) (constantly [])

    (= ::% expr) identity

    (vector? expr)
    (let [[f & args] expr
          [f args]
          (cond
            #?@(:clj [(qualified-symbol? f) [@(requiring-resolve f) args]])
            #?@(:clj [(symbol? f) [@(resolve f) args]])
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

    #?@(:clj [(qualified-symbol? expr) @(requiring-resolve expr)])

    #?@(:clj [(symbol? expr) @(resolve expr)])

    (fn? expr) expr

    (map? expr)
    (if (seq expr)
      (apply every-pred (map (fn [[k v]] (let [kf (expr-row-fn k)] #(= v (kf %)))) expr))
      (constantly true))

    :else (constantly expr)))

;; -- query

;; as :sort/:rsort can use relic expressions
;; we need to deal with the possibility you might use expressions requiring
;; joins
(defn- add-implicit-expr-joins [relvar exprs]
  (binding [*env-deps* #{}]
    (let [expr-fns (mapv expr-row-fn exprs)]
      (if (seq *env-deps*)
        [(conj relvar [:left-join (conj Env [:extend [::env [select-keys ::env *env-deps*]]])]) expr-fns true]
        [relvar expr-fns]))))

(defn q
  "Queries relic for a collection of unique rows (relation).

  Takes a relvar, or a map form [1].

  Relvars are relational expressions, they describe some data that you might want.

  Think SQL table, View & Query rolled up into one idea.

  They are modelled as vectors of statements.

   e.g [stmt1, stmt2, stmt3]

  Each statement is also a vector, a complete relvar would look like:

  [[:table :Customer]
   [:where [= :name \"alice\"]]]

  Operators quick guide:

  [:table name]
  [:where & expr]
  [:extend & [col|[& col] expr]]
  [:expand & [col expr]]
  [:agg [& group-col] & [col agg-expr]]
  [:join relvar {left-col right-col, ...}]
  [:left-join relvar {left-col right-col, ...}]
  [:from relvar]
  [:without & col]
  [:select & col|[col|[& col] expr]]
  [:difference relvar]
  [:union relvar]
  [:intersection relvar]
  [:qualify namespace-string]
  [:rename {existing-col new-col, ...}]
  [:const collection-of-rows]

  ---

   Transducing:

   If you want a different collection back, you can apply a transducer to the relations rows with :xf
   e.g :xf (map :a) will instead of returning you a collection of rows, will return a collection of (:a row)

  ---

  Sorting:

   Sort with :sort / :rsort

   Pass either a relic expr (e.g a column keyword or function, or relic vector), or coll of expressions to sort by those expressions.
   e.g :sort :a == sort by :a
       :sort [:a :b] == sort by :a then :b
       :sort [[inc a]] == sort by (inc (:a row))

   Note: indexes are not used yet for ad-hoc sorts, but you can use rel/index and :btree for that if you are brave.

  ---

  [1] map forms can be used to issue multiple queries at once, this allows relic to share indexes and intermediate structures
  and can be more efficient.

    {key relvar|{:q relvar, :rsort ...}}

  Note: Expect only that the result is seqable/reducable, custom Relation type might follow in a later release"
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
         sort-fn (when sort-fns (if (= 1 (count sort-fns)) (first sort-fns) (apply juxt2 sort-fns)))

         rs (cond
              sort (sort-by sort-fn rs)
              rsort (sort-by sort-fn (fn [a b] (compare b a)) rs)
              :else rs)

         xf (if drop-env
              (comp (map #(dissoc % ::env)) (or xf identity))
              xf)

         rs (cond
              into-coll (if xf (into into-coll xf rs) (into into-coll rs))
              xf (sequence xf rs)
              :else rs)]
     rs)))

(defn what-if
  "Returns the relation for relvar if you were to apply the transactions with transact.
  Because databases are immutable, its not hard to do this anyway with q & transact. This is just sugar."
  [db relvar & tx]
  (q (apply transact db tx) relvar))

;; --
;; change tracking api

(defn watch
  "Establishes watches on the relvars, watched relvars are change tracked for subsequent transactions
  such that track-transact will return changes to those relvars in its results.

  Returns a new database.

  See track-transact."
  [db & relvars]
  (let [db (vary-meta db update ::watched (fnil into #{}) relvars)]
    (materialize* db {:ephemeral true} relvars)))

(defn unwatch
  "Removes a watched relvar, changes for that relvar will no longer be tracked.

  See track-transact."
  [db & relvars]
  (let [db (vary-meta db update ::watched (fnil set/difference #{}) (set relvars))]
    (dematerialize* db {:ephemeral true} relvars)))

(defn track-transact
  "Like transact, but instead of returning you a database, returns a map of

    :result the result of (apply transact db tx)
    :changes a map of {relvar {:added [row1, row2 ...], :deleted [row1, row2, ..]}, ..}

  The :changes allow you to react to additions/removals from derived relvars, and build reactive systems."
  [db & tx]
  (binding [*added* (zipmap (::watched (meta db)) (repeat #{}))
            *deleted* (zipmap (::watched (meta db)) (repeat #{}))]
    (let [ost db
          db (apply transact db tx)
          added (for [[relvar added] *added*
                      :let [idx (index db relvar)]]
                  [relvar {:added (filterv #(contains-row? idx %) added)}])
          deleted (for [[relvar deleted] *deleted*
                        :let [oidx (or (index ost relvar) empty-set-index)
                              idx (or (index db relvar) empty-set-index)]]
                    [relvar {:deleted (filterv (every-pred #(not (contains-row? idx %)) #(contains-row? oidx %)) deleted)}])]
      {:result db
       :changes (merge-with merge (into {} added) (into {} deleted))})))

;; --
;; relvar analysis
;; analysis of relvars will get better later... be patient :)

(defmulti columns* (fn [_ stmt] (operator stmt)))

(defmethod columns* :default [_ _] [])

(defn full-columns [relvar]
  (if (empty? relvar)
    []
    (let [stmt (head-stmt relvar)
          left (left-relvar relvar)]
      (columns* left stmt))))

(defn full-dependencies [relvar]
  (if-some [table (unwrap-table relvar)]
    #{table}
    (let [left (left-relvar relvar)
          stmt (head-stmt relvar)
          {:keys [deps]} (dataflow-node left stmt)]
      (set (mapcat full-dependencies deps)))))

(defn dependencies
  "Returns the (table name) dependencies of the relvar, e.g what tables it could be affected by."
  [relvar]
  (distinct (map #(nth % 1) (full-dependencies relvar))))

(defn columns
  "Returns the (known) columns on the relvar, e.g what it might return."
  [relvar]
  (map :k (full-columns relvar)))

;; -

(defn- assoc-if-not-nil [m k v]
  (if (nil? v)
    m
    (assoc m k v)))

(defn- bind-fn [binding]
  (if (keyword? binding)
    (if (= ::* binding)
      #(merge % %2)
      #(assoc-if-not-nil % binding %2))
    #(merge % (select-keys %2 binding))))

;; -
;; going to be starting specific statement implementation about now...

;; -
;; extension forms [col|[& col] expr]
;; used by :extend and :select
;; for arbitrary relic expressions, allows computing new columns

(defn- extend-form-fn [form]
  (cond
    (vector? form)
    (let [[binding expr] form
          expr-fn (expr-row-fn expr)
          bind (bind-fn binding)]
      (fn bind-extend [row] (bind row (expr-fn row))))

    :else (throw (ex-info "Not a valid binding form, expected a vector [binding expr]" {}))))

(defn- extend-form-cols [form]
  (let [[binding] form]
    (if (keyword? binding)
      #{binding}
      (set binding))))

;; --
;; expansion forms [col expr]
;; used by :expand
;; for expressions that yield collections

(defn- expand-form-xf [form]
  (let [[binding expr] form
        expr-fn (expr-row-fn expr)
        bind-fn (bind-fn binding)]
    (mapcat
      (fn [row]
        (for [v (expr-fn row)]
          (bind-fn row v))))))

;; --
;; helpers for common dataflow patterns


;; --
;; pass through just flows rows to the next guy in dataflow

(defn- pass-through-insert [db rows inserted _inserted1 _deleted _deleted1] (inserted db rows))
(defn- pass-through-delete [db rows _inserted _inserted1 deleted _deleted1] (deleted db rows))

(defn- pass-through-insert1 [db row _inserted inserted1 _deleted _deleted1] (inserted1 db row))
(defn- pass-through-delete1 [db row _inserted _inserted1 _deleted deleted1] (deleted1 db row))

;; --
;; :table

(defmethod dataflow-node ::table*
  [_ [_ table-key]]
  {:insert {nil pass-through-insert}
   :insert1 {nil pass-through-insert1}
   :delete {nil pass-through-delete}
   :delete1 {nil pass-through-delete1}
   :provide (fn [db] (db table-key empty-set-index))})

(defn- req-col-check [table-key col]
  {:pred [some? col],
   :error [str [::esc col] " required by " [::esc table-key] ", but not found."]})

(defmethod dataflow-node :table
  [left [_ table-key {:keys [req check fk unique]} :as stmt]]
  (if (seq left)
    (dataflow-node left [:from [stmt]])
    (let [req-checks (map (partial req-col-check table-key) req)
          check-stmt (into [:check] req-checks)
          check-stmt (into check-stmt check)
          check-stmt (when-not (= 1 (count check-stmt)) check-stmt)
          fk-stmts (map (fn [[relvar clause opts]] [:fk relvar clause opts]) fk)
          unique-stmts (map (fn [cols] (into [:unique] cols)) unique)
          table* [[::table* table-key]]
          constrain (when (or check-stmt (seq fk-stmts) (seq unique-stmts))
                      (conj table* (cond-> [:constrain]
                                           check-stmt (conj check-stmt)
                                           (seq fk-stmts) (into fk-stmts)
                                           (seq unique-stmts) (into unique-stmts))))]
      (if constrain
        {:empty-index empty-set-index
         :deps [constrain table*]
         :pass identity
         :insert {table* pass-through-insert}
         :insert1 {table* pass-through-insert1}
         :delete {table* pass-through-delete}
         :delete1 {table* pass-through-delete1}}
        {:deps [table*]
         :pass identity
         :insert {table* pass-through-insert}
         :insert1 {table* pass-through-insert1}
         :delete {table* pass-through-delete}
         :delete1 {table* pass-through-delete1}}))))

(defmethod columns* :table
  [_ [_ _ {:keys [req]} :as _stmt]]
  (for [k req] {:k k}))

;; --
;; :from

(defmethod dataflow-node :from
  [_ [_ relvar]]
  {:deps [relvar]
   :pass identity
   :insert {relvar pass-through-insert}
   :insert1 {relvar pass-through-insert1}
   :delete {relvar pass-through-delete}
   :delete1 {relvar pass-through-delete1}})

(defmethod columns* :from
  [_ [_ relvar]]
  (full-columns relvar))

;; non overwriting extend could use more straightforward dataflow
;; as it cannot narrow rows to share the same value.

(defn- transform-node
  [left f]
  (let [idx-key [::transform left f]]
    {:deps [left]
     :extras [idx-key]
     :insert1 {left (fn [db row _inserted inserted1 _deleted _deleted1]
                      (let [new-row (f row)
                            idx (db idx-key {})
                            s (idx new-row #{})
                            ns (conj s row)]
                        (if (identical? s ns)
                          db
                          (let [db (assoc db idx-key (assoc idx new-row ns))]
                            (if (empty? s)
                              (inserted1 db new-row)
                              db)))))}
     :insert {left (fn [db rows inserted _inserted1 _deleted _deleted1]
                     (let [idx (db idx-key {})
                           added (volatile! (transient #{}))
                           nidx (reduce (fn [idx row]
                                          (let [new-row (f row)
                                                s (idx new-row #{})
                                                ns (conj s row)]
                                            (if (identical? s ns)
                                              idx
                                              (let [idx (assoc idx new-row ns)]
                                                (when (empty? s)
                                                  (vswap! added conj! new-row))
                                                idx))))
                                        idx rows)
                           db (assoc db idx-key nidx)
                           added (persistent! @added)]
                       (if (seq added)
                         (inserted db added)
                         db)))}
     :delete1 {left (fn [db row _inserted _inserted1 _deleted deleted1]
                      (let [new-row (f row)
                            idx (db idx-key {})
                            s (idx new-row #{})
                            ns (disj s row)]
                        (cond
                          (identical? s ns) db
                          (empty? ns) (-> db (assoc idx-key (dissoc idx new-row)) (deleted1 new-row))
                          :else (-> db (assoc idx-key (assoc idx new-row ns))))))}
     :delete {left (fn [db rows _inserted _inserted1 deleted _deleted1]
                     (let [idx (db idx-key {})
                           deleted-rows (volatile! (transient #{}))
                           nidx (reduce (fn [idx row]
                                          (let [new-row (f row)
                                                s (idx new-row #{})
                                                ns (disj s row)]
                                            (cond
                                              (identical? s ns) idx
                                              (empty? ns) (do (vswap! deleted-rows conj! new-row)
                                                              (dissoc idx new-row))
                                              :else (assoc idx new-row ns))))
                                        idx rows)
                           db (assoc db idx-key nidx)
                           deleted-rows (persistent! @deleted-rows)]
                       (if (seq deleted-rows)
                         (deleted db deleted-rows)
                         db)))}}))
;; --
;; :without

(defmethod dataflow-node :without
  [left [_ & cols]]
  (let [cols (vec (set cols))
        f (fn [row] (apply dissoc row cols))]
    (transform-node left f)))

(defmethod columns* :without
  [left [_ & cols]]
  (remove (comp (set cols) :k) (full-columns left)))

;; --
;; :union

(defmethod dataflow-node :union
  [left [_ right]]
  (let [left (mat-head left)
        right (mat-head right)]
    {:deps [left right]
     :insert {left pass-through-insert
              right pass-through-insert}
     :insert1 {left pass-through-insert1
               right pass-through-insert1}
     :delete {left (fn [db rows _inserted _inserted1 deleted _deleted1]
                     (let [idx2 (db right empty-set-index)
                           del-rows (remove #(contains-row? idx2 %) rows)]
                       (deleted db del-rows)))
              right (fn [db rows _inserted _inserted1 deleted _deleted1]
                      (let [idx2 (db left empty-set-index)
                            del-rows (remove #(contains-row? idx2 %) rows)]
                        (deleted db del-rows)))}
     :delete1 {left (fn [db row _inserted _inserted1 _deleted deleted1]
                      (let [idx2 (db right empty-set-index)]
                        (if (contains-row? idx2 row)
                          db
                          (deleted1 db row))))
               right (fn [db row _inserted _inserted1 _deleted deleted1]
                       (let [idx2 (db left empty-set-index)]
                         (if (contains-row? idx2 row)
                           db
                           (deleted1 db row))))}}))

(defmethod columns* :union
  [left [_ right]]
  (let [left-idx (index-by :k (full-columns left))
        right-idx (index-by :k (full-columns right))]
    (concat
      (for [[k col] left-idx]
        (if (right-idx k)
          (if (= col (right-idx k))
            col
            {:k k})
          col))
      (for [[k col] right-idx
            :when (not (left-idx k))]
        col))))

;; --
;; :intersection

(defmethod dataflow-node :intersection
  [left [_ right]]
  (let [left (mat-head left)
        right (mat-head right)]
    {:deps [left right]
     :insert {left (fn [db rows inserted _inserted1 _deleted _deleted1]
                     (let [idx2 (db right empty-set-index)
                           add-rows (filter #(contains-row? idx2 %) rows)]
                       (inserted db add-rows)))
              right (fn [db rows inserted _inserted1 _deleted _deleted1]
                      (let [idx2 (db left empty-set-index)
                            add-rows (filter #(contains-row? idx2 %) rows)]
                        (inserted db add-rows)))}
     :insert1 {left (fn [db row _inserted inserted1 _deleted _deleted1]
                      (let [idx2 (db right empty-set-index)]
                        (if (contains-row? idx2 row)
                          (inserted1 db row)
                          db)))
               right (fn [db row _inserted inserted1 _deleted _deleted1]
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
  (full-columns left))

;; --
;; :difference

(defmethod dataflow-node :difference
  [left [_ right]]
  (let [left (mat-head left)
        right (mat-head right)]
    {:deps [left right]
     :insert {left (fn [db rows inserted _inserted1 _deleted _deleted1]
                     (let [idx2 (db right empty-set-index)
                           add-rows (remove #(contains-row? idx2 %) rows)]
                       (inserted db add-rows)))
              right (fn [db rows _inserted _inserted1 deleted _deleted1]
                      (let [idx2 (db left empty-set-index)
                            del-rows (filter #(contains-row? idx2 %) rows)]
                        (deleted db del-rows)))}
     :insert1 {left (fn [db row _inserted inserted1 _deleted _deleted1]
                      (let [idx2 (db right empty-set-index)]
                        (if (contains-row? idx2 row)
                          db
                          (inserted1 db row))))
               right (fn [db row _inserted _inserted1 _deleted deleted1]
                       (let [idx2 (db left empty-set-index)]
                         (if (contains-row? idx2 row)
                           (deleted1 db row)
                           db)))}
     :delete {left (fn [db rows _inserted _inserted1 deleted _deleted1]
                     (let [idx2 (db right empty-set-index)
                           del-rows (remove #(contains-row? idx2 %) rows)]
                       (deleted db del-rows)))
              right (fn [db rows inserted _inserted1 _deleted _deleted1]
                      (let [idx2 (db left empty-set-index)
                            add-rows (filter #(contains-row? idx2 %) rows)]
                        (inserted db add-rows)))}
     :delete1 {left (fn [db row _inserted _inserted1 _deleted deleted1]
                      (let [idx2 (db right empty-set-index)]
                        (if (contains-row? idx2 row)
                          db
                          (deleted1 db row))))
               right (fn [db row _inserted inserted1 _deleted _deleted1]
                       (let [idx2 (db left empty-set-index)]
                         (if (contains-row? idx2 row)
                           (inserted1 db row)
                           db)))}}))

(defmethod columns* :difference
  [left [_ _]]
  (full-columns left))

;; --
;; :index-lookup

(defmethod dataflow-node :lookup
  [_ [_ index & path]]
  (let [path (vec path)
        exprs (vec (take (count path) (rest (head-stmt index))))
        path-fn (apply juxt2 (map expr-row-fn exprs))]
    {:deps [index]
     :insert1 {index (fn [db row _inserted inserted1 _deleted _deleted1]
                       (if (= (path-fn row) path)
                         (inserted1 db row)
                         db))}
     :delete1 {index (fn [db row _inserted _inserted1 _deleted deleted1]
                       (if (= (path-fn row) path)
                         (deleted1 db row)
                         db))}
     :provide (fn [db]
                (let [idx (db index)]
                  (when idx
                    (seek-n idx path))))}))

;; --
;; :where

(defmethod dataflow-node :where
  [left [_ & exprs]]
  (let [expr-preds (mapv expr-row-fn exprs)
        pred-fn (if (seq exprs) (apply every-pred expr-preds) (constantly true))]
    {:deps [left]
     :insert {left (fn where-insert [db rows inserted _inserted1 _deleted _deleted1] (inserted db (filterv pred-fn rows)))}
     :insert1 {left (fn where-insert1 [db row _inserted inserted1 _deleted _deleted1] (if (pred-fn row) (inserted1 db row) db))}
     :delete {left (fn where-delete [db rows _inserted _inserted1 deleted _deleted1] (deleted db (filterv pred-fn rows)))}
     :delete1 {left (fn where-delete1 [db row _inserted _inserted1 _deleted deleted1] (if (pred-fn row) (deleted1 db row) db))}}))

(defmethod columns* :where
  [left _]
  (full-columns left))

;; --
;; :extend

;; todo reformulate in terms of implicit-joins / unify with env mechanism
(defn- join-expr? [expr] (and (vector? expr) (= ::join-coll (nth expr 0 nil))))
(defn- extend-expr [[_ expr]] expr)
(defn- join-ext? [extension] (join-expr? (extend-expr extension)))

(defn- join-first-expr? [expr] (and (vector? expr) (= ::join-first (nth expr 0 nil))))
(defn- join-first-ext? [extension] (join-first-expr? (extend-expr extension)))


(defmethod dataflow-node ::extend*
  [left [_ & extensions]]
  (let [extend-fns (mapv extend-form-fn extensions)
        f (apply comp (rseq extend-fns))]
    (transform-node left f)))

(defmethod dataflow-node :extend
  [left [_ & extensions]]
  (->> (for [extensions (partition-by (fn [ext] (cond
                                                  (join-ext? ext) :join
                                                  (join-first-ext? ext) :join1
                                                  :else :std)) extensions)]
         (cond
           (join-ext? (first extensions))
           (for [[binding [_ relvar clause]] extensions
                 :let [_ (assert (keyword? binding) "only keyword bindings allowed for join-coll expressions")]]
             [::join-as-coll relvar clause binding])

           (join-first-ext? (first extensions))
           (for [[binding [_ relvar clause]] extensions
                 stmt
                 [[::join-as-coll relvar clause ::join-expr]
                  [::extend* [binding [first ::join-expr]]]
                  [:without ::join-expr]]]
             stmt)

           :else [(into [::extend*] extensions)]))
       (transduce cat conj left)
       ((fn [relvar]
          (dataflow-node (left-relvar relvar) (head-stmt relvar))))))

(defmethod columns* :extend
  [left [_ & extensions]]
  (let [ext-keys (set (mapcat extend-form-cols extensions))]
    (concat
      (for [col (full-columns left)
            :when (not (ext-keys (:k col)))]
        col)
      (for [k ext-keys]
        {:k k}))))

;; --
;; :qualify

(defmethod dataflow-node :qualify
  [left [_ namespace]]
  (let [namespace (name namespace)
        f #(reduce-kv (fn [m k v] (assoc m (keyword namespace (name k)) v)) {} %)]
    (transform-node left f)))

(defmethod columns* :qualify
  [left [_ namespace] _]
  (for [col (full-columns left)]
    (assoc col :k (keyword namespace (name (:k col))))))

;; --;; :rename

(defmethod dataflow-node :rename
  [left [_ renames]]
  (let [extensions (for [[from to] renames] [to from])
        away (keys renames)
        dep (conj left (into [:extend] extensions) (into [:without] away))]
    {:deps [dep]
     :insert {dep pass-through-insert}
     :insert1 {dep pass-through-insert1}
     :delete {dep pass-through-delete}
     :delete1 {dep pass-through-delete1}}))

(defmethod columns* :rename
  [left [_ renames]]
  (for [col (full-columns left)
        :let [nk (get renames (:k col))]]
    (if nk
      (assoc col :k nk)
      col)))

;; --
;; :expand

(defn- expand-node
  [left xf]
  (let [idx-key [::expand left xf]
        i1 (fn [idx inserted row new-row]
             (let [s (idx new-row #{})
                   ns (conj s row)]
               (if (identical? s ns)
                 idx
                 (let [idx (assoc idx new-row ns)]
                   (when (empty? s)
                     (inserted new-row))
                   idx))))
        d1 (fn [idx deleted row new-row]
             (let [s (idx new-row #{})
                   ns (disj s row)]
               (cond
                 (identical? s ns) idx
                 (empty? ns) (do (deleted new-row) (dissoc idx new-row))
                 :else (assoc idx new-row ns))))]
    {:deps [left]
     :extras [idx-key]
     :insert {left (fn [db rows inserted _inserted1 _deleted _deleted1]
                     (let [idx (db idx-key {})
                           added (volatile! (transient #{}))
                           i! #(vswap! added conj! %)
                           nidx (reduce (fn [idx row]
                                          (let [new-rows (into [] xf [row])]
                                            (reduce #(i1 %1 i! row %2) idx new-rows)))
                                        idx rows)
                           db (assoc db idx-key nidx)
                           added (persistent! @added)]
                       (if (seq added)
                         (inserted db added)
                         db)))}
     :delete {left (fn [db rows _inserted _inserted1 deleted _deleted1]
                     (let [idx (db idx-key {})
                           deleted-rows (volatile! (transient #{}))
                           d! #(vswap! deleted-rows conj! %)
                           nidx (reduce (fn [idx row]
                                          (let [new-rows (into [] xf [row])]
                                            (reduce #(d1 %1 d! row %2) idx new-rows)))
                                        idx rows)
                           db (assoc db idx-key nidx)
                           deleted-rows (persistent! @deleted-rows)]
                       (if (seq deleted-rows)
                         (deleted db deleted-rows)
                         db)))}}))

(defmethod dataflow-node :expand
  [left [_ & expansions]]
  (let [exp-xforms (mapv expand-form-xf expansions)
        exp-xf (apply comp (rseq exp-xforms))]
    (expand-node left exp-xf)))

(defmethod columns* :expand
  [left [_ & expansions]]
  (let [exp-keys (set (for [[binding] expansions
                            k (if (vector? binding) binding [binding])]
                        k))]
    (concat
      (for [col (full-columns left)
            :when (not (exp-keys (:k col)))]
        col)
      (for [k exp-keys]
        {:k k}))))

;; --
;; :select

(defmethod dataflow-node :select
  [left [_ & selections]]
  (let [expr-fns (mapv (fn [binding-or-col]
                         (if (vector? binding-or-col)
                           (let [[binding expr] binding-or-col
                                 expr-fn (expr-row-fn expr)
                                 bind-fn (bind-fn binding)]
                             #(bind-fn %1 (expr-fn %2)))
                           #(assoc-if-not-nil %1 binding-or-col (%2 binding-or-col)))) selections)
        select-fn (fn [row] (reduce (fn [r f] (f r row)) {} expr-fns))]
    (transform-node left select-fn)))

(defmethod columns* :select
  [left [_ & selections]]
  (let [[cols exts] ((juxt filter remove) keyword? selections)
        cols (set (concat cols (mapcat extend-form-cols exts)))]
    (concat
      (for [col (full-columns left)
            :when (not (cols (:k col)))]
        col)
      (for [k cols]
        {:k k}))))

;; --
;; :hash (index)

(defmethod dataflow-node :hash
  [left [_ & exprs]]
  (let [expr-fns (mapv expr-row-fn exprs)]
    {:empty-index (map-index {} expr-fns)
     :deps [left]
     :insert {left pass-through-insert}
     :insert1 {left pass-through-insert1}
     :delete {left pass-through-delete}
     :delete1 {left pass-through-delete1}}))

(defmethod columns* :hash
  [left _]
  (full-columns left))

;; --
;; :unique (index)

(defmethod dataflow-node :unique
  [left [_ & exprs :as stmt]]
  (let [expr-fns (mapv expr-row-fn exprs)
        relvar (conj left stmt)
        raise-violation (fn [old-row new-row] (raise "Unique constraint violation" {:relvar relvar, :old-row old-row, :new-row new-row}))
        upsert-collision
        (if-some [[_ table-key] (some-> (unwrap-table left) head-stmt)]
          (fn [old-row new-row]
            (set! *upsert-collisions* (update *upsert-collisions* table-key set-conj old-row))
            new-row)
          raise-violation)
        on-collision (fn on-collision [old-row new-row]
                       (if *upsert-collisions*
                         (upsert-collision old-row new-row)
                         (raise-violation old-row new-row)))]
    {:empty-index (map-unique-index {} expr-fns on-collision)
     :deps [left]
     :insert {left pass-through-insert}
     :insert1 {left pass-through-insert1}
     :delete {left pass-through-delete}
     :delete1 {left pass-through-delete1}}))

;; --
;; :btree (index)

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
  (full-columns left))

;; --
;; join as coll, implementation detail, a royal pain. Can somebody with a PHD help me?!
(defmethod dataflow-node ::join-as-coll
  [left [_ right clause k]]
  (let [left-exprs (set (keys clause))
        right-path-fn (apply juxt2 (map expr-row-fn left-exprs))

        right-exprs (map clause left-exprs)
        right (conj right (into [:hash] right-exprs))
        left-path-fn (apply juxt2 (map expr-row-fn right-exprs))
        join-matches (fn join-matches [m rows] (assoc m k (set rows)))
        mem-key [::join-as-coll-memory left right clause k]
        find-left-rows (fn find-left-rows [mem path-fn rows]
                         (let [sets (into [] (comp (map path-fn) (distinct) (keep mem)) rows)]
                           (case (count sets)
                             0 nil
                             1 (first sets)
                             (apply set/union sets))))]
    {:deps [right left]
     :extras [mem-key]
     :insert {left (fn join-as-coll-left-insert [db rows inserted _inserted1 _deleted _deleted1]
                     (let [idx (db right)
                           mem (db mem-key {})
                           matches (vec (for [row rows
                                              :let [path (right-path-fn row)
                                                    matches (seek-n idx path)]]
                                          (join-matches row matches)))
                           mem2 (reduce (fn [mem row] (update mem (right-path-fn row) set-conj row)) mem matches)]
                       (-> db
                           (assoc mem-key mem2)
                           (inserted matches))))
              right (fn join-as-coll-right-insert [db rows inserted _inserted1 deleted _deleted1]
                      (let [idx (db right)
                            mem (db mem-key {})
                            old-rows (find-left-rows mem left-path-fn rows)
                            deletes old-rows
                            inserts (vec (for [row old-rows
                                               :let [path (right-path-fn row)
                                                     matches (seek-n idx path)]]
                                           (join-matches row matches)))
                            mem2 (reduce (fn [mem row] (disjoc mem (right-path-fn row) row)) mem old-rows)
                            mem2 (reduce (fn [mem row] (update mem (right-path-fn row) set-conj row)) mem2 inserts)]
                        (-> db
                            (assoc mem-key mem2)
                            (deleted deletes)
                            (inserted inserts))))}
     :delete {left (fn join-as-coll-left-delete [db rows _inserted _inserted1 deleted _deleted1]
                     (let [mem (db mem-key {})
                           matches (find-left-rows mem right-path-fn rows)
                           row-set (set rows)
                           matches (filter #(row-set (dissoc % k)) matches)
                           mem2 (reduce (fn [mem row] (disjoc mem (right-path-fn row) row)) mem matches)]
                       (-> db
                           (assoc mem-key mem2)
                           (deleted matches))))
              right (fn join-as-coll-right-delete [db rows inserted _inserted1 deleted _deleted1]
                      (let [idx (db right)
                            mem (db mem-key {})
                            old-rows (find-left-rows mem left-path-fn rows)
                            deletes old-rows
                            inserts (vec (for [row old-rows
                                               :let [path (right-path-fn row)
                                                     matches (seek-n idx path)]]
                                           (join-matches row matches)))
                            mem2 (reduce (fn [mem row] (disjoc mem (right-path-fn row) row)) mem old-rows)
                            mem2 (reduce (fn [mem row] (update mem (right-path-fn row) set-conj row)) mem2 inserts)]
                        (-> db
                            (assoc mem-key mem2)
                            (deleted deletes)
                            (inserted inserts))))}}))

;; --
;; :join

(defmethod dataflow-node :join
  [left [_ right clause]]
  (let [jac (conj left
                  [::join-as-coll right clause ::join]
                  [:where [seq ::join]])]
    (expand-node jac (mapcat (fn [row]
                               (let [rrows (::join row)
                                     row (dissoc row ::join)]
                                 (mapv (fn [rrow] (merge row rrow)) rrows)))))))

(defmethod columns* :join
  [left [_ right]]
  (let [left-cols (full-columns left)
        right-cols (full-columns right)
        right-idx (reduce #(assoc %1 (:k %2) %2) {} right-cols)]
    (concat
      (remove (comp right-idx :k) left-cols)
      right-cols)))

;; --
;; :left-join

(defmethod dataflow-node :left-join
  [left [_ right clause]]
  (let [jac (conj left [::join-as-coll right clause ::join])]
    (expand-node jac (mapcat (fn [row]
                               (let [rrows (::join row)
                                     row (dissoc row ::join)]
                                 (if (empty? rrows)
                                   [row]
                                   (mapv (fn [rrow] (merge row rrow)) rrows))))))))

(defmethod columns* :left-join
  [left [_ right _clause]]
  [left [_ right]]
  (let [left-cols (full-columns left)
        right-cols (full-columns right)
        right-idx (reduce #(assoc %1 (:k %2) %2) {} right-cols)]
    (concat
      (remove (comp right-idx :k) left-cols)
      right-cols)))

;; --
;; aggregates and utilities

(defn- comp-complete [agg f]
  (let [{:keys [complete]} agg]
    (if complete
      (assoc agg :complete (comp f complete))
      (assoc agg :complete f))))

;; --
;; rel/row-count specialisation

(defmethod dataflow-node ::group
  [left [_ cols binding expr sfn]]
  (let [cols (vec (set cols))
        elfn (expr-row-fn expr)
        idx-key [::group left cols binding expr sfn]
        group-fn (apply juxt2 cols)
        group->row #(zipmap cols %)
        adder (if (= identity expr)
                (fn [idx row changed]
                  (let [group (group-fn row)
                        eset (idx group)
                        nset (set-conj eset row)]
                    (if (identical? eset nset)
                      idx
                      (do
                        (vswap! changed conj! group)
                        (assoc idx group nset)))))
                (fn [idx row changed]
                  (let [group (group-fn row)
                        [rows eset] (idx group)
                        nrows (set-conj rows row)
                        nset (if-some [v (elfn row)] (set-conj eset v) eset)]
                    (if (identical? nrows rows)
                      idx
                      (do
                        (when-not (identical? nset eset)
                          (vswap! changed conj! group))
                        (assoc idx group [nrows nset]))))))
        deleter (if (= identity expr)
                  (fn [idx row changed]
                    (let [group (group-fn row)
                          eset (idx group)
                          nset (disj eset row)]
                      (if (identical? eset nset)
                        idx
                        (do
                          (vswap! changed conj! group)
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
                        (do
                          (when-not (identical? nset eset)
                            (vswap! changed conj! group))
                          (if (empty? nrows)
                            (dissoc idx group)
                            (assoc idx group [nrows nset])))))))
        sfn (if (= identity expr)
              sfn
              #(sfn (nth % 1)))]
    {:deps [left]
     :extras [idx-key]
     :insert {left (fn [db rows inserted _inserted1 deleted _deleted1]
                     (let [idx (db idx-key {})
                           changed (volatile! (transient #{}))
                           nidx (reduce
                                  #(adder %1 %2 changed)
                                  idx rows)
                           db (assoc db idx-key nidx)
                           _ (vswap! changed persistent!)

                           old-rows (keep
                                      (fn [group]
                                        (when-some [eret (idx group)]
                                          (assoc (group->row group) binding (sfn eret))))
                                      @changed)

                           new-rows (keep
                                      (fn [group]
                                        (when-some [nret (nidx group)]
                                          (assoc (group->row group) binding (sfn nret))))
                                      @changed)]

                       (-> db
                           (deleted old-rows)
                           (inserted new-rows))))}

     :delete {left (fn [db rows inserted _inserted1 deleted _deleted1]
                     (let [idx (db idx-key {})
                           changed (volatile! (transient #{}))
                           nidx (reduce
                                  #(deleter %1 %2 changed)
                                  idx rows)
                           db (assoc db idx-key nidx)
                           _ (vswap! changed persistent!)

                           old-rows (keep
                                      (fn [group]
                                        (when-some [eret (idx group)]
                                          (assoc (group->row group) binding (sfn eret))))
                                      @changed)
                           new-rows (keep
                                      (fn [group]
                                        (when-some [nret (nidx group)]
                                          (assoc (group->row group) binding (sfn nret))))
                                      @changed)]
                       (-> db
                           (deleted old-rows)
                           (inserted new-rows))))}}))

(defn row-count
  "A relic agg function that can be used in :agg expressions to calculate the number of rows in a relation."
  ([]
   {:custom-node (fn [left cols [binding]] (conj left [::group cols binding identity count]))})
  ([expr]
   {:custom-node (fn [left cols [binding]] (conj left [::group cols binding [:if expr ::%] count]))}))

;; --
;; greatest / least

(defn greatest-by
  "A relic agg function that returns the greatest row by some function. e.g [rel/greatest-by :a] will return the row for which :a is biggest."
  [expr]
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

(defn least-by
  "A relic agg function that returns the smallest row by some function. e.g [rel/least-by :a] will return the row for which :a is smallest."
  [expr]
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

(defn greatest
  "A relic agg function that returns the greatest value for the expression as applied to each row."
  [expr]
  (comp-complete (greatest-by expr) (expr-row-fn expr)))

(defn least
  "A relic agg function that returns the smallest value for the expression as applied to each row."
  [expr]
  (comp-complete (least-by expr) (expr-row-fn expr)))

;; --
;; sum

(def ^:private sum-add-fn #?(:clj +' :cljs +))

(defn sum
  "A relic agg function that returns the sum of the expressions across each row.

  e.g [rel/sum :a] will return the sum of (:a row) applied to each row in the aggregation."
  [& exprs]
  (case (count exprs)
    0 {:combiner (constantly 0) :reducer (constantly 0)}
    1
    (let [expr (first exprs)
          f (expr-row-fn expr)
          xf (keep f)]
      {:combiner sum-add-fn
       :reducer #(transduce xf sum-add-fn %)})
    (let [fns (map expr-row-fn exprs)
          nums (apply juxt fns)
          xf (comp (mapcat nums) (remove nil?))]
      {:combiner sum-add-fn
       :reducer #(transduce xf sum-add-fn %)})))

;; --
;; set-concat

(defn set-concat [expr]
  {:custom-node (fn [left cols [binding]] (conj left [::group cols binding expr identity]))})

;; --
;; count-distinct

(defn count-distinct [& exprs]
  (let [expr (if (= 1 (count exprs)) (first exprs) (into [vector] exprs))]
    {:custom-node (fn [left cols [binding]] (conj left [::group cols binding expr count]))}))

;; --
;; any, like some but this one is called any.

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

;; --
;; top / bottom

(defmethod dataflow-node ::sorted-group
  [left [_ cols binding expr sfn]]
  (let [cols (vec (set cols))
        elfn (expr-row-fn expr)
        idx-key [::sorted-group left cols expr sfn]
        group-fn (apply juxt2 cols)
        group->row #(zipmap cols %)
        add-row (fn [coll row]
                  (if coll
                    (update coll (elfn row) set-conj row)
                    (update (sorted-map) (elfn row) set-conj row)))
        rem-row (fn [coll row]
                  (if coll
                    (disjoc coll (elfn row) row)
                    (sorted-map)))]
    {:deps [left]
     :extras [idx-key]
     :insert {left (fn [db rows inserted _inserted1 deleted _deleted1]
                     (let [idx (db idx-key {})
                           changed (volatile! (transient #{}))
                           nidx (reduce
                                  (fn [idx row]
                                    (let [group (group-fn row)
                                          ev (idx group)
                                          nv (add-row ev row)]
                                      (if (identical? ev nv)
                                        idx
                                        (do
                                          (vswap! changed conj! group)
                                          (assoc idx group nv)))))
                                  idx rows)
                           db (assoc db idx-key nidx)
                           _ (vswap! changed persistent!)

                           old-rows (keep
                                      (fn [group]
                                        (when-some [s (idx group)]
                                          (assoc (group->row group) binding (sfn s))))
                                      @changed)
                           new-rows (mapv
                                      (fn [group]
                                        (when-some [s (nidx group)]
                                          (assoc (group->row group) binding (sfn s))))
                                      @changed)]

                       (-> db
                           (deleted old-rows)
                           (inserted new-rows))))}

     :delete {left (fn [db rows inserted _inserted1 deleted _deleted1]
                     (let [idx (db idx-key {})
                           changed (volatile! (transient #{}))
                           nidx (reduce
                                  (fn [idx row]
                                    (let [group (group-fn row)
                                          ev (idx group)
                                          nv (rem-row ev row)]
                                      (if (identical? ev nv)
                                        idx
                                        (do
                                          (vswap! changed conj! group)
                                          (if (empty? nv)
                                            (dissoc idx group)
                                            (assoc idx group nv))))))
                                  idx rows)
                           db (assoc db idx-key nidx)
                           _ (vswap! changed persistent!)

                           old-rows (keep
                                      (fn [group]
                                        (when-some [ev (idx group)]
                                          (assoc (group->row group) binding (sfn ev))))
                                      @changed)
                           new-rows (keep
                                      (fn [group]
                                        (when-some [nv (nidx group)]
                                          (assoc (group->row group) binding (sfn nv))))
                                      @changed)]
                       (-> db
                           (deleted old-rows)
                           (inserted new-rows))))}}))

(defn top-by [n expr]
  (assert (nat-int? n) "top requires a 0 or positive integer arg first")
  {:custom-node
   (fn [left cols [binding]]
     (conj left [::sorted-group cols binding expr #(into [] (comp (mapcat val) (take n)) (rseq %))]))})

(defn bottom-by [n expr]
  (assert (nat-int? n) "bottom requires a 0 or positive integer arg first")
  {:custom-node
   (fn [left cols [binding]]
     (conj left [::sorted-group cols binding expr #(into [] (comp (mapcat val) (take n)) (seq %))]))})

(defn top [n expr]
  (assert (nat-int? n) "top requires a 0 or positive integer arg first")
  {:custom-node
   (fn [left cols [binding]]
     (conj left [::sorted-group cols binding expr #(into [] (comp (map key) (take n)) (rseq %))]))})

(defn bottom [n expr]
  (assert (nat-int? n) "bottom requires a 0 or positive integer arg first")
  {:custom-node
   (fn [left cols [binding]]
     (conj left [::sorted-group cols binding expr #(into [] (comp (map key) (take n)) (seq %))]))})

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

(defn- maybe-deref [x] (some-> x deref))

(defn- get-custom-agg-node [left cols [_ expr :as agg]]
  (when-some [n (:custom-node (agg-expr-agg expr))]
    (n left cols agg)))

;; --
;; :agg

(defmethod dataflow-node ::generic-agg
  [left [_ cols aggs]]
  (let [key-fn #(select-keys % cols)
        {:keys [reducer combiner complete merger]} (agg-fns aggs)
        complete (or complete identity)
        merge-fn (or merger merge)
        empty-idx (agg-index key-fn reducer combiner complete merge-fn 32)
        idx-key [::agg-index left cols aggs]]
    {:deps [left]
     :extras [idx-key]
     :insert {left (fn [db rows insert _insert1 delete _delete1]
                     (let [idx (db idx-key empty-idx)
                           ks (set (map key-fn rows))
                           old-rows (keep (comp maybe-deref :indexed-row idx) ks)
                           nidx (reduce index-row idx rows)
                           new-rows (keep (comp maybe-deref :indexed-row nidx) ks)
                           db (assoc db idx-key nidx)]
                       (-> db
                           (delete old-rows)
                           (insert new-rows))))}
     :delete {left (fn [db rows insert _insert1 delete _delete1]
                     (let [idx (db idx-key empty-idx)
                           ks (set (map key-fn rows))
                           old-rows (keep (comp maybe-deref :indexed-row idx) ks)
                           nidx (reduce unindex-row idx rows)
                           new-rows (keep (comp maybe-deref :indexed-row nidx) ks)
                           db (assoc db idx-key nidx)]
                       (-> db
                           (delete old-rows)
                           (insert new-rows))))}}))

(defmethod dataflow-node :agg
  [left [_ cols & aggs :as stmt]]
  (let [cols (vec (set cols))
        join-clause (zipmap cols cols)
        [custom-nodes standard-aggs] ((juxt keep remove) #(get-custom-agg-node left cols %) aggs)]
    (if (seq custom-nodes)
      (let [joins (concat (for [node custom-nodes] [:join node join-clause])
                          (when (seq standard-aggs) [[:join (conj left [::generic-agg cols standard-aggs]) join-clause]]))
            left (conj left (into [:select] cols))
            left (reduce conj left joins)]
        {:deps [left]
         :pass true
         :insert {left pass-through-insert}
         :delete {left (fn [db rows _ _ d _] (d db rows) )}})
      (dataflow-node left [::generic-agg cols standard-aggs]))))

(defmethod columns* :agg
  [left [_ cols & aggs]]
  (let [agg-keys (set (for [[binding] aggs] binding))
        left-cols (filter (comp (set cols) :k) (full-columns left))]
    (concat
      (remove agg-keys left-cols)
      (for [agg agg-keys]
        {:k agg}))))

;; --
;; foreign keys

(defn- bad-fk [left relvar clause row cascade]
  (when (and cascade *foreign-key-cascades*)
    (set! *foreign-key-cascades* (update *foreign-key-cascades* [left relvar clause] set-conj (dissoc row ::fk))))
  (if *foreign-key-violations*
    (set! *foreign-key-violations* (update *foreign-key-violations* [left relvar clause] set-conj (dissoc row ::fk)))
    (raise "Foreign key violation" {:relvar left
                                    :references relvar
                                    :clause clause
                                    :row (dissoc row ::fk)})))

(defn- good-fk [left relvar clause row cascade]
  (when (and cascade *foreign-key-cascades*)
    (set! *foreign-key-cascades* (disjoc *foreign-key-cascades* [left relvar clause] (dissoc row ::fk))))
  (when *foreign-key-violations*
    (set! *foreign-key-violations* (disjoc *foreign-key-violations* [left relvar clause] (dissoc row ::fk)))))

(defmethod dataflow-node :fk
  [left [_ relvar clause {:keys [cascade]}]]
  (let [_ (when (and cascade (not (unwrap-table left)))
            (raise "Cascading :fk constraints are only allowed on table relvars" {:relvar left, :references relvar, :clause clause}))
        dep (conj left [::join-as-coll relvar clause ::fk])
        check-fk (fn [{::keys [fk] :as row}]
                   (if (empty? fk)
                     (bad-fk left relvar clause row cascade)
                     (good-fk left relvar clause row cascade)))]
    {:deps [dep]
     :insert1 {dep (fn [db row _insert _insert1 _delete _delete1]
                     (check-fk row)
                     db)}
     :delete1 {dep (fn [db row _insert _insert1 _delete _delete1] (good-fk left relvar clause row cascade) db)}}))

;; --
;; constraint helpers

(defn- check->pred [relvar check]
  (if (map? check)
    (let [{:keys [pred, error]} check
          pred-fn (expr-row-fn pred)
          error-fn (expr-row-fn (or error "Check constraint violation"))]
      (fn [m]
        (let [r (pred-fn m)]
          (if r
            m
            (raise (error-fn m) {:expr pred, :check check, :row m, :relvar relvar})))))
    (let [f2 (expr-row-fn check)]
      (fn [m]
        (let [r (f2 m)]
          (if r
            m
            (raise "Check constraint violation" {:expr check, :check check, :row m, :relvar relvar})))))))

(defmethod dataflow-node :check
  [left [_ & checks]]
  (let [f (apply comp (map (partial check->pred left) (reverse checks)))]
    {:deps [left]
     :insert1 {left (fn [db row _ _ _ _] (f row) db)}
     :delete1 {left (fn [db row _ _ _ _] (f row) db)}}))

;; --
;; :constrain

(defmethod dataflow-node :constrain
  [left [_ & constraints]]
  {:deps (mapv #(conj left %) constraints)})

;; --
;; :const

(defmethod dataflow-node :const
  [_left [_ relation]]
  (let [relation (into empty-set-index relation)]
    {:provide (constantly relation)}))

;; --
;; env api

(defn get-env [db] (first (q db Env)))
(defn set-env-tx [env] (list [:delete Env] [:insert Env {::env env}]))
(defn with-env [db env] (transact db (set-env-tx env)))
(defn update-env [db f & args] (with-env db (apply f (get-env db) args)))

;; --
;; functions for going back and forth between 'normal maps' and relic

(defn strip-meta
  "Given a relic database map, removes any relic meta data."
  [db]
  (vary-meta db (comp not-empty dissoc)
             ::state
             ::graph
             ::transactor
             ::materialized
             ::watched))
;; --
;; ed. probably best not to use right now.

(defn ed [relvar]
  #?(:clj ((requiring-resolve 'com.wotbrew.relic.ed/ed) relvar)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library!")))

(defn ed-transact [& tx]
  #?(:clj (apply (requiring-resolve 'com.wotbrew.relic.ed/global-transact!) tx)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library?!")))

(defn ed-set-env [env]
  #?(:clj ((requiring-resolve 'com.wotbrew.relic.ed/global-set-env!) env)
     :cljs (js/console.log "No ed for cljs yet... Anybody know a good datagrid library?!")))