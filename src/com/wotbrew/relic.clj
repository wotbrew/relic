(ns com.wotbrew.relic
  (:require [clojure.set :as set])
  (:import (clojure.lang IEditableCollection)
           (java.util ArrayList)))

(def set-conj (fnil conj #{}))

(defn- expr-col-deps [expr]
  (cond
    (keyword? expr) #{expr}
    (vector? expr) (let [[_ & args] expr] (into #{} (mapcat expr-col-deps) args))
    :else #{}))

(defn- expr-row-fn [expr]
  ;; todo nil / missing dep safety
  (cond
    (= [] expr) (constantly [])

    ;; todo map? walk?

    ;; todo vector / kw escape ?!

    (vector? expr)
    (let [[f & args] expr
          [get-f args]
          (cond
            (qualified-symbol? f) [(constantly @(requiring-resolve f)) args]
            (symbol? f) [(constantly @(resolve f)) args]
            (= f :and) [(apply every-pred (map expr-row-fn args))]
            (= f :or) [(apply some-fn (map expr-row-fn args))]
            (= f :not) [(constantly not) args]
            :else [(constantly f) args])
          args (map expr-row-fn args)
          get-args (when (seq args) (apply juxt args))]
      (if get-args
        #(apply (get-f %) (get-args %))
        get-f))

    (keyword? expr) expr

    (qualified-symbol? expr) (constantly @(requiring-resolve expr))

    (symbol? expr) (constantly @(resolve expr))

    :else (constantly expr)))

(defn- assoc-if-not-nil [m k v]
  (if (nil? v)
    m
    (assoc m k v)))

(defn- extend-form-fn [form]
  ;; todo nil / missing dep safety
  (cond
    (vector? form)
    (let [[binding ?sep expr] form
          expr (if ?sep expr ?sep)
          expr-fn (expr-row-fn expr)]
      (if (keyword? binding)
        #(assoc-if-not-nil % binding (expr-fn %))
        #(merge % (select-keys (expr-fn %) binding))))

    :else (throw (Exception. "Not a valid binding form, expected a vector [binding :<- expr]"))))

(defn- extend-form-cols [form]
  (let [[binding] form]
    (if (keyword? binding)
      #{binding}
      (set binding))))

(defn- agg-form-fn [form]
  (let [[binding _sep expr] form
        ;;todo complex expressions
        expr-fn expr]
    (if (keyword? binding)
      (fn [m rows]
        (assoc m binding (expr-fn rows)))
      (fn [m rows]
        (merge m (set/project (set (expr-fn rows)) binding))))))

(defn- aggs-fn [forms]
  (let [fs (mapv agg-form-fn forms)]
    (fn [group rows] (reduce (fn [m f] (f m rows)) group fs))))

(defn expand-form-xf [form]
  ;; todo missing nil safety
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

(defn- clj-left-join [xrel yrel km]
  (let [k km
        idx (set/index yrel (vals k))]
    (if (instance? IEditableCollection xrel)
      (persistent!
        (reduce (fn [ret x]
                  (let [found (idx (set/rename-keys (select-keys x (keys k)) k))]
                    (if found
                      (reduce #(conj! %1 (merge x %2)) ret found)
                      (conj! ret x))))
                (transient (empty xrel)) xrel))
      (reduce (fn [ret x]
                (let [found (idx (set/rename-keys (select-keys x (keys k)) k))]
                  (if found
                    (reduce #(conj %1 (merge x %2)) ret found)
                    (conj ret x))))
              (empty xrel) xrel))))

(declare interpret)

(defn- enumerate-index
  ([m-or-set]
   (if-not (map? m-or-set)
     m-or-set
     (let [meta (meta m-or-set)
           {::keys [agg depth]} meta]
       (cond
         agg (:rs m-or-set #{})
         depth (enumerate-index m-or-set depth)
         :else
         ((fn ! [m-or-set]
            (let [vseq (vals m-or-set)]
              (if (map? (first vseq))
                (mapcat ! vseq)
                vseq))) m-or-set)))))
  ([m-or-coll depth]
   (if (<= depth 0)
     (if (map? m-or-coll)
       [m-or-coll]
       m-or-coll)
     (if (map? m-or-coll)
       (recur (vals m-or-coll) (dec depth))
       (recur (mapcat vals m-or-coll) (dec depth))))))

(defn- realise-set [base xf]
  (if xf
    (into #{} xf (enumerate-index base))
    (enumerate-index base)))

(defn- interpret-sop [st base xf relvar2 f & args]
  (let [coll (realise-set base xf)]
    (apply f coll (interpret st relvar2) args)))

(defn- stmt-xf [stmt]
  (case (nth stmt 0)
    :where (filter (apply every-pred (map expr-row-fn (rest stmt))))
    :extend (map (apply comp (map extend-form-fn (reverse (rest stmt)))))
    :expand (apply comp (map expand-form-xf (reverse (rest stmt))))
    :project (let [ks (nth stmt 1)] (comp (map #(select-keys % ks)) (distinct)))
    :project-away (let [ks (nth stmt 1)] (comp (map #(apply dissoc % ks)) (distinct)))
    :select (let [[cols exts] ((juxt filter remove) keyword? (rest stmt))
                  cols (set (concat cols (mapcat extend-form-cols exts)))]
              (map (apply comp #(select-keys % cols) (map extend-form-fn (reverse exts)))))
    nil))

(defn- interpret-from [st relvar i base xf]
  (loop [i i
         xf xf
         base base]
    (if (< i (count relvar))
      (let [stmt (nth relvar i)]
        (case (nth stmt 0)
          ;; core
          :base (recur (inc i) nil (st [stmt] {}))
          :from (let [[_ relvar] stmt] (recur (inc i) nil (interpret st relvar)))
          :agg (let [[_ ks & aggs] stmt
                     f (aggs-fn aggs)
                     groups (group-by #(select-keys % ks) (realise-set base xf))
                     ns (set (for [[m coll] groups] (f m coll)))]
                 (recur (inc i) nil ns))

          ;; sop specials
          :join
          (let [[_ relvar2 clause] stmt
                coll2 (interpret-sop st base xf relvar2 set/join clause)]
            (recur (inc i) nil coll2))
          :left-join
          (let [[_ relvar2 clause] stmt
                coll2 (interpret-sop st base xf relvar2 clj-left-join clause)]
            (recur (inc i) nil coll2))
          :union
          (let [[_ relvar2] stmt
                coll2 (interpret-sop st base xf relvar2 set/union)]
            (recur (inc i) nil coll2))
          :difference
          (let [[_ relvar2] stmt
                coll2 (interpret-sop st base xf relvar2 set/difference)]
            (recur (inc i) nil coll2))
          :intersection
          (let [[_ relvar2] stmt
                coll2 (interpret-sop st base xf relvar2 set/intersection)]
            (recur (inc i) nil coll2))

          ;; indexes
          :hash
          (let [[_ & exprs] stmt
                path-fn (apply juxt (mapv expr-row-fn exprs))
                coll (realise-set base xf)
                idx (reduce #(update-in %1 (path-fn %2) set-conj %2) {} coll)]
            idx)

          :hash-unique
          (let [[_ & exprs] stmt
                path-fn (apply juxt (mapv expr-row-fn exprs))
                coll (realise-set base xf)
                idx (reduce #(assoc-in %1 (path-fn %2) %2) {} coll)]
            idx)

          ;; index lookups
          :lookup
          (let [[_ vals] stmt
                _ (assert (nil? xf) "lookup applied to non index")
                _ (assert (map? base) "lookup applied to non index")]
            (recur (inc i) nil (set (enumerate-index (get-in base vals)))))

          ;; xf specials
          (if-some [xf2 (stmt-xf stmt)]
            (recur (inc i) (if xf (comp xf xf2) xf2) base)
            (throw (Exception. "Unknown stmt")))))
      (realise-set base xf))))

(defn interpret [st relvar]
  (let [m (meta st)]
    (assert m "State had no meta, not valid.")
    (enumerate-index
      (loop [i (count relvar)]
        (if (= 0 i)
          (or (st relvar) (interpret-from st relvar 0 #{} nil))
          (let [sv (subvec relvar 0 i)
                coll (m sv)]
            (if coll
              (interpret-from st relvar i coll nil)
              (recur (dec i)))))))))

(defn- ground? [relvar] (and (= 1 (count relvar)) (= :base (ffirst relvar))) )

(defn- third [coll] (nth coll 2 nil))

(defn- left [relvar]
  (when-some [stmt (peek relvar)]
    (case (nth stmt 0)
      :base nil
      :from (nth stmt 1)
      (pop relvar))))

(defn- right [stmt] (case (nth stmt 0) (:join :left-join :union :difference :intersection) (second stmt) nil))
(defn- clause [stmt] (case (nth stmt 0) (:join :left-join) (third stmt) nil))

(defn- best-join-index [profile relvar exprs]
  (conj relvar (into [:hash] exprs)))

(defn- base-indexes [stmt]
  (when (= :base (nth stmt 0))
    (let [[_ _name _cols _pk & keys] stmt]
      (seq (map (fn [cols] (conj [stmt] (into [:hash-unique] cols))) keys)))))

(defn- implicit-indexes [profile relvar]
  (let [stmt (peek relvar)
        left (left relvar)
        right (right stmt)
        clause (clause stmt)


        left-exprs (when clause (keys clause))
        left-index (when clause (best-join-index profile left left-exprs))

        right-exprs (when clause (vals clause))
        right-index (when clause (best-join-index profile right right-exprs))

        base-indexes (vec (base-indexes stmt))]
    (cond-> base-indexes
            left-index (conj left-index)
            right-index (conj right-index))))

(defn- add-node
  ([g relvar] (add-node g relvar #{}))
  ([g relvar edges]
   (cond
     (empty? relvar) g
     (contains? g relvar) (update g relvar into edges)
     :else
     (let [g (assoc g relvar edges)
           left (left relvar)
           stmt (peek relvar)
           right (right stmt)
           g (if-not left g (add-node g left #{[:left relvar]}))
           g (if-not right g (add-node g right #{[:right relvar]}))
           g (reduce add-node g (implicit-indexes (::profile g) relvar))]
       g))))

(defn flow-fn [edges f]
  (let [flow-sort {:index 0, :left 1, :right 2}
        ;; ? flow from table ?
        ;; e.g function row -> val, val map to flow
        ;; this can reduce computation of certain relvar tree's if you have lots of unique branches for
        ;; different restricts from the same base - not sure if worth it.
        fns (mapv (fn [[edge relvar]] (f relvar edge)) (sort-by (comp flow-sort first) edges))]
    (case (count fns)
      1 (first fns)
      (fn [st rows] (reduce (fn [st f'] (f' st rows)) st fns)))))

(defn- query
  ([st q] (query st q nil))
  ([st q params]
   ;; todo parameterized queries (relvars with holes)
   ;; query optimiser e.g where clause to index lookup
   (interpret st q)))

(defn- empty-index [index-stmt]
  (let [[_ & exprs] index-stmt
        depth (count exprs)]
    (with-meta {} {::depth depth})))

(defn- base-inserter [relvar flow-inserted]
  ;; todo uniqueness invariant check
  (let [stmt (nth relvar 0)
        [_ _name _cols pk] stmt
        pk-path (when pk (apply juxt (mapv expr-row-fn pk)))
        insert (if pk #(assoc-in %1 (pk-path %2) %2) conj)
        default (if pk (empty-index (into [:hash-unique] pk)) #{})]
    (fn insert-base [st rows]
      (let [set1 (st relvar default)
            set2 (reduce insert set1 rows)]
        (if (identical? set1 set2)
          st
          (-> st
              (assoc relvar set2)
              (vary-meta flow-inserted (remove set1 rows))))))))

(defn- dissoc-in [m ks]
  (if-let [[k & ks] (seq ks)]
    (if (seq ks)
      (let [v (dissoc-in (get m k) ks)]
        (if (empty? v)
          (dissoc m k)
          (assoc m k v)))
      (dissoc m k))
    m))

(defn- base-deleter [relvar flow-deleted]
  (let [stmt (nth relvar 0)
        [_ _name _cols pk] stmt
        pk-path (when pk (apply juxt (mapv expr-row-fn pk)))
        delete (if pk #(dissoc-in %1 (pk-path %2)) disj)
        default (if pk (empty-index (into [:hash-unique] pk)) #{})]
    (fn insert-base [st rows]
      (let [set1 (st relvar default)
            set2 (reduce delete set1 rows)]
        (if (identical? set1 set2)
          st
          (-> st
              (assoc relvar set2)
              (vary-meta flow-deleted (filter set1 rows))))))))

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

(def ^:dynamic *warn-on-naive* false)

(defn- mat-meta [profile]
  (let [relvars (:materialize profile)
        g (reduce add-node {} relvars)

        inserter* (volatile! nil)
        deleter* (volatile! nil)

        flow-inserter (memoize #(flow-fn % @inserter*))
        flow-deleter (memoize #(flow-fn % @deleter*))
        ;; todo fast-path for updates? e.g replace primitive old-row/new-row pairs?

        inserter
        (memoize
          (fn inserter [relvar edge]
            (let [flow-inserted (flow-inserter (g relvar))
                  flow-deleted (flow-deleter (g relvar))
                  stmt (peek relvar)
                  xf (stmt-xf stmt)]
              (case (nth stmt 0)
                :base (base-inserter relvar flow-inserted)

                :from flow-inserted

                :hash
                (let [path-fn (apply juxt (map expr-row-fn (rest stmt)))
                      empty (empty-index stmt)]
                  (fn insert-hash [st rows]
                    (let [oidx (st relvar empty)
                          rf (fn [nidx row] (update-in nidx (path-fn row) set-conj row))
                          nidx (reduce rf oidx rows)]
                      (if (identical? nidx oidx)
                        st
                        (assoc st relvar nidx)))))

                :hash-unique
                (let [path-fn (apply juxt (map expr-row-fn (rest stmt)))
                      empty (empty-index stmt)]
                  (fn insert-hash [st rows]
                    (let [oidx (st relvar empty)
                          ;; todo uniqueness invariant check
                          ;; upsert
                          ;; collision is allowed as we are replacing a row with another
                          ;; delayed check:
                          ;; track collisions as part of transactions ?
                          ;; if row still exists in :left set at the end of transaction (was not ephemeral) then
                          ;; the database integrity is violated, track which index and row caused the problem
                          rf (fn [nidx row] (assoc-in nidx (path-fn row) row))
                          nidx (reduce rf oidx rows)]
                      (if (identical? nidx oidx)
                        st
                        (assoc st relvar nidx)))))

                :join
                (let [clause (clause stmt)
                      left (left relvar)
                      right (right stmt)

                      left-index (best-join-index profile left (keys clause))
                      right-index (best-join-index profile right (vals clause))

                      reverse-clause (set/map-invert clause)

                      left-exprs (keys clause)
                      right-exprs (vals clause)

                      left-expr-sort (set/map-invert (into {} (map-indexed vector) left-exprs))
                      right-expr-sort (set/map-invert (into {} (map-indexed vector) right-exprs))

                      from-left-index-path (apply juxt (map expr-row-fn (sort-by left-expr-sort (map reverse-clause right-exprs))))
                      from-right-index-path (apply juxt (map expr-row-fn (sort-by right-expr-sort (map clause left-exprs))))

                      path-fn (case edge :left from-right-index-path from-left-index-path)
                      merge-fn (case edge :left merge (fn reverse-merge [a b] (merge b a)))
                      index (case edge :left right-index left-index)
                      clause-card (count clause)
                      index-card (dec (count index))]
                  ;; todo unique index one side will require less boxing
                  (fn insert [st rows]
                    (let [idx (st index {})
                          new-rows (for [row rows
                                         row2 (enumerate-index (get-in idx (path-fn row)) (- clause-card index-card))]
                                     (merge-fn row row2))
                          coll (st relvar #{})
                          coll2 (reduce conj coll new-rows)]
                      (if (identical? coll coll2)
                        st
                        (-> st
                            (assoc relvar coll2)
                            (flow-inserted (remove coll new-rows)))))))

                :agg
                (let [[_ cols & aggs] stmt
                      key-fn #(select-keys % cols)
                      agg-fn (aggs-fn aggs)
                      empty ^::agg {:rs #{}, :tree {}}]
                  (fn insert [st rows]
                    (let [om (st relvar empty)
                          otree (:tree om)
                          ors (:rs om)
                          grouped (group-by key-fn rows)
                          added (ArrayList.)
                          deleted (ArrayList.)
                          ;; todo recursive branching tree for commutative, associative functions
                          ntree (reduce-kv
                                  (fn [m group new-rows]
                                    (let [{old-rows :rows
                                           old-res :res} (m group)
                                          missing (nil? old-rows)
                                          old-rows (or old-rows #{})
                                          all-rows (set/union old-rows (set new-rows))]
                                      (if (identical? all-rows old-rows)
                                        m
                                        (let [res (agg-fn group all-rows)]
                                          (cond
                                            missing
                                            (do (.add added res)
                                                (assoc m group {:rows all-rows, :res res}))
                                            (not= old-res res)
                                            (do
                                              (.add deleted old-res)
                                              (.add added res)
                                              (assoc m group {:rows all-rows, :res res}))
                                            :else m)))))
                                  otree
                                  grouped)]
                      (if (identical? ntree otree)
                        st
                        (let [nrs (reduce disj ors deleted)
                              nrs (reduce conj nrs added)]
                          (cond->
                            (assoc st relvar (assoc empty :rs nrs :tree ntree))

                            (seq deleted)
                            (flow-deleted deleted)

                            (seq added)
                            (flow-inserted added)))))))

                :union
                (fn insert [st rows]
                  (let [oset (st relvar #{})
                        nset (reduce conj oset rows)]
                    (if (identical? nset oset)
                      st
                      (let [st (assoc st relvar nset)]
                        (flow-inserted st (remove oset rows))))))

                (:intersection :difference)
                (let [right (right stmt)
                      f (case (nth stmt 0)
                          :intersection set/intersection
                          :difference set/difference)
                      f' (case edge
                           :left (fn [st rows] (f (set rows) (query st right)))
                           :right (fn [st rows] (f (query st left) (set rows))))]
                  (fn insert [st rows]
                    (let [nrows (f' st rows)
                          oset (st relvar #{})
                          nset (reduce conj oset nrows)]
                      (if (identical? nset oset)
                        st
                        (let [st (assoc st relvar nset)]
                          (flow-inserted st (remove oset nrows)))))))


                (if xf
                  (fn insert-xf [st rows]
                    (let [oset (st relvar #{})
                          added (ArrayList.)
                          rf
                          (fn
                            ([s] s)
                            ([s row]
                             (let [ns (conj s row)]
                               (if (identical? ns s)
                                 s
                                 (do (.add added row) ns)))))
                          nset (transduce xf rf oset rows)]
                      (if (= 0 (.size added))
                        st
                        (let [st (assoc st relvar nset)]
                          (flow-inserted st added)))))
                  (do
                    (when *warn-on-naive* (println "WARN naive insert materialization" stmt))
                    (fn insert-naive [st _]
                      (let [set1 (st relvar #{})
                            set2 (query st relvar)]
                        (if (identical? set1 set2)
                          st
                          (-> st
                              (assoc relvar set2)
                              (flow-inserted (set/difference set2 set1))))))))))))

        deleter
        (memoize
          (fn deleter [relvar edge]
            (let [flow-inserted (flow-inserter (g relvar))
                  flow-deleted (flow-deleter (g relvar))
                  stmt (peek relvar)
                  xf (stmt-xf stmt)]
              (case (nth stmt 0)
                :base (base-deleter relvar flow-deleted)

                :from flow-deleted

                :hash
                (let [path-fn (apply juxt (map expr-row-fn (rest stmt)))
                      empty (empty-index stmt)]
                  (fn delete-hash [st rows]
                    (let [oidx (st relvar empty)
                          rf (fn [nidx row] (disjoc-in nidx (path-fn row) row))
                          nidx (reduce rf oidx rows)]
                      (if (identical? nidx oidx)
                        st
                        (assoc st relvar nidx)))))
                :hash-unique
                (let [path-fn (apply juxt (map expr-row-fn (rest stmt)))
                      empty (empty-index stmt)]
                  (fn delete-hash-unique [st rows]
                    (let [oidx (st relvar empty)
                          rf (fn [nidx row] (dissoc-in nidx (path-fn row)))
                          nidx (reduce rf oidx rows)]
                      (if (identical? nidx oidx)
                        st
                        (assoc st relvar nidx)))))
                :join
                (let [left (left relvar)
                      right (right stmt)
                      clause (clause stmt)
                      left-index (best-join-index profile left (keys clause))
                      right-index (best-join-index profile right (vals clause))

                      reverse-clause (set/map-invert clause)

                      left-exprs (keys clause)
                      right-exprs (vals clause)

                      left-expr-sort (set/map-invert (into {} (map-indexed vector) left-exprs))
                      right-expr-sort (set/map-invert (into {} (map-indexed vector) right-exprs))

                      from-left-index-path (apply juxt (map expr-row-fn (sort-by left-expr-sort (map reverse-clause right-exprs))))
                      from-right-index-path (apply juxt (map expr-row-fn (sort-by right-expr-sort (map clause left-exprs))))

                      path-fn (case edge :left from-right-index-path from-left-index-path)
                      merge-fn (case edge :left merge (fn reverse-merge [a b] (merge b a)))
                      index (case edge :left right-index left-index)
                      clause-card (count clause)
                      index-card (dec (count index))]
                  (fn delete-join [st rows]
                    (let [idx (st index {})
                          new-rows (for [row rows
                                         row2 (enumerate-index (get-in idx (path-fn row)) (- clause-card index-card))]
                                     (merge-fn row row2))
                          coll (st relvar #{})
                          coll2 (reduce disj coll new-rows)]
                      (if (identical? coll coll2)
                        st
                        (-> st
                            (assoc relvar coll2)
                            (flow-deleted (keep coll new-rows)))))))

                :agg
                (let [[_ cols & aggs] stmt
                      key-fn #(select-keys % cols)
                      agg-fn (aggs-fn aggs)
                      empty ^::agg {:rs #{}
                                    :tree {}}]
                  (fn delete-agg [st rows]
                    (let [om (st relvar empty)
                          ors (:rs om)
                          otree (:tree om)
                          grouped (group-by key-fn rows)
                          added (ArrayList.)
                          deleted (ArrayList.)
                          ntree (reduce-kv
                                  (fn [m group deleted-rows]
                                    (let [{old-rows :rows
                                           old-res :res} (m group)
                                          missing (nil? old-rows)
                                          remaining-rows (if missing #{} (set/difference old-rows (set deleted-rows)))]
                                      (cond
                                        missing m
                                        (empty? remaining-rows)
                                        (do
                                          (.add deleted old-res)
                                          (dissoc m group))
                                        :else
                                        (let [res (agg-fn group remaining-rows)]
                                          (.add deleted old-res)
                                          (.add added res)
                                          (assoc m group {:rows remaining-rows, :res res})))))
                                  otree
                                  grouped)]
                      (if (identical? ntree otree)
                        st
                        (let [nrs (reduce disj ors deleted)
                              nrs (reduce conj nrs added)]
                          (cond->
                            (assoc st relvar (assoc empty :rs nrs :tree ntree))

                            (seq deleted)
                            (flow-deleted deleted)

                            (seq added)
                            (flow-inserted added)))))))

                ;; todo delete union/intersection/difference

                (if xf
                  (fn delete-xf [st rows]
                    (let [oset (st relvar #{})
                          deleted (ArrayList.)
                          rf
                          (fn
                            ([s] s)
                            ([s row]
                             (let [ns (disj s row)]
                               (if (identical? ns s)
                                 s
                                 (do (.add deleted row) ns)))))
                          nset (transduce xf rf oset rows)]
                      (if (= 0 (.size deleted))
                        st
                        (let [st (assoc st relvar nset)]
                          (flow-deleted st deleted)))))
                  (do
                    (when *warn-on-naive* (println "WARN naive delete materialization" stmt))
                    (fn delete-naive [st _]
                      (let [set1 (st relvar #{})
                            set2 (query st relvar)]
                        (if (identical? set1 set2)
                          st
                          (-> st
                              (assoc relvar set2)
                              (flow-deleted (set/difference set1 set2))))))))))))

        _ (vreset! inserter* inserter)
        _ (vreset! deleter* deleter)

        updater
        (memoize
          (fn [relvar]
            (assert (ground? relvar) "cannot update derived relvar")
            (let [insert (inserter relvar :left)
                  delete (deleter relvar :left)]
              (fn [st smap stmts]
                (let [nrelvar (reduce conj relvar stmts)

                      sfn (apply comp (for [[k expr] smap
                                            :let [ef (expr-row-fn expr)]]
                                        (fn [row] (assoc row k (ef row)))))

                      matched-rows (query st nrelvar)
                      updated-rows (into #{} (map sfn) matched-rows)

                      to-delete (set/difference matched-rows updated-rows)
                      to-insert (set/difference updated-rows matched-rows)]
                  (-> st
                      (delete to-delete)
                      (insert to-insert)))))))]
    {::state true
     ::inserter (memoize
                  (fn [relvar]
                    (assert (ground? relvar) "cannot insert into derived relvar")
                    (inserter relvar :left)))
     ::deleter (memoize
                 (fn [relvar]
                   (assert (ground? relvar) "cannot delete into derived relvar")
                   (deleter relvar :left)))
     ::updater (memoize
                 (fn [relvar]
                   (assert (ground? relvar) "cannot update derived relvar")
                   (updater relvar)))}))

(defn transact [st & tx]
  (let [{::keys [state inserter deleter updater]} (meta st)]
    (when-not state (throw (ex-info "Transact applied to state without correct meta" {})))
    (reduce
      (fn transact1 [st tx]
        (if (map? tx)
          (reduce-kv (fn [st relvar rows] ((inserter relvar) st rows)) st tx)
          (let [[op relvar & args] tx]
            (case op
              :insert ((inserter relvar) st args)
              :delete ((deleter relvar) st args)
              :update ((updater relvar) st (first args) (nnext args))))))
      st tx)))

(defn empty-state
  ([] (empty-state {}))
  ([profile] (with-meta {} (mat-meta profile))))

(defn state
  ([m] (state m {}))
  ([m profile]
   (transact (empty-state profile) m)))