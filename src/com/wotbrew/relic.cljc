(ns com.wotbrew.relic
  (:require [clojure.set :as set]))

(defn sum [coll] (reduce + coll))
(defn count-distinct [coll] (count (set coll)))

(def ^:private set-conj (fnil conj #{}))

(defn- expr-col-deps
  "Given an expression will return a set of columns the expression depends on."
  [expr]
  (cond
    (keyword? expr) #{expr}
    (vector? expr) (let [[_ & args] expr] (into #{} (mapcat expr-col-deps) args))
    :else #{}))

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

(defn- clj-left-join [xrel yrel km]
  (let [k km
        idx (set/index yrel (vals k))]
    (reduce (fn [ret x]
              (let [found (idx (set/rename-keys (select-keys x (keys k)) k))]
                (if found
                  (reduce #(conj %1 (merge x %2)) ret found)
                  (conj ret x))))
            (empty xrel) xrel)))

(defn- group-add-in [m [k & ks] empty row]
  (if ks
    (assoc m k (group-add-in (get m k empty) ks empty row))
    (update m k set-conj row)))

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

(declare interpret)

(defn- enumerate-index
  ([m-or-set]
   (if-not (map? m-or-set)
     m-or-set
     (let [meta (meta m-or-set)
           {::keys [agg depth unique]} meta]
       (cond
         agg (:rs m-or-set #{})
         depth (enumerate-index m-or-set depth unique)
         :else
         ((fn ! [m-or-set]
            (let [vseq (vals m-or-set)]
              (if (map? (first vseq))
                (mapcat ! vseq)
                (mapcat identity vseq)))) m-or-set)))))
  ([m-or-coll depth unique]
   (cond
     (nil? m-or-coll) nil

     (= depth 1)

     (if unique
       (if (map? m-or-coll)
         (vals m-or-coll)
         (mapcat vals m-or-coll))
       (if (map? m-or-coll)
         (mapcat identity (vals m-or-coll))
         (mapcat vals m-or-coll)))

     (<= depth 0) (if unique [m-or-coll] m-or-coll)

     :else
     (for [v (if (map? m-or-coll) (vals m-or-coll) m-or-coll)
           v2 (enumerate-index v (dec depth) unique)]
       v2))))

(defn- realise-set [base xf]
  (if xf
    (into #{} xf (enumerate-index base))
    (if (set? base)
      base
      (set (enumerate-index base)))))

(defn- interpret-sop [st base xf relvar2 f & args]
  (let [coll (realise-set base xf)]
    (apply f coll (realise-set (interpret st relvar2) nil) args)))

(defn- stmt-type [stmt]
  (nth stmt 0))

(defn- stmt-xf [stmt]
  (case (stmt-type stmt)
    :where (filter (apply every-pred (map expr-row-fn (rest stmt))))
    :extend (map (apply comp (map extend-form-fn (reverse (rest stmt)))))
    :expand (apply comp (map expand-form-xf (reverse (rest stmt))))
    :project (let [ks (nth stmt 1)] (comp (map #(select-keys % ks)) (distinct)))
    :project-away (let [ks (nth stmt 1)] (comp (map #(apply dissoc % ks)) (distinct)))
    :select (let [[cols exts] ((juxt filter remove) keyword? (rest stmt))
                  cols (set (concat cols (mapcat extend-form-cols exts)))]
              (map (apply comp #(select-keys % cols) (map extend-form-fn (reverse exts)))))
    nil))

(def ^:dynamic *warn-on-interpret* false)

(defn- interpret-from [st relvar i base xf]
  (loop [i i
         xf xf
         base base]
    (if (< i (count relvar))
      (let [stmt (nth relvar i)]
        (when *warn-on-interpret* (when-not (= :from (stmt-type stmt)) (println "WARN interpreting" (pr-str stmt))))
        (case (stmt-type stmt)
          ;; core
          :state (recur (inc i) nil (st [stmt]))
          :from (let [[_ relvar] stmt] (recur (inc i) nil (interpret st relvar)))
          :agg (let [[_ ks & aggs] stmt
                     f (aggs-fn aggs)
                     groups (group-by #(select-keys % ks) (realise-set base xf))
                     ns (set (for [[m coll] groups] (f m coll)))]
                 (recur (inc i) nil ns))
          :coll (let [[_ coll] stmt] (recur (inc i) nil coll))

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
            (recur (inc i) nil idx))

          :hash-unique
          (let [[_ & exprs] stmt
                path-fn (apply juxt (mapv expr-row-fn exprs))
                coll (realise-set base xf)
                idx (reduce #(assoc-in %1 (path-fn %2) %2) ^{::unique true, ::depth (count exprs)} {} coll)]
            (recur (inc i) nil idx))

          :btree
          (let [[_ & exprs] stmt
                sort-key (apply juxt (mapv expr-row-fn exprs))
                coll (realise-set base xf)
                sm (sorted-map)
                idx (reduce #(group-add-in %1 (sort-key %2) sm %2) (with-meta sm {::depth (count exprs)}) coll)]
            (recur (inc i) nil idx))

          ;; index lookups
          :lookup
          (let [[_ & vals] stmt
                _ (assert (nil? xf) "lookup applied to non index")
                _ (assert (map? base) "lookup applied to non index")]
            (recur (inc i) nil (get-in base vals)))

          ;; xf specials
          (if-some [xf2 (stmt-xf stmt)]
            (recur (inc i) (if xf (comp xf xf2) xf2) base)
            (throw (Exception. "Unknown stmt")))))
      (if xf
        (realise-set base xf)
        base))))

(defn interpret
  "Interprets the relvar directly without query optimisation.

  Returns a collection whose type will depend on the last statement in the relvar.

  e.g TODO EXAMPLE"
  [st relvar]
  (let [m (or (meta st) st)]
    (loop [i (count relvar)]
      (if (= 0 i)
        (or (st relvar) (interpret-from st relvar 0 #{} nil))
        (let [sv (subvec relvar 0 i)
              coll (m sv)]
          (if coll
            (interpret-from st relvar i coll nil)
            (recur (dec i))))))))

(defn- ground? [relvar]
  (and (= 1 (count relvar)) (= :state (stmt-type (first relvar)))) )

(defn- third [coll] (nth coll 2 nil))

(defn- left [relvar]
  (when-some [stmt (peek relvar)]
    (case (stmt-type stmt)
      :state nil
      :from (nth stmt 1)
      (pop relvar))))

(defn- right [stmt] (case (stmt-type stmt) (:join :left-join :union :difference :intersection) (second stmt) nil))
(defn- clause [stmt] (case (stmt-type stmt) (:join :left-join) (third stmt) nil))

(defn- best-join-index [profile relvar exprs]
  (conj relvar (into [:hash] exprs)))

(defn- base-indexes [profile relvar stmt]
  (when (= :state (stmt-type stmt))
    (let [[_ _name {:keys [uks fks]}] stmt]
      (concat
        (map (fn [cols] (conj [stmt] (into [:hash-unique] cols))) uks)
        (mapcat (fn [[right clause]]
                  [(best-join-index profile relvar (keys clause))
                   (best-join-index profile right (vals clause))]) fks)))))

(defn- implicit-indexes [profile relvar]
  (let [stmt (peek relvar)
        left (left relvar)
        right (right stmt)
        clause (clause stmt)

        left-exprs (when clause (keys clause))
        left-index (when clause (best-join-index profile left left-exprs))

        right-exprs (when clause (vals clause))
        right-index (when clause (best-join-index profile right right-exprs))

        base-indexes (vec (base-indexes profile relvar stmt))]
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
           ;; ignore left if we are replacing the base
           left (when-not (= :state (stmt-type stmt)) left)
           right (right stmt)
           g (if-not left g (add-node g left #{[:left relvar]}))
           g (if-not right g (add-node g right #{[:right relvar]}))
           g (reduce add-node g (implicit-indexes (::profile g) relvar))]
       g))))

(defn flow-fn [edges f]
  (let [flow-sort {:index 0, :left 1, :right 2}
        fns (mapv (fn [[edge relvar]] (f relvar edge)) (sort-by (comp flow-sort first) edges))]
    (case (count fns)
      1 (first fns)
      2 (let [[f1 f2] fns] (fn [st rows] (f2 (f1 st rows) rows)))
      (fn [st rows]
        (reduce (fn [st f'] (f' st rows)) st fns)))))

(defn query*
  ([st q] (query* st q nil))
  ([st q params]
   (interpret st q)))

(defn query
  ([st q] (query st q nil))
  ([st q params]
   (enumerate-index (query* st q params))))

(defn- empty-index [index-stmt]
  (let [[index-type & exprs] index-stmt
        depth (count exprs)]
    (with-meta {} {::index index-stmt
                   ::unique (case index-type
                              :hash-unique true
                              false)
                   ::depth depth})))

;; will make cljs compat easier
(defn- mutable-list [] #?(:clj (java.util.ArrayList.)
                          :cljs (js/Array.)))
(defn- add! [mutable-list row]
  #?(:clj (.add ^java.util.ArrayList mutable-list row)
     :cljs (.push mutable-list row)))

(defn- base-inserter [relvar flow-inserted]
  (let [stmt (nth relvar 0)
        [_ _name {:keys [pk]}] stmt
        pk-fns (when pk (mapv expr-row-fn pk))
        pk-path (when pk (apply juxt pk-fns))

        insert (if pk
                 (case (count pk)
                   1 (let [[k] pk-fns] #(assoc %1 (k %2) %2))
                   #(assoc-in %1 (pk-path %2) %2))
                 conj)

        default (if pk (empty-index (into [:hash-unique] pk)) #{})
        contains-row? (if pk #(= %2 (get-in %1 (pk-path %2))) contains?)]
    (fn insert-base [st rows]
      (let [set1 (st relvar default)
            set2 (reduce insert set1 rows)]
        (if (identical? set1 set2)
          st
          (-> st
              (assoc relvar set2)
              (vary-meta assoc relvar set2)
              (vary-meta flow-inserted (remove #(contains-row? set1 %) rows))))))))

(defn- base-deleter [relvar flow-deleted]
  (let [stmt (nth relvar 0)
        [_ _name {:keys [pk]}] stmt
        pk-fns (when pk (mapv expr-row-fn pk))
        pk-path (when pk (apply juxt pk-fns))
        delete (if pk
                 (case (count pk)
                   1 (let [[k] pk-fns] #(dissoc %1 (k %2)))
                   #(dissoc-in %1 (pk-path %2)))
                 disj)
        default (if pk (empty-index (into [:hash-unique] pk)) #{})
        get-row (if pk #(get-in %1 (pk-path %2) %2) get)]
    (fn delete-base [st rows]
      (let [set1 (st relvar default)
            set2 (reduce delete set1 rows)]
        (if (identical? set1 set2)
          st
          (-> st
              (assoc relvar set2)
              (vary-meta assoc relvar set2)
              (vary-meta flow-deleted (keep #(get-row set1 %) rows))))))))

(def ^:dynamic *warn-on-naive-materialization* false)

(defn- join-calcs [relvar profile edge]
  (let [stmt (peek relvar)
        left (left relvar)
        right (right stmt)
        clause (clause stmt)
        left-index (best-join-index profile left (keys clause))
        right-index (best-join-index profile right (vals clause))

        reverse-clause (set/map-invert clause)

        left-exprs (keys clause)
        right-exprs (vals clause)

        left-expr-sort (set/map-invert (into {} (map-indexed vector) left-exprs))
        right-expr-sort (set/map-invert (into {} (map-indexed vector) right-exprs))

        from-right-path-exprs (sort-by left-expr-sort (map reverse-clause right-exprs))
        from-left-path-exprs (sort-by right-expr-sort (map clause left-exprs))

        path-exprs (case edge :left from-right-path-exprs :right from-left-path-exprs)
        path-fn (apply juxt (map expr-row-fn path-exprs))
        merge-fn (case edge :left merge (fn reverse-merge [a b] (merge b a)))
        index (case edge :left right-index left-index)
        clause-card (count clause)
        index-card (dec (count (peek index)))]
    {:index index
     :path-fn path-fn
     :path-exprs path-exprs
     :merge-fn merge-fn
     :index-depth (- index-card clause-card)
     :index-unique (case (stmt-type (peek index))
                     :hash-unique true
                     false)}))

(defn- mat-meta [profile]
  (let [relvars (:materialize profile)
        g (reduce add-node {} relvars)

        inserter* (volatile! nil)
        deleter* (volatile! nil)

        flow-inserter (memoize #(flow-fn % @inserter*))
        flow-deleter (memoize #(flow-fn % @deleter*))

        fns
        (memoize
          (fn fns [relvar edge]
            (let [flow-inserted (flow-inserter (g relvar))
                  flow-deleted (flow-deleter (g relvar))
                  stmt (peek relvar)
                  xf (stmt-xf stmt)]
              (case (stmt-type stmt)
                :state
                {:inserter (base-inserter relvar flow-inserted)
                 :deleter (base-deleter relvar flow-deleted)}

                :from
                {:inserter flow-inserted
                 :deleter flow-deleted}

                :hash
                {:inserter
                 (let [path-fn (apply juxt (map expr-row-fn (rest stmt)))
                       empty (empty-index stmt)
                       rf (fn [nidx row] (update-in nidx (path-fn row) set-conj row))]
                   (fn insert-hash [st rows]
                     (let [oidx (st relvar empty)
                           nidx (reduce rf oidx rows)]
                       (if (identical? nidx oidx)
                         st
                         (assoc st relvar nidx)))))
                 :deleter
                 (let [path-fn (apply juxt (map expr-row-fn (rest stmt)))
                       empty (empty-index stmt)]
                   (fn delete-hash [st rows]
                     (let [oidx (st relvar empty)
                           rf (fn [nidx row] (disjoc-in nidx (path-fn row) row))
                           nidx (reduce rf oidx rows)]
                       (if (identical? nidx oidx)
                         st
                         (assoc st relvar nidx)))))}

                :btree
                {:inserter
                 (let [[_ & exprs] stmt
                       path-fn (apply juxt (map expr-row-fn exprs))
                       sm (sorted-map)
                       rf (fn [nidx row] (group-add-in nidx (path-fn row) sm row))]
                   (fn insert-sort [st rows]
                     (let [oidx (st relvar sm)
                           nidx (reduce rf oidx rows)]
                       (if (identical? nidx oidx)
                         st
                         (assoc st relvar nidx)))))
                 :deleter
                 (let [[_ & exprs] stmt
                       path-fn (apply juxt (map expr-row-fn exprs))
                       sm (sorted-map)
                       rf (fn [nidx row] (disjoc-in nidx (path-fn row) row))]
                   (fn delete-sort [st rows]
                     (let [oidx (st relvar sm)
                           nidx (reduce rf oidx rows)]
                       (if (identical? nidx oidx)
                         st
                         (assoc st relvar nidx)))))}

                :hash-unique
                {:inserter
                 (let [path-fn (apply juxt (map expr-row-fn (rest stmt)))
                       empty (empty-index stmt)
                       rf (fn [nidx row] (assoc-in nidx (path-fn row) row))]
                   (fn insert-hash [st rows]
                     (let [oidx (st relvar empty)
                           nidx (reduce rf oidx rows)]
                       (if (identical? nidx oidx)
                         st
                         (assoc st relvar nidx)))))
                 :deleter
                 (let [path-fn (apply juxt (map expr-row-fn (rest stmt)))
                       empty (empty-index stmt)]
                   (fn delete-hash-unique [st rows]
                     (let [oidx (st relvar empty)
                           rf (fn [nidx row] (dissoc-in nidx (path-fn row)))
                           nidx (reduce rf oidx rows)]
                       (if (identical? nidx oidx)
                         st
                         (assoc st relvar nidx)))))}

                :join
                {:inserter
                 (let [{:keys [index, path-fn merge-fn index-depth index-unique]} (join-calcs relvar profile edge)]
                   (fn insert-join [st rows]
                     (let [idx (st index {})
                           new-rows (for [row rows
                                          row2 (enumerate-index (get-in idx (path-fn row)) index-depth index-unique)]
                                      (merge-fn row row2))
                           coll (st relvar #{})
                           coll2 (reduce conj coll new-rows)]
                       (if (identical? coll coll2)
                         st
                         (-> st
                             (assoc relvar coll2)
                             (flow-inserted (remove coll new-rows)))))))
                 :deleter
                 (let [{:keys [index index-depth path-fn merge-fn index-unique]} (join-calcs relvar profile edge)]
                   (fn delete-join [st rows]
                     (let [idx (st index {})
                           new-rows (for [row rows
                                          row2 (enumerate-index (get-in idx (path-fn row)) index-depth index-unique)]
                                      (merge-fn row row2))
                           coll (st relvar #{})
                           coll2 (reduce disj coll new-rows)]
                       (if (identical? coll coll2)
                         st
                         (-> st
                             (assoc relvar coll2)
                             (flow-deleted (keep coll new-rows)))))))}

                :left-join
                {:inserter
                 (let [{:keys [index, path-fn, merge-fn, index-depth index-unique]} (join-calcs relvar profile edge)
                       default-row (case edge :left first second)
                       empty-row (case edge :left [{}] [])]
                   (fn insert-left-join [st rows]
                     (let [idx (st index {})
                           matched-rows (for [row rows
                                              :let [path (path-fn row)]
                                              row2 (let [rs (enumerate-index (get-in idx path) index-depth index-unique)]
                                                     (if (seq rs)
                                                       rs
                                                       empty-row))]
                                          [row row2])
                           new-rows (map (fn [[a b]] (merge-fn a b)) matched-rows)
                           default-rows (map default-row matched-rows)
                           coll (st relvar #{})
                           delete-rows (keep coll default-rows)
                           coll2 (reduce conj coll new-rows)
                           coll2 (reduce disj coll2 delete-rows)]
                       (if (identical? coll coll2)
                         st
                         (-> st
                             (assoc relvar coll2)
                             (flow-deleted delete-rows)
                             (flow-inserted (remove coll new-rows)))))))
                 :deleter
                 (let [{:keys [index, path-fn merge-fn index-depth index-unique]} (join-calcs relvar profile edge)
                       default-row (case edge :right second nil)
                       empty-row (case edge :left [{}] [])]
                   (fn delete-left-join [st rows]
                     (let [idx (st index {})
                           matched-rows (for [row rows
                                              row2 (let [rs (enumerate-index (get-in idx (path-fn row)) index-depth index-unique)]
                                                     (if (seq rs)
                                                       rs
                                                       empty-row))]
                                          [row row2])
                           delete-rows (map (fn [[a b]] (merge-fn a b)) matched-rows)
                           default-rows (when default-row (map default-row matched-rows))
                           coll (st relvar #{})
                           restore-rows (set (remove coll default-rows))
                           coll2 (reduce disj coll delete-rows)
                           coll2 (reduce conj coll2 restore-rows)]
                       (if (identical? coll coll2)
                         st
                         (-> st
                             (assoc relvar coll2)
                             (flow-inserted restore-rows)
                             (flow-deleted (keep coll delete-rows)))))))}

                :agg
                {:inserter
                 (let [[_ cols & aggs] stmt
                       key-fn #(select-keys % cols)
                       agg-fn (aggs-fn aggs)
                       empty ^::agg {:rs #{}, :tree {}}]
                   (fn insert-agg [st rows]
                     (let [om (st relvar empty)
                           otree (:tree om)
                           ors (:rs om)
                           grouped (group-by key-fn rows)
                           added (mutable-list)
                           deleted (mutable-list)
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
                                             (do (add! added res)
                                                 (assoc m group {:rows all-rows, :res res}))
                                             (not= old-res res)
                                             (do
                                               (add! deleted old-res)
                                               (add! added res)
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
                 :deleter
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
                           added (mutable-list)
                           deleted (mutable-list)
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
                                           (add! deleted old-res)
                                           (dissoc m group))
                                         :else
                                         (let [res (agg-fn group remaining-rows)]
                                           (add! deleted old-res)
                                           (add! added res)
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
                             (flow-inserted added)))))))}

                :union
                {:inserter
                 (fn insert-union [st rows]
                   (let [oset (st relvar #{})
                         nset (reduce conj oset rows)]
                     (if (identical? nset oset)
                       st
                       (let [st (assoc st relvar nset)]
                         (flow-inserted st (remove oset rows))))))
                 :deleter
                 (let [[_ relvar2] stmt
                       a-key (case edge :left relvar2 (left relvar))]
                   (fn delete-union [st rows]
                     (let [set2 (query* st a-key)
                           to-remove (remove set2 rows)
                           set1 (st relvar #{})
                           ns (reduce disj set1 to-remove)]
                       (if (identical? ns set1)
                         st
                         (-> st
                             (assoc relvar ns)
                             (flow-deleted to-remove))))))}

                :intersection
                {:inserter
                 (let [right (right stmt)
                       f' (case edge
                            :left (fn [st rows] (set/intersection (set rows) (query* st right)))
                            :right (fn [st rows] (set/intersection (query* st left) (set rows))))]
                   (fn insert-intersection-or-difference [st rows]
                     (let [nrows (f' st rows)
                           oset (st relvar #{})
                           nset (reduce conj oset nrows)]
                       (if (identical? nset oset)
                         st
                         (let [st (assoc st relvar nset)]
                           (flow-inserted st (remove oset nrows)))))))
                 :deleter
                 (fn delete-intersection [st rows]
                   (let [set1 (st relvar #{})
                         ns (reduce disj set1 rows)]
                     (if (identical? ns set1)
                       st
                       (-> st
                           (assoc relvar ns)
                           (flow-deleted (keep set1 rows))))))}

                :difference
                {:inserter
                 (let [right (right stmt)
                       f' (case edge
                            :left (fn [st rows] (set/difference (set rows) (query* st right)))
                            :right (fn [st rows] (set/difference (query* st left) (set rows))))]
                   (fn insert-intersection-or-difference [st rows]
                     (let [nrows (f' st rows)
                           oset (st relvar #{})
                           nset (reduce conj oset nrows)]
                       (if (identical? nset oset)
                         st
                         (let [st (assoc st relvar nset)]
                           (flow-inserted st (remove oset nrows)))))))
                 :deleter
                 (case edge
                   :left
                   (fn delete-difference [st rows]
                     (let [set1 (st relvar #{})
                           ns (reduce disj set1 rows)]
                       (if (identical? ns set1)
                         st
                         (-> st
                             (assoc relvar ns)
                             (flow-deleted (keep set1 rows))))))
                   (fn nop [st _rows] st))}

                (if xf
                  {:inserter
                   (fn insert-xf [st rows]
                     (let [oset (st relvar #{})
                           added (mutable-list)
                           rf
                           (fn
                             ([s] s)
                             ([s row]
                              (let [ns (conj s row)]
                                (if (identical? ns s)
                                  s
                                  (do (add! added row) ns)))))
                           nset (transduce xf rf oset rows)]
                       (if (= 0 (.size added))
                         st
                         (let [st (assoc st relvar nset)]
                           (flow-inserted st added)))))
                   :deleter
                   (fn delete-xf [st rows]
                     (let [oset (st relvar #{})
                           deleted (mutable-list)
                           rf
                           (fn
                             ([s] s)
                             ([s row]
                              (let [ns (disj s row)]
                                (if (identical? ns s)
                                  s
                                  (do (add! deleted row) ns)))))
                           nset (transduce xf rf oset rows)]
                       (if (= 0 (.size deleted))
                         st
                         (let [st (assoc st relvar nset)]
                           (flow-deleted st deleted)))))}

                  (do
                    (when *warn-on-naive-materialization* (println "WARN naive" edge "materialization" stmt))
                    {:inserter
                     (fn insert-naive [st _]
                       (let [set1 (st relvar #{})
                             set2 (interpret-from st relvar 0 nil nil)]
                         (if (identical? set1 set2)
                           st
                           (-> st
                               (assoc relvar set2)
                               (flow-inserted (set/difference set2 set1))))))
                     :deleter
                     (fn delete-naive [st _]
                       (let [set1 (st relvar #{})
                             set2 (interpret-from st relvar 0 nil nil)]
                         (if (identical? set1 set2)
                           st
                           (-> st
                               (assoc relvar set2)
                               (flow-deleted (set/difference set1 set2))))))}))))))

        inserter (comp :inserter fns)
        deleter (comp :deleter fns)

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

                      matched-rows (query* st nrelvar)
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
              :update ((updater relvar) st (first args) (rest args))))))
      st tx)))

(defn empty-state
  ([] (empty-state {}))
  ([profile] (with-meta {} (mat-meta profile))))

(defn state
  ([m] (state m {}))
  ([m profile]
   (transact (empty-state profile) m)))