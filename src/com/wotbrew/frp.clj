(ns com.wotbrew.frp
  (:require [clojure.set :as set]
            [clojure.pprint :as pprint])
  (:import (clojure.lang Seqable IPersistentSet IPersistentCollection IObj IMeta IEditableCollection)
           (java.util ArrayList)
           (java.io Writer)))

;; .cljc will require type definitions to be separated
;; lack of var might have implications
;; ArrayList usage in materialize fns

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
  (let [[binding _sep expr] form
        expr-fn (expr-row-fn expr)]
    (if (keyword? binding)
      #(assoc-if-not-nil % binding (expr-fn %))
      #(merge % (select-keys (expr-fn %) binding)))))

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

(defprotocol IRelationImpl
  (-push-xf [row-set xf])
  (-realize [row-set])
  (-st [row-set]))

(deftype Relation
  [^:volatile-mutable realised
   coll-fn
   xf
   st
   mdata]
  IRelationImpl
  (-push-xf [_ xf2]
    (if realised
      (Relation. nil (constantly realised) xf2 st mdata)
      (Relation. nil coll-fn (comp xf xf2) st mdata)))
  (-realize [row-set]
    (when-not realised
      (set! (.-realised row-set)
            (let [coll (coll-fn)]
              (if (= identity xf)
                (if (set? coll)
                  coll
                  (set coll))
                (into #{} xf coll)))))
    (.-realised row-set))
  (-st [row-set] st)
  IObj
  (withMeta [this mta] (Relation. realised coll-fn xf st mta))
  IPersistentSet
  (disjoin [this o] (disj (-realize this) o))
  (contains [this o] (contains? (-realize this) o))
  (get [this o] (get (-realize this) o))
  Seqable
  (seq [this] (-realize this) (seq realised))
  IPersistentCollection
  (count [this] (count (-realize this)))
  (cons [this o] (conj (-realize this) o))
  (empty [this] (Relation. #{} nil identity st nil))
  (equiv [this o] (= o (-realize this)))
  IMeta
  (meta [this] mdata))

(defn col-set [rel]
  (or (::col-set (meta rel))
      (loop [rel rel]
        (if-some [stmt (peek rel)]
          (let [tail (pop rel)]
            (case (nth stmt 0)
              :from (col-set (second tail))
              :where (col-set tail)
              :union (apply set/union (map col-set (rest stmt)))
              :diff (apply set/union (map col-set (butlast (rest stmt))))


              ))
          #{}))))

(declare relation)

(defn- where-xf [stmt]
  (filter (apply every-pred (map expr-row-fn (rest stmt)))))

(defn- extend-xf [stmt]
  (map (apply comp (map extend-form-fn (reverse (rest stmt))))))

(defn- expand-xf [stmt]
  (apply comp (map expand-form-xf (rest stmt))))

(defn- project-xf [stmt]
  (let [ks (subvec stmt 1)] (map #(select-keys % ks))))

(defn- project-away-xf [stmt]
  (let [ks (subvec stmt 1)] (map #(apply dissoc % ks))))

(defn- select-xf [stmt]
  (let [forms (rest stmt)
        [project-cols extend-forms] ((juxt filter remove) keyword? forms)
        cols (into (set project-cols) (mapcat extend-form-cols) extend-forms)
        project (into [:project] cols)
        project-xf (project-xf project)]
    (if (empty? extend-forms)
      project-xf
      (comp (extend-xf (into [:extend] extend-forms)) project-xf))))

(defn- join->join1s [stmt]
  (let [[_ & join-pairs] stmt
        join-pairs (partition 2 join-pairs)]
    (->> join-pairs
         (mapv (fn [[right clause]] [:join1 right clause])))))

(defn- left-join->join1s [stmt]
  (let [[_ & join-pairs] stmt
        join-pairs (partition 2 join-pairs)]
    (->> join-pairs
         (mapv (fn [[right clause]] [:left-join1 right clause])))))

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

(defn- build-sops [f stmt]
  (let [[_ & rels] stmt]
    (->> rels
         (mapv (fn [right] [:sop f right])))))

(defn- enumerate-index [m-or-set]
  (if (map? m-or-set)
    (map enumerate-index vals m-or-set)
    m-or-set))

(defn- transform [rel stmt]
  (case (nth stmt 0)
    :from
    (relation (-st rel) (second stmt))

    ;; transducer step
    :xf
    (-push-xf rel (apply comp (rest stmt)))

    :where
    (-push-xf rel (where-xf stmt))

    :extend
    (-push-xf rel (extend-xf stmt))

    :expand
    (-push-xf rel (expand-xf stmt))

    :project
    (-push-xf rel (project-xf stmt))

    :project-away
    (-push-xf rel (project-away-xf stmt))

    :select
    (-push-xf rel (select-xf stmt))

    :sop
    (let [[_ sop rel2 & args] stmt
          st (-st rel)
          coll-fn #(apply sop (-realize rel) (relation st rel2) args)]
      (->Relation nil coll-fn identity st (meta rel)))

    :join1
    (let [[_ rel2 clause] stmt]
      (transform rel (if clause [:sop set/join rel2 clause] [:sop set/join rel2])))

    :left-join1
    (let [[_ rel2 clause] stmt]
      (transform rel [:sop clj-left-join rel2 clause]))

    :join
    (->> (join->join1s stmt)
         (reduce transform rel))

    :left-join
    (->> (left-join->join1s stmt)
         (reduce transform rel))

    :nat-join
    (->> (join->join1s stmt)
         (reduce transform rel))

    :union
    (->> (build-sops set/union stmt)
         (reduce transform rel))

    (:diff :difference)
    (->> (build-sops set/difference stmt)
         (reduce transform rel))

    (:intersect :intersection)
    (->> (build-sops set/intersection stmt)
         (reduce transform rel))

    (:agg :summarize)
    (let [coll #(-realize rel)
          [_ group-cols & aggs] stmt
          group-key-fn (if (seq group-cols) #(select-keys % group-cols) (constantly {}))
          grouped #(group-by group-key-fn (coll))
          agg-f (aggs-fn aggs)
          f (fn [[group rows]] (agg-f group rows))]
      (if (seq aggs)
        (->Relation nil grouped (map f) (-st rel) (meta rel))
        (->Relation nil (comp set keys grouped) identity (-st rel) (meta rel))))

    ;; todo, this is more query planning rather than relation interpretation
    ;; materialization will not use these primitives, perhaps they might use tables of some kind
    ;; hash-lookup will require some kind of conditional flow from a table, not sure if worth it?!
    :hash-lookup
    (let [[_ index & vals] stmt
          st (-st rel)
          m (get st index {})
          m-or-set (get-in m vals)]
      (->Relation nil #(enumerate-index m-or-set) identity st nil))

    ;; indexed join
    :hash-join
    (let [[_ index & index-exprs] stmt
          expr-fns (mapv expr-row-fn index-exprs)
          st (-st rel)
          right-index (st index)
          join-row (fn [row rows] (map (partial merge row) rows))
          xf1 (mapcat (fn [row] (join-row row (enumerate-index (get-in right-index (mapv (fn [f] (f row)) expr-fns))))))]
      (-push-xf rel xf1))))

(defn relation [st relvar]
  (if-some [s (get st relvar)]
    (->Relation s nil identity st nil)
    (if (empty? relvar)
      (->Relation #{} nil identity st nil)
      (let [stmt (peek relvar)
            base (pop relvar)]
        (transform (relation st base) stmt)))))

(declare graph-index)

(defn- join-index [profile relvar ks]
  ;; todo pick covering indexes if they exist
  (into [:index relvar] (set ks)))

(defn- graph-relvar
  "Constructs a graph of relvar to other relvars that will receive inserted/deleted/updated rows
  the edges are mostly named :left and sometimes :right (for join/union/diff/intersect).

  So you for a relvar:

  [base [:xf xform]]

  end up with a graph like this
  {base #{[:left [base [:xf xform]]}
   [base [:xf xform]] #{}}"
  ([g profile relvar] (graph-relvar g profile relvar #{}))
  ([g profile relvar next]
   (if (contains? g relvar)
     (update g relvar into next)
     (let [g (assoc g relvar next)
           head (peek relvar)
           dep1 (pop relvar)
           g (if (empty? dep1) g (graph-relvar g profile dep1 #{[:left relvar]}))]
       (case (nth head 0)

         (:join1 :left-join1)
         (let [[_ right-relvar clause] head
               index-left (join-index profile dep1 (keys clause))
               index-right (join-index profile right-relvar (vals clause))]
           (-> g
               (graph-index index-left)
               (graph-index index-right)
               (graph-relvar profile right-relvar #{[:right relvar]})))

         :sop
         (let [[_ _ right-relvar] head
               g (graph-relvar g profile right-relvar #{[:right relvar]})]
           g))))))

(defn- graph-index
  [g index]
  (let [[_ relvar] index
        g (graph-relvar g relvar #{[:index [index]]})]
    g))

(declare deleter inserter)

(defn- updater
  [g profile relvar from]
  (let [insert (inserter g profile relvar from)
        delete (deleter g profile relvar from)]
    (fn insert-base [st smap stmts]
      (let [nrelvar (reduce conj relvar stmts)

            sfn (apply comp (for [[k expr] smap
                                  :let [ef (expr-row-fn expr)]]
                              (fn [row] (assoc row k (ef row)))))

            matched-rows (relation st nrelvar)
            updated-rows (into #{} (map sfn) matched-rows)

            to-delete (set/difference matched-rows updated-rows)
            to-insert (set/difference updated-rows matched-rows)]
        (-> st
            (delete to-delete)
            (insert to-insert))))))

(defn flow-fn [g flow f]
  (let [flow-sort {:index 0, :left 1, :right 2}
        ;; ? flow from table ?
        ;; e.g function row -> val, val map to flow
        ;; this can reduce computation of certain relvar tree's if you have lots of unique branches for
        ;; different restricts from the same base - not sure if worth it.
        fns (mapv (fn [[from relvar]] (f g relvar from)) (sort-by (comp flow-sort first) flow))]
    (case (count fns)
      1 (first fns)
      (fn [st added] (reduce (fn [st f'] (f' st added)) st fns)))))

(defn- deleter
  "Given a relvar graph (from graphize) will construct a function that when given a
  st and rows will materialize changes to the state based on the graph.

  This function alongside updater/deleter form the foundation for materialized views."
  [g profile target from]
  (let [head (peek target)
        flow (g target)
        flow-deleted (flow-fn g flow deleter)
        flow-updated (flow-fn g flow updater)]
    (case (nth head 0)

      :hash
      (let [[_ _ & exprs] head
            exprs (mapv expr-row-fn exprs)
            path-fn (apply juxt exprs)
            disjoc-in (fn d [m ks x]
                        (let [[k & ks] ks]
                          (if ks
                            (let [nm (d (get m k) ks x)]
                              (if (empty? nm)
                                (dissoc m k)
                                (assoc m k nm)))
                            (let [ns (disj (get m k) x)]
                              (if (empty? ns)
                                (dissoc m k)
                                (assoc m k ns))))))]
        (fn hash-expr [st rows]
          (let [oidx (st target {})
                rf (fn [nidx row]
                     (let [path (path-fn row)]
                       (disjoc-in nidx path row)))
                nidx (reduce rf oidx rows)]
            (assoc st target nidx))))

      :xf
      (let [xf (apply comp (rest head))]
        (fn delete-xf [st rows]
          (let [oset (st target #{})
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
              (let [st (assoc st target nset)]
                (flow-deleted st deleted))))))

      :sop
      (let [[_ sop right & args] head]
        (fn delete-sop [st rows]
          (let [nrows
                (case from
                  :left (apply sop rows (st right #{}) args)
                  :right (apply sop (st (pop target) #{}) rows args))
                oset (st target #{})
                nset (reduce disj oset nrows)]
            (if (identical? nset oset)
              st
              (let [st (assoc st target nset)
                    deleted (set/intersection nrows oset)]
                (flow-deleted st deleted))))))

      :agg
      (let [[_ cols & aggs] head
            key-fn #(select-keys % cols)
            agg-fn (aggs-fn aggs)]
        (fn delete-agg [st rows]
          (let [om (st target {})
                grouped (group-by key-fn rows)
                updated (ArrayList.)
                deleted (ArrayList.)
                nm (reduce-kv
                     (fn [m group deleted-rows]
                       (let [{old-rows :rows
                              res :res} (m group)
                             missing (nil? old-rows)
                             remaining-rows (if missing #{} (set/difference old-rows (set deleted-rows)))]
                         (cond
                           missing m
                           (empty? remaining-rows)
                           (do
                             (.add deleted res)
                             (dissoc m group))
                           :else
                           (let [res (agg-fn group remaining-rows)]
                             (.add updated res)
                             (assoc m group {:rows remaining-rows, :res res})))))
                     om
                     grouped)]
            (if (identical? nm om)
              st
              (cond->
                (assoc st target nm)

                (seq updated)
                (flow-updated updated)

                (seq deleted)
                (flow-deleted deleted))))))

      (fn delete-base [st stmts]
        (let [oset (st target #{})
              rows (relation st (conj target (into [:where] stmts)))
              nset (reduce disj oset rows)]
          (if (identical? nset oset)
            st
            (let [st (assoc st target nset)
                  deleted (set/intersection rows oset)]
              (flow-deleted st deleted))))))))

(def set-conj (fnil conj #{}))

(defn- inserter
  "Given a relvar/index graph (from graphize) will construct a function that when given a
  st and rows will materialize changes to the state based on the graph.

  This function alongside updater/deleter form the foundation for materialized views."
  [g profile target from]
  (let [head (peek target)
        flow (g target)
        flow-inserted (flow-fn g flow inserter)
        flow-updated (flow-fn g flow updater)]
    (case (nth head 0)
      :hash
      (let [[_ _ & exprs] head
            exprs (mapv expr-row-fn exprs)
            path-fn (apply juxt exprs)]
        (fn [st rows]
          (let [oidx (st target {})
                rf (fn [nidx row] (update-in nidx (path-fn row) set-conj row))
                nidx (reduce rf oidx rows)]
            (if (identical? nidx oidx)
              st
              ;; could just flow out, but unsure if needed e.g does flow from indexes make sense
              (assoc st target nidx)))))

      :xf
      (let [xf (apply comp (rest head))]
        (fn insert-xf [st rows]
          (let [oset (st target #{})
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
              (let [st (assoc st target nset)]
                (flow-inserted st added))))))

      :sop
      (let [[_ sop right & args] head]
        (fn insert-sop [st rows]
          (let [nrows
                (case from
                  :left (apply sop rows (st right #{}) args)
                  :right (apply sop (st (pop target) #{}) rows args))
                oset (st target #{})
                nset (reduce conj oset nrows)]
            (if (identical? nset oset)
              st
              (let [st (assoc st target nset)
                    added (set/difference nrows oset)]
                (flow-inserted st added))))))

      :agg
      (let [[_ cols & aggs] head
            key-fn #(select-keys % cols)
            agg-fn (aggs-fn aggs)]
        (fn insert-agg [st rows]
          (let [om (st target {})
                grouped (group-by key-fn rows)
                added (ArrayList.)
                updated (ArrayList.)
                nm (reduce-kv
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
                                 (.add updated res)
                                 (assoc m group {:rows all-rows, :res res}))
                               :else m)))))
                     om
                     grouped)]
            (if (identical? nm om)
              st
              (cond->
                (assoc st target nm)
                (seq added)
                (flow-inserted added)

                (seq updated)
                (flow-updated updated))))))

      (:join1 :left-join1)
      (let [[join-type relvar2 clause] head
            ;; clause can be nil for nat-join
            ;; but not sure how to index for that yet!
            left-index (join-index profile (pop target) (keys clause))
            right-index (join-index profile relvar2 (vals clause))

            ;; {:a :a1, :b :b1}
            ;; so if looking up in right index [:b1 :a1] you would get path with [:b :a] so basically need to sort
            ;; and map through the 'clause' to get the right order and index path

            [_ _ & left-ks] left-index
            [_ _ & right-ks] right-index

            reverse-clause (set/map-invert clause)

            left-ks-sort (set/map-invert (into {} (map-indexed vector) left-ks))
            right-ks-sort (set/map-invert (into {} (map-indexed vector) right-ks))

            left-path (apply juxt (sort-by left-ks-sort (map reverse-clause right-ks)))
            right-path (apply juxt (sort-by right-ks-sort (map clause left-ks)))]
        (case from
          :right
          (fn [st rows]
            (let [left-idx (st left-index {})
                  new-rows (set (for [row rows
                                      lrow (enumerate-index (get-in left-idx (left-path row)))]
                                  (merge lrow row)))
                  coll (st target #{})
                  coll2 (reduce conj coll new-rows)]
              (if (identical? coll coll2)
                st
                (-> st
                    (assoc target coll2)
                    (flow-inserted (set/difference new-rows coll))))))
          :left
          (fn [st rows]
            (let [right-idx (st right-index {})
                  ;; todo left-join special sauce
                  new-rows (set (for [row rows
                                      :let [rrow (enumerate-index (get-in right-idx (right-path row)))]]
                                  (merge row rrow)))

                  coll (st target #{})
                  coll2 (reduce conj coll new-rows)]
              (if (identical? coll coll2)
                st
                (-> st
                    (assoc target coll2)
                    (flow-inserted (set/difference new-rows coll))))))))


      (fn insert-base [st rows]
        (let [oset (st target #{})
              nset (reduce conj oset rows)]
          (if (identical? nset oset)
            st
            (let [st (assoc st target nset)
                  added (remove oset rows)]
              (flow-inserted st added))))))))

(defn modify [st tx]
  (let [{:keys [::insert-fn ::update-fn ::delete-fn]} (meta st)]
    (if (map? tx)
      (modify st (for [[rel rows] tx] (into [:insert rel] rows)))
      (reduce
        (fn [st stmt]
          (case (nth stmt 0)
            :insert (let [[_ relvar & rows] stmt
                          inserter (insert-fn relvar)]
                      (inserter st rows))
            :update (let [[_ relvar smap & stmts] stmt
                          updater (update-fn relvar)]
                      (updater st smap stmts))
            :delete (let [[_ relvar & stmts] stmt
                          deleter (delete-fn relvar)]
                      (deleter st stmts))))
        st tx))))

(defn optimise [relvar profile]
  ;; consider fusion of :where & :extend & :expand pre :xf as there might be gains doing this before transducers are baked
  ;; consider fusion of :xf nodes, trading lack of sharing for more free memory
  ;; dead code elimination (dropping extension that do not need to be computed)
  ;; moving projections around might lead to some advantages (e.g earlier in the tree = less rows, later in the tree = less ops on rows)
  ;; consider simplifying select as extend + project
  ;; join / sop ordering e.g low card potential earlier to test less rows against indexes?

  (loop [acc ()
         relvar relvar]
    (if (empty? relvar)
      (vec acc)
      (let [head (peek relvar)
            tail (pop relvar)]
        (case (nth head 0)
          :from
          (recur (reduce conj acc (reverse (optimise (second head) profile))) tail)
          :where
          (recur (conj acc [:xf (where-xf head)]) tail)
          :extend
          (recur (conj acc [:xf (extend-xf head)]) tail)
          :expand
          (recur (conj acc [:xf (expand-xf head)]) tail)
          :project
          (recur (conj acc [:xf (project-xf head)]) tail)
          :project-away
          (recur (conj acc [:xf (project-away-xf head)]) tail)
          :select
          (recur (conj acc [:xf (select-xf head)]) tail)

          :join (recur acc (reduce conj tail (join->join1s head)))
          :left-join (recur acc (reduce conj tail (left-join->join1s head)))
          :nat-join (recur acc (reduce conj tail (join->join1s head)))
          :union (recur acc (reduce conj tail (build-sops set/union head)))
          :difference (recur acc (reduce conj tail (build-sops set/difference head)))
          :intersection (recur acc (reduce conj tail (build-sops set/intersection head)))

          :sop (recur (conj acc (update head 2 optimise profile)) tail)

          (recur (conj acc head) tail))))))

(defn- profile-fns [profile]
  (let [g {}
        {:keys [materialize indexes]} profile
        optimise-fn (memoize (fn [relvar] (optimise relvar profile)))
        materialize (mapv optimise-fn materialize)
        indexes (for [index indexes] (update index 1 optimise-fn))
        g (reduce #(graph-relvar %1 profile %2) g materialize)
        g (reduce graph-index g indexes)
        insert-fn (memoize (fn [relvar] (inserter g profile (optimise-fn relvar) nil)))
        update-fn (memoize (fn [relvar] (updater g profile (optimise-fn relvar) nil)))
        delete-fn (memoize (fn [relvar] (deleter g profile (optimise-fn relvar) nil)))]
    {::profile profile
     ;; you won't necessarily want to cache every query
     ;; consider variables
     ::optimise-fn optimise-fn
     ::insert-fn insert-fn
     ::update-fn update-fn
     ::delete-fn delete-fn}))

(defn new-state
  ([] (new-state nil))
  ([profile] (with-meta {} (merge {::profile profile} (profile-fns profile)))))

;; todo query plan
;; e.g choose index scans as interpretable steps in the relvar (as opposed to materialization which does not need to)