(ns com.wotbrew.frp
  (:require [clojure.set :as set])
  (:import (clojure.lang Seqable IPersistentSet IPersistentCollection IObj IMeta)
           (java.util ArrayList)))

;; tuple data dsl
;; left->right like god intended for programming
;; advantages, decent printing (compared to a tree)
;; ITS JUST DATA

;; disadvantages: IS ACTUALLY A TREE

;; relation syntax
[;; base definition, might be symbolic / registered
 [::relvar [:a :b]]

 ;; projection / extend (extend)?
 [:project :a :b]
 [:extend [:a+b :<- [+ :a :b]]]

 ;; select is project + compute, computes require all dep keys, perhaps
 ;; consider an missing/maybe extension
 ;; consider sub select extension
 [:select :a :b
  [:a+b :<- [+ :a :b]]
  ;; can use a vector binding if merging keys from EXPR yielding a map
  [[:e :d] :<- {:e 42, :d 43}]]

 ;; expand to add rows from some function
 [:expand [:d :<- (vector 1 2 3 4)]]

 ;; restrict, if dep & nil implicitly discarded unless annotated ^:nil-safe , or proved safe
 ;; consider sub select extension
 [:where [= :a :b]]

 ;; std inner join
 [:join
  [[:base ::relvar2 [:a :c]]] {:a :a}
  [[:base ::relvar3 [:a :c]]] {:c :a}]

 ;; left join with gaps
 [:left-join
  [[:base ::relvar2 [:a :c]]] {:a :a}
  [[:base ::relvar3 [:a :c]]] {:c :a}]

 ;; aggregations
 [:agg [:a] [:n :<- count]]]

;; union/intersection/difference probably work as you expect...
;; not sure what to do about union & mismatched col sets

;; quick- upsert modification syntax, great for initialization
{::relvar [{:a 1, :b 2}, {:a 2, :b 3}]}

;; full modification syntax
[:insert ::relvar {:a 1, :b 2}]
[:delete ::relvar {:a 1, :b 2} [:where [= :a 42]]]
[:update ::relvar {:b 3} [:where [= :a 1]]]

;; special extension
[:upsert ::relvar [:b] {:a 1, :b 2}]

;; PROFILE
;; constraints and optimisation hints form a PROFILE
;; this value is used to tune relations for a profile, and compile for a profile.
;; if a PROFILE is known ahead of time, we can emit near optimal code

;; constraint syntax
;; PK affects storage, and optimises writes & joins sometimes (set vs map)
[:pk ::relvar [:a]]
;; UK is good for upsert and gives you a sweet index
[:uk ::relvar [:a :b]]
;; FK is good for joins, gives you right index by default
[:fk ::relvar ::relvar2 {:a :b}]

;; row-check is easy
[:check ::relvar [< :a 42]]
;; global check IS FN HARD but do we need it??!?!
[:global-check [= 0 [count [::relvar [:where [= :a 1]]]]]]

;; MATERIALIZATION
;; enables incremental maintenance
;; graph + set diff ops for the most part
;; aggregates are interesting, in many cases natural combine+reduce will work
;; sometimes you may be able to specify how to aggregate/disaggregate, fallback can be a big-boy scan.
[:materialize ::relvar]

;; INDEXES
[:hash-index ::relvar :a]
[:hash-index ::relvar [+ :a :b]]
[:sort-index ::relvar [+ :a :b]]

;; EXECUTION

;; relations ARE plans
;; but as optimisations we can swap out and re-order certain nodes to form
;; more specific plans
;; WHAT IS COOL is the dsl can accept the same plans
;; so if you want to control index access and specifics you can
[[::relvar [:a :b]]
 [:loop-kv-hash-lookup :a [1, 2, 3, 4]]
 [:hash-join ::relvar2 {:a :a} :a]
 [:xf (filter (comp even? :a))]]

;; materialization
;; relvars are stacks
;; relvars can share structure, branches are possible via multi set instructions like join
;; [a, b, c] c always depends on [a, b] and [a, b] depends on [a]
;; for joins

;; compiler can omit code given indexes, constraints that is probably better than what you could have added
;; if it knows about LOW CARDINALITY then it can even unwrap the SETS

;; DATA STRUCTURES
;; custom PersistentRelation data structure for results
;; this will let us fuse transducers more naturally, and realise it lazily
;; could be good for printing
;; has to be a set, seqable all the good stuff

;; TOOLS
;; static analysis will be awesome
;; col typos can be tracked as all rel vars know their columns
;; specs can be checked for compatibility
;; WEB/swing thing that lets you experiment with building a relvar live based on some example data
;; (load data with a >tap or something) then start building?!

;; INTEGRATIONS ?!
;; malli/spec
;; datalogagog ??!?!
;; core.logic


;; API
(comment
  ;; ANONYMOUS RELVAR
  ;; data is interpreted directly with simple transducer chain and memoization for joins
  ;; plain data

  ;; using a registered BASE
  ;; [keyword? some-relvar-data...]

  ;; using an inline base, in which case for equality usage must be exactly the same.
  ;; [[::my-base [:a, :b, :c]]]

  ;; MACRO def to use ::alias names in a registry
  ;; you can then use the ::alias everywhere
  ;; you might be warned about compatibility breakages (e.g removing keys)
  (def-relvar alias statements)

  ;; PROFILE definition, just a map
  {:indexes [...]
   :materialize [...]
   :constraints [...]
   ;; optimise particular relations for read
   :optimise [...]}

  ;; REGISTER A PROFILE for compile time optimisation with:
  (def-profile ::my-profile PROFILE)

  ;; STATE
  ;; looks just like a map, might choose a better representation for certain
  ;; base rels, defines how to add/del from rels and materialize certain relations
  ;; the actual map when printed should just show the state and constraints

  ;; CAN you add constraints/indexes/materialization hints ?!
  ;; adding constraints and indexes at runtime can cause de-optimisation for compiled relations
  (new-state PROFILE)

  ;; GET A RELATION
  ;; relation is a (lazy) Set, reducable, ?!foldable?! etc for good fun times rf and xforms
  (relation st relvar)

  ;; or maybe ILookup/IFn on state?
  (get st relvar)

  ;; TRANSFORMATION
  ;; allows you to take an existing relation and do some work on it without a variable
  ;; not sure if useful
  ;; e.g (transform relation [:where [< :a 42]])
  (transform relation TRANSFORM)


  )

(defn- expr-row-fn [expr]
  ;; todo nil / missing dep safety
  (cond
    (vector? expr)
    (let [[f & args] expr
          args (map expr-row-fn args)
          get-args (when (seq args) (apply juxt args))]
      (if get-args
        #(apply f (get-args %))
        f))

    (keyword? expr) expr

    (fn? expr) expr

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

(defn agg-form-xf [form]
  (let [[binding _sep expr] form
        ;;todo complex expressions
        expr-fn expr]
    (if (keyword? binding)
      (map
        (fn [[m rows]]
          (assoc m binding (expr-fn rows))))
      (map
        (fn [[m rows]]
          (merge m (set/project (set (expr-fn rows)) binding)))))))

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

(defprotocol IRowSet
  (-push-xf [row-set xf])
  (-realize [row-set])
  (-st [row-set]))

(deftype PersistentRelation
  [^:volatile-mutable realised
   coll
   xf
   st
   mdata]
  IRowSet
  (-push-xf [_ xf2]
    (if realised
      (PersistentRelation. nil realised xf2 st mdata)
      (PersistentRelation. nil coll (comp xf xf2) st mdata)))
  (-realize [row-set]
    (when-not realised
      (set! (.-realised row-set)
            (if (= identity xf)
              (if (set? coll)
                coll
                (set coll))
              (into #{} xf coll))))
    realised)
  (-st [row-set] st)
  IObj
  (withMeta [this mta] (PersistentRelation. realised coll xf st mta))
  IPersistentSet
  (disjoin [this o] (disj (-realize this) o))
  (contains [this o] (contains? (-realize this) o))
  (get [this o] (get (-realize this) o))
  Seqable
  (seq [this] (-realize this) (seq realised))
  IPersistentCollection
  (count [this] (count (-realize this)))
  (cons [this o] (conj (-realize this) o))
  (empty [this] (PersistentRelation. #{} #{} identity st nil))
  (equiv [this o] (= o (-realize this)))
  IMeta
  (meta [this] mdata))

(declare relation)

(defn- realize-set [rel] (-realize rel))

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
      (comp (extend-xf (into [:extend] extend-form-fn)) project-xf))))

(defn- join->sops [stmt]
  (let [[_ join-pairs] stmt
        join-pairs (partition 2 join-pairs)]
    (->> join-pairs
         (mapv (fn [[right clause]] [:sop set/join right clause])))))

(defn- build-sops [f stmt]
  (let [[_ & rels] stmt]
    (->> rels
         (mapv (fn [right] [:sop f right])))))

(defn- transform [rel stmt]
  (case (nth stmt 0)
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
          coll (-realize rel)
          coll (apply sop coll (relation st rel2) args)]
      (->PersistentRelation coll coll identity st (meta rel)))

    :join
    (->> (join->sops stmt)
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
    (let [coll (realize-set rel)
          [_ group-cols & aggs] stmt
          group-key-fn (if (seq group-cols) #(select-keys % group-cols) (constantly {}))
          grouped (group-by group-key-fn coll)]
      (if (seq aggs)
        (->PersistentRelation nil grouped (comp (apply comp (map agg-form-xf aggs))) (-st rel) (meta rel))
        (->PersistentRelation nil (keys grouped) identity (-st rel) (meta rel))))))

(def relvar-registry (atom {}))
(def profile-registry (atom {}))

(defn- get-materialized [st relvar]
  (let [s (get st relvar #{})]
    (->PersistentRelation s s identity st nil)))

(defn relation [st relvar]
  (if (keyword? relvar)
    (relation st (get @relvar-registry relvar))
    (reduce
      transform
      (get-materialized st (nth relvar 0))
      (subvec relvar 1))))

(defmacro def-relvar [k & forms] `(do (swap! relvar-registry assoc ~k [~@forms]) nil))
(defmacro def-profile [k m] `(do (swap! profile-registry assoc ~k ~m) nil))

(defn new-state
  ([] (new-state nil))
  ([profile] (with-meta {} {::profile profile})))

(defn- graphize
  "Constructs a graph of relvar to other relvars that will receive inserted/deleted/updated rows
  the edges are mostly named :left and sometimes :right (for join/union/diff/intersect).

  So you for a relvar:

  [base [:xf xform]]

  end up with a graph like this
  {base #{[:left [base [:xf xform]]}
   [base [:xf xform]] #{}}"
  [g relvar next]
  (if (contains? g relvar)
    (update g relvar into next)
    (let [g (assoc g relvar next)
          head (peek relvar)
          dep1 (pop relvar)
          g (if (empty? dep1) g (graphize g dep1 #{[:left relvar]}))]
      (case (nth head 0)
        :join1
        (let [[_ right-relvar] head
              g (graphize g right-relvar #{[:right relvar]})]
          g)
        g))))

(defn- inserter
  "Given a relvar graph (from graphize) will construct a function that when given a
  st and rows will materialize changes to the state based on the graph.

  This function alongside updater/deleter form the foundation for materialized views."
  [g relvar from]
  (let [head (peek relvar)
        flow (g relvar)
        nfns (mapv (fn [[from relvar]] (inserter g relvar from)) flow)
        flow-fn (case (count nfns)
                  1 (first nfns)
                  (fn [st added] (reduce (fn [st f] (f st added)) st nfns)))]
    (case (nth head 0)
      :xf
      (let [xf (apply comp (rest head))]
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
                (flow-fn st added))))))

      :sop
      (let [[_ sop right & args] head]
        (fn insert-join1 [st rows]
          (let [nrows
                (case from
                  :left (apply sop rows (st right #{}) args)
                  :right (apply sop (st (pop relvar) #{}) rows args))
                oset (st relvar #{})
                nset (reduce conj oset nrows)]
            (if (identical? nset oset)
              st
              (let [st (assoc st relvar nset)
                    added (set/difference nrows oset)]
                (flow-fn st added))))))

      (fn insert-base [st rows]
        (let [oset (st relvar #{})
              nset (reduce conj oset rows)]
          (if (identical? nset oset)
            st
            (let [st (assoc st relvar nset)
                  added (set/difference rows oset)]
              (flow-fn st added))))))))

(defn modify [st tx]
  (let [{:keys [::graph]} (meta st)]
    (if (map? tx)
      (modify st (for [[rel rows] tx
                       row rows]
                   [:insert rel row]))
      (reduce
        (fn [st stmt]
          (case (nth stmt 0)
            :insert (let [[_ relvar row] stmt
                          inserter (inserter graph relvar nil)]
                      (inserter st #{row}))
            :update st
            :delete st))
        st tx))))

(defn optimise [relvar profile]

  ;; select/project/extend/expand/where replaced with xf
  ;; join replaced with multiple join1 or various indexed joins
  ;; union replaced with multiple union1, diff/intersection are the same
  ;; agg replaced with bucketed agg strategy e.g reduce/combine pattern

  ;; we can compile a fast relation function of a state with that profile
  ;; and of course this graph can be materialized.

  ;; :where and :join can use indexes in PROFILE and therefore might be replaced, as long as it would not affect
  ;; the result.

  ;; return an optimised relvar

  (loop [acc ()
         relvar relvar]
    (if (empty? relvar)
      (vec acc)
      (let [head (peek relvar)
            tail (pop relvar)]
        (case (nth head 0)
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

          :join (recur (reduce conj acc (join->sops head)) tail)
          :union (recur (reduce conj acc (build-sops set/union head)) tail)
          (:diff :difference) (recur (reduce conj acc (build-sops set/difference head)) tail)
          (:intersect :intersection) (recur (reduce conj acc (build-sops set/intersection head)) tail)

          (recur (conj acc head) tail))))))


