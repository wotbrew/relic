(ns com.wotbrew.tarpit
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [com.wotbrew.idx :as idx])
  (:refer-clojure :exclude [extend]))

;; data dsl
{:op :base
 :cols [:a, :b, :c]}

(defmulti col-exists? (fn [rel col] (:op rel)))
(defmulti interpret-data (fn [rel st] (:op rel)))

(defn q [st rel]
  (with-redefs [interpret-data (memoize interpret-data)]
    (interpret-data rel st)))

(defn base
  "Defines a new anonymous base relation."
  ([cols]
   ^{:type ::base} {:op :base, :cols cols})
  ([name cols]
   (assoc (base cols) :name name)))

(defmethod print-method ::base
  [{:keys [name cols] :or {name "anon"}} writer]
  (.write writer "#rel/")
  (.write writer (str name))
  (.write writer " (")
  (.write writer (str/join "," cols))
  (.write writer ")"))

(defmethod interpret-data :base
  [rel st]
  (get st rel #{}))

(defmethod col-exists? :base
  [{:keys [cols]} col]
  (some #{col} cols))

(defmethod interpret-data :project
  [{:keys [from cols]} st]
  (set/project (interpret-data from st) cols))

(defmethod col-exists? :project
  [{:keys [cols]} col]
  (some #{col} cols))

(defn project [rel cols]
  (doseq [col cols]
    (when-not (col-exists? rel col)
      (throw (Exception. (format "Projected column does not exist %s" col)))))
  ^{:type ::project} {:op :project :cols cols :from rel})

(defmethod print-method ::project
  [{:keys [from cols]} writer]
  (.write writer "#rel/project (")
  (print-method from writer)
  (.write writer ",")
  (.write writer (str/join "," cols))
  (.write writer ")"))

(defmethod interpret-data :project-away
  [{:keys [from cols]} st]
  (into #{} (map #(reduce dissoc % cols)) (interpret-data from st)))

(defmethod col-exists? :project-away
  [{:keys [from cols]} col]
  (and (col-exists? from) (not (some #{col} cols))))

(defn project-away [rel cols]
  (doseq [col cols]
    (when-not (col-exists? rel col)
      (throw (Exception. (format "Projected away column does not exist %s" col)))))
  ^{:type ::project-away} {:op :project-away :cols cols, :from rel})

(defmethod print-method ::project-away
  [{:keys [from cols]} writer]
  (.write writer "#rel/project-away (")
  (print-method from writer)
  (.write writer ",")
  (.write writer (str/join "," cols))
  (.write writer ")"))

(defrecord KW [k])
(defn kw [k] (->KW k))
(defn kw? [k] (instance? KW k))

(defn- arg->row-fn [arg]
  (cond (keyword? arg) arg
        (kw? arg) (constantly (:k arg))
        :else (constantly arg)))

(defn- extension-row-fn [[binding f & args]]
  (let [add-fn (if (coll? binding) merge #(assoc %1 binding %2))]
    (if (and (keyword? f) (empty? args))
      (fn extend-row [row] (add-fn row (f row)))
      (let [f (if (symbol? f) (resolve f) f)
            arg-fns (mapv arg->row-fn args)
            base-ret-fn (fn [row] (apply f (map (fn [f] (f row)) arg-fns)))
            ret-fn (if (coll? binding) #(select-keys (base-ret-fn %) binding) base-ret-fn)]
        (fn extend-row [row] (add-fn row (ret-fn row)))))))

(defn- extension-provision [[binding]] (if (coll? binding) binding #{binding}))
(defn- extension-deps [[binding f & args]]
  (if (and (keyword? f) (empty? args))
    (cons f (filter keyword? args))
    (filter keyword? args)))

(defmethod interpret-data :extend
  [{:keys [from extensions]} st]
  (let [row-fns (mapv extension-row-fn extensions)
        f (fn [row] (reduce (fn [row f] (f row)) row row-fns))]
    (into #{} (map f) (interpret-data from st))))

(defmethod col-exists? :extend
  [{:keys [from extensions]} col]
  (or (col-exists? from col)
      (some #{col} (mapcat extension-provision extensions))))

(defn extend [rel extensions]
  (reduce
    (fn [provision ext]
      (doseq [dep (extension-deps ext)]
        (cond
          (contains? provision dep) nil
          (col-exists? rel dep) nil
          :else (throw (Exception. (format "Extension dependency does not exist %s" dep)))))
      (into provision (extension-provision ext)))
    #{}
    extensions)
  ^{:type ::extend} {:op :extend, :extensions extensions, :from rel})

(defmethod print-method ::extend
  [{:keys [from extensions]} writer]
  (.write writer "#rel/extend (")
  (print-method from writer)
  (.write writer (str/join "," extensions))
  (.write writer ")"))

(defn- submap? [m1 m2]
  (set/subset? (set m1) (set m2)))

(defn- restrict-row-fn [restriction]
  (cond
    (map? restriction) (partial submap? restriction)

    :else
    (let [[f & args] restriction
          f (if (symbol? f) (resolve f) f)
          arg-fns (mapv arg->row-fn args)
          ret-fn (fn [row] (apply f (map (fn [f] (f row)) arg-fns)))]
      (fn restrict-row [row] (ret-fn row)))))

(defn restriction-deps [restriction]
  (if (map? restriction)
    (keys restriction)
    (let [[_ & args] restriction]
      (filter keyword? args))))

(defmethod interpret-data :restrict
  [{:keys [from restrictions]} st]
  (let [pred (apply every-pred (map restrict-row-fn restrictions))]
    (set/select pred (interpret-data from st))))

(defmethod col-exists? :restrict
  [{:keys [from]} col]
  (col-exists? from col))

(defn restrict [rel restrictions]
  (doseq [res restrictions
          dep (restriction-deps res)]
    (when-not (col-exists? rel dep)
      (throw (Exception. (format "Restriction dependency does not exist %s" dep)))))
  ^{:type ::restrict} {:op :restrict, :restrictions restrictions, :from rel})

(defmethod print-method ::restrict
  [{:keys [from restrictions]} writer]
  (.write writer "#rel/restrict (")
  (print-method from writer)
  (.write writer (str/join "," restrictions))
  (.write writer ")"))

(defmethod interpret-data :join
  [{:keys [from join clause]} st]
  (set/join (interpret-data from st) (interpret-data join st) clause))

(defmethod col-exists? :join
  [{:keys [from join]} col]
  (or (col-exists? from col) (col-exists? join col)))

(defn join [rel1 rel2 clause]
  (doseq [left-dep (keys clause)]
    (when-not (col-exists? rel1 left-dep)
      (throw (Exception. (format "Join clause dependency does not exist %s" left-dep)))))
  (doseq [right-dep (vals clause)]
    (when-not (col-exists? rel2 right-dep)
      (throw (Exception. (format "Join (right) clause dependency does not exist %s" right-dep)))))
  ^{:type ::join} {:op :join, :from rel1 :join rel2 :clause clause})

(defmethod print-method ::join
  [{:keys [from join clause]} writer]
  (.write writer "#rel/join (")
  (print-method from writer)
  (.write writer ",")
  (print-method join writer)
  (print-method clause writer)
  (.write writer ")"))

(defmethod interpret-data :union
  [{:keys [from union]} st]
  (apply set/union (interpret-data from st) (map #(interpret-data % st) union)))

(defmethod col-exists? :union
  [{:keys [from union]} col]
  (or (col-exists? from col) (some #(col-exists? % col) union)))

(defn union
  ([rel1] rel1)
  ([rel1 & more]
   ^{:type ::union} {:op :union, :from rel1, :union more}))

(defmethod print-method ::union
  [{:keys [from, union]} writer]
  (.write writer "#rel/union (")
  (print-method from writer)
  (doseq [x union]
    (.write writer ",")
    (print-method x writer))
  (.write writer ")"))

(defmethod interpret-data :difference
  [{:keys [from difference]} st]
  (set/difference (interpret-data from st) (interpret-data difference st)))

(defn difference [rel1 rel2] {:op :difference :from rel1, :difference rel2})

(defmethod col-exists? :difference
  [{:keys [from]} col]
  (col-exists? from col))

(defmethod interpret-data :intersection
  [{:keys [from intersection]} st]
  (set/intersection (interpret-data from st) (interpret-data intersection st)))

(defmethod col-exists? :intersection
  [{:keys [from]} col]
  (col-exists? from col))

(defn intersection [rel1 rel2]
  {:op :intersection, :from rel1, :intersection rel2})

(defn- arg->row-seq-fn [arg] (constantly arg))

(defn- aggregate-row-fn [[binding f & args]]
  (let [arg-fns (mapv arg->row-seq-fn args)
        f (if (symbol? f) (resolve f) f)
        base-ret-fn (fn [coll] (apply f coll (map (fn [f] (f coll)) arg-fns)))
        ret-fn (if (coll? binding) #(select-keys (base-ret-fn %) binding) base-ret-fn)
        add-fn (if (coll? binding) merge #(assoc %1 binding %2))]
    (fn aggregate-row [row coll] (add-fn row (ret-fn coll)))))

(defn- aggregate-provision [[binding]]
  (if (coll? binding) binding #{binding}))

(defmethod interpret-data :summarize
  [{:keys [from cols aggregates]} st]
  (let [agg-fns (mapv aggregate-row-fn aggregates)]
    (->> (interpret-data from st)
         (group-by #(select-keys % cols))
         (reduce-kv (fn [s row coll] (conj s (reduce (fn [row f] (f row coll)) row agg-fns))) #{}))))

(defmethod col-exists? :summarize
  [{:keys [cols aggregates]} col]
  (or (some #{col} cols)
      (some #{col} (mapcat aggregate-provision aggregates))))

(defn summarize [rel cols aggregates]
  (doseq [col cols]
    (when-not (col-exists? rel col)
      (throw (Exception. (format "Summarized over a column that does not exist %s" col)))))
  ^{:type ::summarize} {:op :summarize, :cols cols, :aggregates aggregates, :from rel})

(defmethod col-exists? :summarize
  [{:keys [cols]} col]
  (some #{col} cols))

(defmethod print-method ::summarize
  [{:keys [from cols aggregates]} writer]
  (.write writer "#rel/summarize (")
  (print-method from writer)
  (doseq [aggregate aggregates]
    (.write writer ",")
    (print-method aggregate writer))
  (.write writer ")"))

(defmethod interpret-data :expand
  [{:keys [from expansion]} st]
  (let [set1 (interpret-data from st)
        [binding f & args] expansion
        arg-fns (mapv arg->row-fn args)]
    (set (for [row set1
               ret (apply f (map (fn [f] (f row)) arg-fns))]
           (if (keyword? binding)
             (assoc row binding ret)
             (merge row (select-keys ret binding)))))))

(defmethod col-exists? :expand
  [{:keys [from expansion]} col]
  (let [[binding] expansion
        binding-provision (if (coll? binding) binding #{binding})]
    (or (col-exists? from col) (some #{col} binding-provision))))

(defn expand [rel expansion]
  (doseq [dep (extension-deps expansion)]
    (when-not (col-exists? rel dep)
      (throw (Exception. (format "Expand requires a column that does not exist %s." dep)))))
  ^{:type ::expand} {:op :expand, :from rel, :expansion expansion})

(defmethod print-method ::expand
  [{:keys [from expansion]} writer]
  (.write writer "#rel/expand (")
  (print-method from writer)
  (.write writer ",")
  (print-method expansion writer)
  (.write writer ")"))

(defn- check-integrity [st] st)

(defn modify [st commands]
  (let [set-conj (fnil conj (idx/index #{}))
        set-diff (fnil set/difference (idx/index #{}))
        set-into (fnil into (idx/index #{}))

        apply-set-map (fn [set row] (merge row set))
        rf (fn rf [st command]
             (case (nth command 0)
               :insert
               (let [[_ rel row] command]
                 (update st rel set-conj row))
               :update
               (let [[_ rel set match-rel] command
                     matches (interpret-data match-rel st)
                     new-rows (into #{} (map #(apply-set-map set %)) matches)
                     del (set/difference matches new-rows)
                     add (set/difference new-rows matches)]
                 (-> st
                     (update rel set/difference del)
                     (update rel set-into add)))
               :delete
               (let [[_ rel match-rel] command
                     matches (interpret-data match-rel st)]
                 (update st rel set-diff matches))

               (rf st (into [:insert] command))))]
    (-> (reduce rf st commands)
        (check-integrity))))

;; dogfood

(defmulti normalize :op)

(def Base (base 'Base [:base]))
(def BaseCol (expand Base [:col :cols :base]))

(defmethod normalize :base
  [{:keys [name cols] :as rel}]
  [[Base {:base rel, :name name}]])

(def Project (base 'Project [:project]))
(def ProjectCol (expand Project [:col :cols :project]))

(defmethod normalize :project
  [{:keys [from cols] :as rel}]
  (concat (normalize from)
          [[Project {:project rel}]]))

(def ProjectAway (base 'ProjectAway [:project-away]))
(def ProjectAwayCol (expand ProjectAway [:col :cols :project-away]))

(defmethod normalize :project-away
  [{:keys [from] :as rel}]
  (concat (normalize from) [[ProjectAway {:project-away rel}]]))

(def Extend (base 'Extend [:extend]))
(def Extension (expand Extend [:extension :extensions :extend]))
(def ExtensionDep (expand Extension [:col extension-deps :extension]))
(def ExtensionProvided (expand Extension [:provided extension-provision :extension]))

(defmethod normalize :extend
  [{:keys [from] :as rel}]
  (concat (normalize from) [[Extend {:extend rel}]]))

(def Restrict (base 'Restrict [:restrict :from]))
(def Restriction (expand Restrict [:restriction :restrictions :restrict]))
(def RestrictionDep (expand Restriction [:col restriction-deps :restriction]))

(defmethod normalize :restrict
  [{:keys [from] :as rel}]
  (concat (normalize from) [[Restrict {:restrict rel}]]))

(def Join (base 'Join [:join]))

(defmethod normalize :join
  [{:keys [from join] :as rel}]
  (concat (normalize from)
          (normalize join)
          [[Join {:join rel}]]))

(def Expand (base 'Expand [:expand :from :expansion]))

(defmethod normalize :expand
  [{:keys [from] :as rel}]
  (concat (normalize from) [[Expand {:expand rel}]]))

(def Summarize (base 'Summarize [:summarize]))

(defmethod normalize :summarize
  [{:keys [from] :as rel}]
  (concat (normalize from) [[Summarize {:summarize rel}]]))

(defn- project-rename [rel renames]
  (-> (extend rel (for [[k1 k2] renames] [k2 k1]))
      (project (vals renames))))

(def Rel
  (-> (project-rename Base {:base :rel})
      (union
        (project-rename Project {:project :rel})
        (project-rename ProjectAway {:project-away :rel})
        (project-rename Join {:join :rel})
        (project-rename Extend {:extend :rel})
        (project-rename Restrict {:restrict :rel})
        (project-rename Expand {:expand :rel})
        (project-rename Summarize {:summarize :rel}))))

(def Index (base 'Index [:index :rel :type]))
(def IndexCol (expand Index [:col :cols :index]))

(def RestrictionDepInfo (join Restriction RestrictionDep {:restriction :restriction}))
(def RestrictionIndexCol (join RestrictionDepInfo IndexCol {:col :col, :restriction :rel}))
(def RestrictionIndex (summarize RestrictionIndexCol [:restriction :index] [[:col-hits count]]))

(defn- best-index [rows]
  (:index (last (sort-by :col-hits rows))))

(def RestrictionBestIndex
  (summarize RestrictionIndex [:restriction] [[:best-index best-index]]))

(defn qp [st rel]
  (clojure.pprint/print-table (q st rel)))