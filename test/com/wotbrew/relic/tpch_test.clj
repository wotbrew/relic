(ns com.wotbrew.relic.tpch-test
  (:require [clojure.test :refer [deftest is testing]]
            [com.wotbrew.relic :as rel]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [dev]
            [clojure.instant :as inst]
            [com.wotbrew.relic.randomizer :as rand])
  (:import (io.airlift.tpch GenerateUtils TpchColumn TpchColumnType$Base TpchEntity TpchTable)))

(defn tpch-map [^TpchTable t ^TpchEntity b]
  (->> (for [^TpchColumn c (.getColumns t)]
         [(keyword (.getColumnName c))
          (condp = (.getBase (.getType c))
            TpchColumnType$Base/IDENTIFIER
            (.getIdentifier c b)
            TpchColumnType$Base/INTEGER
            (.getInteger c b)
            TpchColumnType$Base/VARCHAR
            (.getString c b)
            TpchColumnType$Base/DOUBLE (.getDouble c b)
            TpchColumnType$Base/DATE
            (clojure.instant/read-instant-date
              (GenerateUtils/formatDate (.getDate c b))))])
       (into {})))

(defn- generate-maps
  [^TpchTable t scale-factor]
  (map (partial tpch-map t) (seq (.createGenerator ^TpchTable t scale-factor 1 1))))

(defn gen-tx
  [scale-factor]
  (->> (for [^TpchTable table (TpchTable/getTables)]
         [(keyword (.getTableName table)) (generate-maps table scale-factor)])
       (into {})))

(defn db
  [scale-factor]
  (rel/transact {} (gen-tx scale-factor)))

(defn parse-result-file [in]
  (with-open [rdr (io/reader in)]
    (vec (for [s (rest (line-seq rdr))]
           (str/split s #"\|")))))

(defn check-answer [db qry]
  (let [{:keys [desc cols result q]} qry
        rs (rel/q db q)]
    (testing desc
      (->> "row counts should match"
           (is (= (count result) (count rs))))
      (doseq [[i row] (map-indexed vector rs)
              :let [csv-row (zipmap cols (nth result i nil))]]

        (doseq [col cols
                :let [relv (col row)
                      tpch (col csv-row)
                      parsed (try
                               (cond
                                 (int? relv) (Long/parseLong tpch)
                                 (double? relv) (Double/parseDouble tpch)
                                 (inst? relv) (inst/read-instant-date tpch)
                                 :else tpch)
                               (catch Throwable e
                                 tpch))]]

          (cond
            (and (some? relv) (nil? parsed))
            (is (= parsed relv) (str col ", row " i))
            (double? relv)
            (let [err 0.01
                  diff (Math/abs (double (- relv parsed)))]
              (when-not (<= diff err)
                (is (= parsed relv) (str col ", row " i))))
            :else (is (= parsed relv) (str col ", row " i))))))))

(def q1
  {:desc "Q1: Pricing summary report query"

   :q
   [[:from :lineitem]
    [:where [<= :l_shipdate #inst "1998-09-02"]]
    [:agg
     [:l_returnflag
      :l_linestatus]
     [:sum_qty [rel/sum :l_quantity]]
     [:sum_base_price [rel/sum :l_extendedprice]]
     [:sum_disc_price  [rel/sum [* :l_extendedprice [- 1 :l_discount]]]]
     [:sum_charge [rel/sum [* :l_extendedprice [- 1 :l_discount] [+ 1 :l_tax]]]]
     [:avg_qty [rel/avg :l_quantity]]
     [:avg_price [rel/avg :l_extendedprice]]
     [:avg_disc [rel/avg :l_discount]]
     [:count_order count]]
    [:sort [:l_returnflag] [:l_linestatus]]]

   :cols
   [:l_returnflag
    :l_linestatus
    :sum_qty
    :sum_base_price
    :sum_disc_price
    :sum_charge
    :avg_qty
    :avg_price
    :avg_disc
    :count_order]

   :result (parse-result-file (io/resource "io/airlift/tpch/queries/q1.result"))})

(def LowestCostSupplierInEurope
  [[:from :partsupp]
   [:join
    :supplier {:ps_suppkey :s_suppkey}
    :nation {:s_nationkey :n_nationkey}
    :region {:n_regionkey :r_regionkey}]
   [:where [= "EUROPE" :r_name]]
   [:agg
    [:ps_partkey :r_regionkey]
    [:min_supplier [rel/min-by :ps_supplycost]]]
   [:select
    :ps_partkey
    :r_regionkey
    [[:ps_suppkey] :min_supplier]]])

(def q2
  {:desc "Q2: Minimum cost supplier query"
   :q
   [[:from :part]

    [:where
     [= :p_size 15]
     [re-find #"^.*BRASS$" :p_type]]

    [:join
     LowestCostSupplierInEurope
     {:p_partkey :ps_partkey}

     :supplier {:ps_suppkey :s_suppkey}
     :nation {:s_nationkey :n_nationkey}
     :region {:n_regionkey :r_regionkey}]

    [:select
     :s_acctbal
     :s_name
     :n_name
     :p_partkey
     :p_mfgr
     :s_address
     :s_phone
     :s_comment]
    [:sort

     [:s_acctbal :desc]
     [:n_name]
     [:s_name]
     [:p_partkey]]]

   :cols
   [:s_acctbal
    :s_name
    :n_name
    :p_partkey
    :p_mfgr
    :s_address
    :s_phone
    :s_comment]

   :result (parse-result-file (io/resource "io/airlift/tpch/queries/q2.result"))})

(def q3
  {:desc "Q3: Shipping priority query"
   :q
   [[:from :customer]
    [:where [= "BUILDING" :c_mktsegment]]
    [:join :orders {:c_custkey :o_custkey}]
    [:where [< :o_orderdate #inst "1995-03-15"]]
    [:join :lineitem {:o_orderkey :l_orderkey}]
    [:where [> :l_shipdate #inst "1995-03-15"]]
    [:agg
     [:l_orderkey :o_orderdate :o_shippriority]
     [:revenue [rel/sum [* :l_extendedprice [- 1 :l_discount]]]]]
    [:sort-limit 10 [:revenue :desc] [:o_orderdate]]]

   :cols [:l_orderkey :revenue :o_orderdate :o_shippriority]

   :result (parse-result-file (io/resource "io/airlift/tpch/queries/q3.result"))})

(def q4
  {:desc "Q4: Order priority query"
   :q
   [[:from :orders]
    [:where
     [>= :o_orderdate #inst "1993-07-01"]
     [< :o_orderdate #inst "1993-10-01"]
     [rel/sel1
      [[:from :lineitem]
       [:where [< :l_commitdate :l_receiptdate]]]
      {:o_orderkey :l_orderkey}]]
    [:agg [:o_orderpriority] [:order_count count]]
    [:sort [:o_orderpriority]]]

   :cols [:o_orderpriority :order_count]

   :result (parse-result-file (io/resource "io/airlift/tpch/queries/q4.result"))})

(def q5
  {:desc "Q5"
   :q
   [[:from :orders]
    [:where
     [>= :o_orderdate #inst "1994-01-01"]
     [< :o_orderdate #inst "1995-01-01"]]
    [:join
     :customer {:o_custkey :c_custkey}
     :lineitem {:o_orderkey :l_orderkey}
     :supplier {:l_suppkey :s_suppkey
                :c_nationkey :s_nationkey}
     :nation {:s_nationkey :n_nationkey}
     :region {:n_regionkey :r_regionkey}]
    [:where
     [= :r_name "ASIA"]]
    [:agg [:n_name]
     [:revenue [rel/sum [* :l_extendedprice [- 1 :l_discount]]]]]
    [:sort [:revenue :desc]]]

   :cols [:n_name :revenue]

   :result (parse-result-file (io/resource "io/airlift/tpch/queries/q5.result"))})

(def q6
  {:desc "Q6"
   :q
   [[:from :lineitem]
    [:where
     [>= :l_shipdate #inst "1994-01-01"]
     [< :l_shipdate #inst "1995-01-01"]
     [>= :l_discount 0.05]
     [<= :l_discount 0.07]
     [< :l_quantity 24]]
    [:agg [] [:revenue [rel/sum [* :l_extendedprice :l_discount]]]]]
   :cols [:revenue]
   :result (parse-result-file (io/resource "io/airlift/tpch/queries/q6.result"))})

(def q7
  {:desc "q7"
   :q
   (let [Shipping
         [[:from :lineitem]

          [:where
           [>= :l_shipdate #inst "1995-01-01"]
           [<= :l_shipdate #inst "1996-12-31"]]

          [:join

           :supplier {:l_suppkey :s_suppkey}
           :orders {:l_orderkey :o_orderkey}
           :customer {:o_custkey :c_custkey}]

          [:extend

           [:n1 [rel/sel1 :nation {:s_nationkey :n_nationkey}]]
           [:n2 [rel/sel1 :nation {:c_nationkey :n_nationkey}]]]

          [:where
           [:or
            [:and
             [= [:n_name :n1] "FRANCE"]
             [= [:n_name :n2] "GERMANY"]]
            [:and
             [= [:n_name :n1] "GERMANY"]
             [= [:n_name :n2] "FRANCE"]]]]

          [:select

           [:supp_nation [:n_name :n1]]
           [:cust_nation [:n_name :n2]]
           [:l_year [+ 1900 [#(.getYear %) :l_shipdate]]]
           [:volume [* :l_extendedprice [- 1 :l_discount]]]]]]
     [[:from Shipping]
      [:agg [:supp_nation
             :cust_nation
             :l_year]
       [:revenue [rel/sum :volume]]]
      [:sort [:supp_nation] [:cust_nation] [:l_year]]])

   :cols [:supp_nation
          :cust_nation
          :l_year
          :revenue]

   :result (parse-result-file (io/resource "io/airlift/tpch/queries/q7.result"))})

(defn- bench [db sf qry]
  (let [q (:q qry)]
    (println "Bench sf" sf (:desc qry))
    (dev/bench (rel/q db q))))

(defn- qbench [db sf qry]
  (let [q (:q qry)]
    (print "Time sf" sf (:desc qry) "| ")
    (time (do (rel/q db q) nil))))

(def model
  {:hints
   [
    ;; indexes
    [[:from :lineitem] [:btree :l_shipdate]]
    [[:from :orders] [:btree :o_orderdate]]
    [[:from :nation] [:unique :n_nationkey]]

    [[:from :customer] [:hash :c_mktsegment]]
    [[:from :part] [:hash :p_size :p_type]]]

   :fks
   [
    ;; fks
    [[:from :partsupp]
     [:constrain
      [:fk :part {:ps_partkey :p_partkey}]
      [:fk :supplier {:ps_suppkey :s_suppkey}]]]

    [[:from :supplier]
     [:constrain
      [:fk :nation {:s_nationkey :n_nationkey}]]]

    [[:from :nation]
     [:constrain
      [:fk :region {:n_regionkey :r_regionkey}]]]

    [[:from :orders]
     [:constrain
      [:fk :customer {:o_custkey :c_custkey}]]]

    [[:from :lineitem]
     [:constrain
      [:fk :orders {:l_orderkey :o_orderkey}]]]]


   :queries
   [q1
    q2
    q3
    q4
    q5
    q6
    q7]})

(deftest tpch-test
  (let [d (db 0.01)]
    (doseq [q (:queries model)]
      (check-answer d q))))

(def mut-iterations 1)

(deftest mutdb-test
  (dotimes [x mut-iterations]
    (let [tx (gen-tx 0.01)
          d (rand/mutdb model 100 tx)]
      (doseq [[table data] tx]
        (->> (str "table row counts should match" table)
             (is (= (count data) (count (rel/q d table))))))
      (doseq [q (:queries model)]
        (check-answer d q)))))

(defn bench-all [& {:keys [sf quick]}]
  (doseq [sf (if sf
               [sf]
               [0.01
                0.05
                0.15])]
    (println "Benching for scale factor (sf)" sf)
    (print "Creating database sf" sf "| ")
    (let [d (time (db sf))
          _ (println "Benching" (count (:queries model)) "cases")]
      (doseq [q (:queries model)]
        (if quick
          (qbench d sf q)
          (bench d sf q))))))