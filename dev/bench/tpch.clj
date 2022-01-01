(ns bench.tpch
  (:require [com.wotbrew.relic :as rel]
            [clojure.instant])
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
            TpchColumnType$Base/DOUBLE
            (.getDouble c b)
            TpchColumnType$Base/DATE
            (clojure.instant/read-instant-date
              (GenerateUtils/formatDate (.getDate c b))))])
       (into {})))

(defn- generate-maps
  [^TpchTable t scale-factor]
  (map (partial tpch-map t) (seq (.createGenerator ^TpchTable t scale-factor 1 1))))

(defn gen-tx
  [scale-factor]
  (for [^TpchTable table (TpchTable/getTables)]
    (into [:insert (keyword (.getTableName table))] (generate-maps table scale-factor))))

(def q1
  "Pricing summary report query"
  [[:from :lineitem]
   [:where [<= [compare :l_shipdate #inst "1998-09-02"] 0]]
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
    [:count_order count]]])

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
  "Minimum cost supplier query"
  [[:from :part]

   [:where
    [= :p_size 15]
    [re-find #"^.*BRASS$" :p_type]]

   [:join
    LowestCostSupplierInEurope
    {:p_partkey :ps_partkey}

    :supplier {:ps_suppkey :s_suppkey}
    :nation {:s_nationkey :n_nationkey}
    :region {:r_regionkey :r_regionkey}]

   [:select
    :s_acctbal
    :s_name
    :n_name
    :p_partkey
    :p_mfgr
    :s_address
    :s_phone
    :s_comment]])

(def q3
  "Shipping priority query"
  [[:from :customer]
   [:where [= "BUILDING" :c_mktsegment]]
   [:join :orders {:c_custkey :o_custkey}]
   [:where [< [compare :o_orderdate #inst "1995-03-15"] 0]]
   [:join :lineitem {:o_orderkey :l_orderkey}]
   [:where [> [compare :l_shipdate #inst "1995-03-15"]] 0]
   [:agg
    [:l_orderkey :o_orderdate :o_shippriority]
    [:revenue [rel/sum [* :l_extendedprice [- 1 :l_discount]]]]]
   [:agg [] [:top [rel/top-by 10 :revenue]]]
   [:expand [:* :top]]
   [:without :top]])

(def q4
  "Order priority query"
  [[:from :orders]
   [:where
    [>= [compare :o_orderdate #inst "1993-07-01"] 0]
    [< [compare :o_orderdate #inst "1993-10-01"] 0]
    [rel/sel1
     [[:from :lineitem]
      [:where [< [compare :l_commitdate :l_receiptdate] 0]]]
     {:o_orderkey :l_orderkey}]]
   [:agg [:o_orderpriority] [:order_count count]]])

(defn db
  [scale-factor]
  (->
    {}
    (rel/materialize
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
        [:fk :orders {:l_orderkey :o_orderkey}]]])
    (rel/transact (gen-tx scale-factor))))