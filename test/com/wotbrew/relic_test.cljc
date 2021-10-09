(ns com.wotbrew.relic-test
  (:require [clojure.test :refer :all])
  (:require [com.wotbrew.relic :as r]))

(deftest interpret-test
  (let [a [[:state :A]]
        b [[:state :B]]
        st (r/state
             {a #{{:a 42}, {:a 43}, {:a 45}}
              b #{{:a 42, :b 42}, {:a 42, :b 43}, {:a 43, :b 44}}})]
    (is (= #{} (r/interpret (r/empty-state) [])))
    (are [x ret] (= ret (r/interpret st x))

      [[:coll #{{:c 0}}]]
      ;; =>
      #{{:c 0}}

      [[:from a]
       [:coll #{{:c 0}}]
       [:extend [:c+1 :<- [inc :c]]]]
      ;; =>
      #{{:c 0, :c+1 1}}

      a
      ;; =>
      #{{:a 42}, {:a 43}, {:a 45}}

      [[:from a]]
      ;; =>
      #{{:a 42}, {:a 43}, {:a 45}}

      [[:from a] [:where [even? :a]]]
      ;; =>
      #{{:a 42}}

      [[:from a]
       [:where [even? :a]]
       [:extend [:b :<- [inc :a]]]]
      ;; =>
      #{{:a 42, :b 43}}

      [[:from a]
       [:where [even? :a]]
       [:extend [:b :<- (r/esc [:a])]]]
      ;; =>
      #{{:a 42, :b [:a]}}

      [[:from a]
       [:where [even? :a]]
       [:expand [:b :<- [range :a [+ :a 2]]]]]
      ;; =>
      #{{:a 42 :b 42} {:a 42, :b 43}}

      [[:from a]
       [:extend [:b :<- [inc :a]]]
       [:project [:b]]]
      ;; =>
      #{{:b 43} {:b 44}, {:b 46}}

      [[:from a]
       [:extend [:b :<- [inc :a]]]
       [:project-away [:a]]]
      ;; =>
      #{{:b 43} {:b 44}, {:b 46}}

      [[:from a] [:union b]]
      ;; =>
      #{{:a 42} {:a 43} {:a 45} {:a 42, :b 42} {:a 42, :b 43} {:a 43, :b 44}}

      [[:from a] [:join b {:a :a}]]
      ;; =>
      #{{:a 42, :b 42} {:a 42, :b 43} {:a 43, :b 44}}

      [[:from a] [:left-join b {:a :a}]]
      ;; =>
      #{{:a 42, :b 42} {:a 42, :b 43} {:a 43, :b 44} {:a 45}}


      [[:from a] [:agg [] [:n :<- count]]]
      ;; =>
      #{{:n 3}}

      [[:from a] [:join b {:a :a}] [:agg [:a] [:n :<- count]]]
      ;; =>
      #{{:a 42, :n 2} {:a 43, :n 1}}


      [[:from a] [:join b {:a :a}]
       [:agg [:a] [:n :<- count]]
       [:select [:x :<- [+ :a :n]]]]
      ;; =>
      #{{:x 44}}


      [[:from a] [:hash-unique :a]]
      ;; =>
      {42 {:a 42} 43 {:a 43} 45 {:a 45}}

      [[:from a] [:hash [even? :a]]]
      ;; =>
      {true #{{:a 42}} false #{{:a 43} {:a 45}}}

      [[:from a] [:hash [even? :a]] [:where some?]]
      ;; =>
      #{{:a 42}, {:a 43}, {:a 45}}

      [[:from a] [:hash-unique :a] [:where some?]]
      ;; =>
      #{{:a 42}, {:a 43}, {:a 45}}


      )))

(def Customer [[:state :Customer]])
(def c1 {:id 1})
(def c2 {:id 2})
(def c3 {:id 3})
(def customers [c1 c2 c3])

(def Order [[:state :Order]])
(def o1 {:customer 1, :date 1, :value 1})
(def o2 {:customer 1, :date 2, :value 2})
(def o3 {:customer 1, :date 3, :value 3})
(def o4 {:customer 2, :date 1, :value 4})
(def o5 {:customer 2, :date 2, :value 5})
(def o6 {:customer 2, :date 3, :value 6})
(def o7 {:customer 2, :date 4, :value 7})
(def o8 {:customer 2, :date 5, :value 8})
(def orders [o1 o2 o3 o4 o5 o6 o7 o8])

(def CustomerOrder
  [[:from Customer]
   [:join Order {:id :customer}]])

(defn- sum [coll] (reduce + 0 coll))

(def CustomerOrderStats
  [[:from Customer]
   [:left-join Order {:id :customer}]
   [:agg [:id]
    [:order-count :<- [count :date]]
    [:spend :<- [sum :value]]]
   [:extend [:aov :<- [bigdec [/ :spend [max 1 :order-count]]]]]])

(def MondayOrders
  [[:from Order]
   [:where [= 1 :date]]])

(def TuesdayOrders
  [[:from Order]
   [:where [= 2 :date]]])

(def EarlyWeekOrders
  [[:from MondayOrders]
   [:union TuesdayOrders]
   [:project [:customer :date]]])

(def OrderInfo
  [[:from Order]
   [:extend [:value-class :<- [#(condp <= %
                                  5 :high
                                  3 :med
                                  :low) :value]]]])

(def HighValueOrder
  [[:from OrderInfo]
   [:where [= (r/esc :high) :value-class]]])

(defn co-state
  []
  (binding [r/*warn-on-naive-materialization* true]
    (r/state
      {Customer customers
       Order orders}
      {:materialize [CustomerOrder
                     CustomerOrderStats]})))

(def cq (partial r/query* (co-state)))

(deftest co-mat-tests
  (let [co-state (co-state)]
    (binding [r/*warn-on-interpret* false]

      (is (= {1 {:id 1}
              2 {:id 2}
              3 {:id 3}}
             (r/query* co-state (conj Customer [:hash-unique :id]))))

      (is (contains? (meta co-state) CustomerOrderStats))

      (is (= #{{:id 3, :order-count 0, :spend 0, :aov 0M}
               {:id 1, :order-count 3, :spend 6, :aov 2M}
               {:id 2, :order-count 5, :spend 30, :aov 6M}} (cq CustomerOrderStats)))

      (is (= #{{:customer 1, :date 1}, {:customer 1 :date 2}} (cq [[:from EarlyWeekOrders] [:where [= :customer 1]]])))

      (is
        (= #{{:id 3, :order-count 0, :spend 0, :aov 0M}
             {:id 1, :order-count 4, :spend 9, :aov 2.25M}
             {:id 2, :order-count 5, :spend 30, :aov 6M}}
           (r/query*
             (r/transact co-state {Order [{:customer 1, :date 10, :value 3}]})
             CustomerOrderStats)))

      (is
        (= #{{:id 3, :order-count 0, :spend 0, :aov 0M}
             {:id 1, :order-count 3, :spend 6, :aov 2M}
             {:id 2, :order-count 5, :spend 60, :aov 12M}}
           (r/query*
             (r/transact co-state [:update Order {:value [* :value 2]} [:where [= :customer 2]]])
             CustomerOrderStats)))

      (is
        (= #{{:id 3, :order-count 0, :spend 0, :aov 0M}
             {:id 1, :order-count 3, :spend 6, :aov 2M}
             {:id 2, :order-count 5, :spend 30, :aov 6M}}
           (r/query*
             (r/transact co-state [:update Order {:value [* :value 2]} [:where [= :customer 42]]])
             CustomerOrderStats)))

      (is
        (= #{{:id 3, :order-count 0, :spend 0, :aov 0M}
             {:id 1, :order-count 3, :spend 12, :aov 4M}
             {:id 2, :order-count 5, :spend 60, :aov 12M}}
           (r/query*
             (r/transact co-state [:update Order {:value [* :value 2]}])
             CustomerOrderStats)))


      (is
        (= #{{:id 3, :order-count 0, :spend 0, :aov 0M}
             {:id 1, :order-count 4, :spend 9, :aov 2.25M}}
           (r/query*
             (r/transact co-state {Order [{:customer 1, :date 10, :value 3}]} [:delete Customer {:id 2}])
             CustomerOrderStats)))

      (is
        (= #{{:id 3, :order-count 0, :spend 0, :aov 0M}
             {:id 1, :order-count 3, :spend 6, :aov 2M}}
           (r/query*
             (r/transact co-state {Order [{:customer 2, :date 10, :value 3}]} [:delete Customer {:id 2}])
             CustomerOrderStats))))))