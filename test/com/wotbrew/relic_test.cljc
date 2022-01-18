(ns com.wotbrew.relic-test
  (:require [clojure.test :refer [deftest is are testing]]
            [com.wotbrew.relic :as rel]
            [com.wotbrew.relic.impl.relvar :as r]
            [com.wotbrew.relic.impl.dataflow :as dataflow]
            [com.wotbrew.relic.impl.expr :as e]))

(defn- result-set [node] (some-> node :result-set deref))

(deftest basics-test
  (let [a [[:from :A]]
        b [[:from :B]]
        db (rel/transact {} {:A #{{:a 42}, {:a 43}, {:a 45}}
                             :B #{{:a 42, :b 42}, {:a 42, :b 43}, {:a 43, :b 44}}})]
    (are [x ret]

      (and (= (set ret)
              (set (rel/q db x))
              (set (rel/q (rel/mat db x) x))
              (set (rel/q (rel/demat (rel/mat db x) x) x))))

      a
      ;; =>
      #{{:a 42}, {:a 43}, {:a 45}}

      [[:from a]]
      ;; =>
      #{{:a 42}, {:a 43}, {:a 45}}

      [[:from a] [:where [even? :a]]]
      ;; =>
      [{:a 42}]

      [[:from a]
       [:where [even? :a]]
       [:extend [:b [inc :a]]]]
      ;; =>
      #{{:a 42, :b 43}}

      [[:from a]
       [:where [even? :a]]
       [:extend [:b [::rel/esc [:a]]]]]
      ;; =>
      #{{:a 42, :b [:a]}}

      [[:from a]
       [:where [even? :a]]
       [:expand [:b [range :a [+ :a 2]]]]]
      ;; =>
      #{{:a 42 :b 42} {:a 42, :b 43}}

      [[:from a]
       [:extend [:b [inc :a]]]
       [:select :b]]
      ;; =>
      #{{:b 43} {:b 44}, {:b 46}}

      [[:from a]
       [:extend [:b [inc :a]]]
       [:without :a]]
      ;; =>
      #{{:b 43} {:b 44}, {:b 46}}

      [[:from a] [:union b]]
      ;; =>
      #{{:a 42} {:a 43} {:a 45} {:a 42, :b 42} {:a 42, :b 43} {:a 43, :b 44}}

      [[:union a b]]
      ;; =>
      #{{:a 42} {:a 43} {:a 45} {:a 42, :b 42} {:a 42, :b 43} {:a 43, :b 44}}

      [[:union b a]]
      ;; =>
      #{{:a 42} {:a 43} {:a 45} {:a 42, :b 42} {:a 42, :b 43} {:a 43, :b 44}}

      [[:from b] [:union a]]
      ;; =>
      #{{:a 42} {:a 43} {:a 45} {:a 42, :b 42} {:a 42, :b 43} {:a 43, :b 44}}

      [[:from a] [:join b {:a :a}]]
      ;; =>
      #{{:a 42, :b 42} {:a 42, :b 43} {:a 43, :b 44}}

      [[:from a] [:left-join b {:a :a}]]
      ;; =>
      #{{:a 42, :b 42} {:a 42, :b 43} {:a 43, :b 44} {:a 45}}


      [[:from a] [:agg [] [:n count]]]
      ;; =>
      #{{:n 3}}

      [[:from a] [:join b {:a :a}] [:agg [:a] [:n count]]]
      ;; =>
      #{{:a 42, :n 2} {:a 43, :n 1}}

      [[:from a] [:join b {:a :a}] [:agg [] [:s [rel/sum :a :b]]]]
      ;; =>
      #{{:s 256}}

      [[:from a] [:join b {:a :a}]
       [:agg [:a] [:n count]]
       [:select [:x [+ :a :n]]]]
      ;; =>
      #{{:x 44}}

      [[:from a]
       [:join [[:const [{:b 1}]]]]]
      ;;=>
      #{{:a 42, :b 1}, {:a 43, :b 1}, {:a 45, :b 1}}

      [[:const [{:a 42}, {:b 43}]]]
      ;;=>
      #{{:a 42} {:b 43}}

      [[:const [{:a {:a 42, :b 42}}]]
       [:extend [::rel/* :a]]
       [:select :a :b]]
      ;; =>
      #{{:a 42, :b 42}}

      [[:const [{:a 42}]]
       [:intersection [[:const [{:a 42} {:b 1}]]]]]
      ;; =>
      #{{:a 42}}

      [[:intersection [[:const [{:a 42}]]] [[:const [{:a 42} {:b 1}]]]]]
      ;; =>
      #{{:a 42}}

      [[:const [{:a 42}]]
       [:union [[:const [{:a 42} {:b 1}]]]]]
      ;; =>
      #{{:a 42} {:b 1}}

      [[:const [{:a 42}]]
       [:difference [[:const [{:a 42} {:b 1}]]]]]
      ;; =>
      #{}

      [[:const [{:a 42}]]
       [:difference [[:const [{:a 43} {:b 1}]]]]]
      ;; =>
      #{{:a 42}}

      [[:difference [[:const [{:a 42}]]] [[:const [{:a 43} {:b 1}]]]]]
      ;; =>
      #{{:a 42}})))

(deftest const-test
  (is (= [{:a 42}] (rel/q {} [[:const #{{:a 42}}]])))
  (is (= [{:a 42}] (rel/q {} [[:const [{:a 42}]]]))))

(deftest base-relvar-test
  (let [A [[:table :A]]]
    (is (r/table-relvar? A))
    (is (not (r/table-relvar? [])))
    (is (not (r/table-relvar? (conj A [:where [= 1 1]]))))))

(deftest state-statement-test
  (let [A [[:from :A]]
        db {}
        a0 {:a 42}
        a1 {:a 43}
        a2 {:a 44}]
    (is (= nil (rel/q db A)))
    (is (= db (rel/mat db A)))
    (is (= {:A #{}} (rel/transact db {A []})))
    (is (= [a0] (rel/what-if db A {A [a0]})))
    (is (= {:A #{a0}} (rel/transact db [:insert A a0])))
    (is (= {:A #{}} (rel/transact db [:insert A a0] [:delete A a0])))
    (is (= {} (rel/transact db [:delete A a0])))
    (is (= [a0] (rel/what-if db (vec (concat A A)) {A [a0]})))
    (is (= #{a0 a1 a2} (set (rel/what-if db A {A [a0 a1 a2 a0 a1]}))))
    (is (= [a2] (rel/what-if db A {A [a2 a2 a2]})))
    (is (= [a2] (rel/what-if db A {A [a0 a1]} {A [a2]} [:delete-exact A a0 a1])))))

(deftest from-statement-test
  (let [A [[:from :A]]
        F [[:from A]]
        a0 {:a 42}
        a1 {:a 43}
        a2 {:a 44}
        db {}]
    (is (= nil (rel/q db F)))
    (is (= db (rel/mat db F)))
    (is (= [a0] (rel/what-if db F {A [a0]})))
    (is (= [a0] (rel/what-if db (vec (concat A F)) {A [a0]})))
    (is (= #{a0 a1 a2} (set (rel/what-if db F {A [a0 a1 a2 a0 a1]}))))
    (is (= [a2] (rel/what-if db F {A [a2 a2 a2]})))
    (is (= [a2] (rel/what-if db F {A [a0 a1]} {A [a2]} [:delete-exact A a0 a1])))))

(deftest where-expr-test
  (are [result row expr]
    (if result
      (= #{row} (set (rel/what-if {} [[:from :A] [:where expr]] {[[:from :A]] [row]})))
      (empty? (rel/what-if {} [[:from :A] [:where expr]] {[[:from :A]] [row]})))

    true {} (constantly true)
    true {} [= 1 1]
    true {:a 42} [= 1 1]
    true {:a 42} :a
    true {:a 42} [= 42 :a]
    false {:a 42} [= 43 :a]
    false {} (constantly false)
    false {} :b
    false {:a 43} [even? :a]
    true {:a 43} [odd? :a]
    true {:a 43} [even? [+ :a 1]]
    true {:a :b} [= [::rel/esc :b] :a]
    true {:a 42, :b 42} [= :a :b]
    false {:a 43, :b 42} [= :a :b]
    true {:a 43, :b 42} [= [inc :a] [+ :b 2]]))

(deftest agg-test
  (let [A [[:from :A]]
        R [[:from A] [:agg [] [:n [rel/sum :b]]]]
        aid (volatile! 0)
        a (fn [b] {:a (vswap! aid inc), :b b})

        a0 (a 1)
        a1 (a 1)
        a2 (a 1)

        db (rel/mat {} R)]

    (is (= nil (rel/q db R)))
    (is (= [{:n 3}] (rel/what-if db R {A [a0 a1 a2]})))
    (is (= [{:n 2}] (rel/what-if db R {A [a0 a1 a2]} [:delete-exact A a0])))
    (is (= [{:n 1}] (rel/what-if db R {A [a1]} [:delete-exact A a1] [:insert A a1])))
    (is (= [{:n 2}] (rel/what-if db R {A [a1, a2]} [:delete-exact A a1] [:insert A a1])))))

(deftest join-test
  (let [A [[:from :A]]
        B [[:from :B]]
        R [[:from A] [:join B {:a :a}]]
        aid (volatile! -1)
        a (fn [x] {:a (vswap! aid inc), :x x})
        b (fn [a] {:b -1, :a a})

        a0 (a 0)
        a1 (a 1)
        a2 (a 2)

        b0 (b 0)
        b1 (b 1)
        b2 (b 2)

        ab0 (merge a0 b0)
        ab1 (merge a1 b1)
        ab2 (merge a2 b2)

        db (rel/mat {} R)]

    (is (= nil (rel/q db R)))
    (is (= nil (rel/what-if db R {A [a0]})))
    (is (= nil (rel/what-if db R {B [b0]})))
    (is (= [ab0] (rel/what-if {} R [:insert A a0], [:insert B b0])))
    (is (= [ab0] (rel/what-if db R [:insert A a0], [:insert B b0])))
    (is (= [ab0] (rel/what-if db R [:insert B b0], [:insert A a0])))
    (is (= [ab0] (rel/what-if db R {A [a0] B [b0]})))
    (is (= [ab0] (rel/what-if db R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= [ab0] (rel/what-if db R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))))

(deftest join-product-test
  (let [A [[:from :A]]
        B [[:from :B]]
        R [[:from A] [:join B]]
        aid (volatile! -1)
        a (fn [x] {:a (vswap! aid inc), :x x})
        b (fn [a] {:b -1, :a a})

        a0 (a 0)
        a1 (a 1)
        a2 (a 2)

        b0 (b 0)
        b1 (b 1)
        b2 (b 2)

        ab0 (merge a0 b0)
        ab1 (merge a1 b1)
        ab2 (merge a2 b2)

        db (rel/mat {} R)]

    (is (= nil (rel/q db R)))
    (is (= nil (rel/what-if db R {A [a0]})))
    (is (= nil (rel/what-if db R {B [b0]})))
    (is (= [ab0] (rel/what-if db R [:insert A a0], [:insert B b0])))
    (is (= [ab0] (rel/what-if db R [:insert B b0], [:insert A a0])))
    (is (= [ab0] (rel/what-if db R {A [a0] B [b0]})))
    (is (= [ab0] (rel/what-if db R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= [ab0] (rel/what-if db R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))
    (is (= #{ab0, (merge a0 b1)} (set (rel/what-if db R {A [a0] B [b0, b1]}))))
    (is (= #{(merge a0, b0), (merge a0 b1), (merge a1, b0), (merge a1 b1)} (set (rel/what-if db R {A [a0, a1] B [b0, b1]}))))))

(deftest left-join-test
  (let [A [[:from :A]]
        B [[:from :B]]
        R [[:from A] [:left-join B {:a :a}]]
        aid (volatile! -1)
        a (fn [x] {:a (vswap! aid inc), :x x})
        b (fn [a] {:b -1, :a a})

        a0 (a 0)
        a1 (a 1)
        a2 (a 2)

        b0 (b 0)
        b1 (b 1)
        b2 (b 2)

        ab0 (merge a0 b0)
        ab1 (merge a1 b1)
        ab2 (merge a2 b2)

        db (rel/mat {} R)]

    (is (= nil (rel/q db R)))
    (is (= [a0] (rel/what-if db R {A [a0]})))
    (is (= nil (rel/what-if db R {B [b0]})))
    (is (= [ab0] (rel/what-if db R [:insert A a0], [:insert B b0])))
    (is (= [ab0] (rel/what-if db R [:insert B b0], [:insert A a0])))
    (is (= [ab0] (rel/what-if db R {A [a0] B [b0]})))
    (is (= [ab0] (rel/what-if db R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= [ab0] (rel/what-if db R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))
    (is (= [a0] (rel/what-if db R {A [a0] B [b0]} [:delete B b0])))
    (is (= [a0] (rel/what-if db R {A [a0]} [:delete B b0])))
    (is (= #{a0, ab1} (set (rel/what-if db R {A [a0, a1], B [b0, b1]} [:delete B [= :% b0]]))))))

(deftest migrate-table-state-test
  (let [db {}
        A [[:from :A {:req [:a]}]]
        A2 [[:from :A {:req [:a, :b]}]]
        B [[:from :A {:req [:a]}] [:select :b]]
        B2 [[:from :A {:req [:a :b]}] [:select :b]]
        db (rel/transact db {A [{:a 1, :b 1}]})
        db (rel/transact db {A2 [{:a 2, :b 2}]})]

    (is (= #{{:a 1, :b 1}, {:a 2, :b 2}} (set (rel/q db A))))
    (is (= #{{:b 1} {:b 2}} (set (rel/q db B))))

    (is (= #{{:a 1, :b 1}, {:a 2, :b 2}} (set (rel/q db A2))))
    (is (= nil (dataflow/get-id (dataflow/gg db) A2)))
    (is (= #{{:a 1, :b 1}, {:a 2, :b 2}} (set (rel/q db :A))))
    (is (= #{{:b 1} {:b 2}} (set (rel/q db B2))))))

(deftest where-lookup-test
  (let [A [[:from :A]]
        db {}]
    (is (= [{:a 42}] (rel/what-if db [[:from A] [:where [= :a 42]]] {A [{:a 42}, {:a 43}]})))
    (is (= #{{:a 43}} (-> (rel/mat db [[:from A] [:where [= :a 43]]])
                          (rel/transact {A [{:a 42}, {:a 43}]})
                          (dataflow/gg)
                          (dataflow/get-node [[:from A] [:where [= :a 43]]])
                          result-set)))

    (is (= [{:a 42}] (rel/what-if db [[:from A] [:where [= :a 42] {[:b :% [::rel/esc ::missing]] ::missing}]] {A [{:a 42}, {:a 43}]})))
    (is (= nil (rel/what-if db [[:from A] [:where {:a 42} {:a 43}]])))
    (is (= nil (rel/what-if db [[:from A] [:where [:or {:a 42} {:a 43}]]])))))

(deftest dematerialize-test
  (let [A [[:from :A]]
        A1 [[:from :A] [:where [= :a 42]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/mat db A1)]
    (is (= #{{:a 42}} (-> db (dataflow/gg) (dataflow/get-node A1) result-set)))
    (is (nil? (-> db (rel/demat A1) (dataflow/gg) (dataflow/get-node A1))))
    (is (= #{{:a 42}} (-> db (rel/demat A1) dataflow/gg :A)))))

(deftest dematerialize-deletes-orphaned-transitives-test
  (let [A [[:from :A]]
        A1 [[:from :A] [:where [= :a 42]]]
        A2 [[:from :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/mat db A2)]
    (is (= #{{:a 43}} (-> db dataflow/gg (dataflow/get-node A2) result-set)))
    (is (nil? (-> db (rel/demat A2) dataflow/gg (dataflow/get-node A1))))
    (is (= #{{:a 42}} (-> db (rel/demat A2) dataflow/gg :A)))))

(deftest dematerialize-keeps-materialized-transitives-test
  (let [A [[:from :A]]
        A1 [[:from :A] [:where [= :a 42]]]
        A2 [[:from :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/mat db A2 A1)]
    (is (= #{{:a 42}} (-> db dataflow/gg (dataflow/get-node A1) result-set)))
    (is (= #{{:a 43}} (-> db dataflow/gg (dataflow/get-node A2) result-set)))
    (is (some? (-> db (rel/demat A2) dataflow/gg (dataflow/get-node A1))))
    (is (-> db (rel/demat A1) dataflow/gg (dataflow/get-node A1) some?))
    (is (nil? (-> db (rel/demat A2 A1) dataflow/gg (dataflow/get-node A1))))))

(deftest watch-table-test
  (let [A :A
        db (rel/transact {} {A [{:a 42}]})
        db (rel/watch db A)
        {db2 :db, :keys [changes]} (rel/track-transact db {A [{:a 43}]})]
    (is (seq changes))
    (is (= {A {:added [{:a 43}], :deleted []}} changes))
    (is (= {:A #{{:a 42}, {:a 43}}} db2))
    (is (= {A {:added [{:a 43}], :deleted [{:a 42}]}} (:changes (rel/track-transact db [:delete A {:a 42}] {A [{:a 43}]}))))))

(deftest watch-view-test
  (let [A [[:from :A]]
        A1 [[:from :A] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/watch db A1)
        {db2 :db, :keys [changes]} (rel/track-transact db {A [{:a 43}]})]
    (is (seq changes))
    (is (= {A1 {:added [{:a 44}], :deleted []}} changes))
    (is (= {:A #{{:a 42}, {:a 43}}} db2))
    (is (= {A1 {:added [{:a 44}], :deleted [{:a 43}]}} (:changes (rel/track-transact db [:delete A {:a 42}] {A [{:a 43}]}))))))

(deftest unwatch-removes-mat-test
  (let [A [[:from :A]]
        A1 [[:from :A] [:where [= :a 42]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/watch db A1)
        db (rel/unwatch db A1)]
    (is (nil? (-> db dataflow/gg (dataflow/get-node A1))))))

(deftest unwatch-keeps-explicitly-mat-test
  (let [A [[:from :A]]
        A1 [[:from :A] [:where [= :a 42]]]
        A2 [[:from :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/mat db A2)
        db (rel/watch db A2)
        db (rel/unwatch db A2)]

    (is (some? (-> db dataflow/gg (dataflow/get-node A1))))
    (is (= #{{:a 43}} (-> db dataflow/gg (dataflow/get-node A2) result-set)))))

(deftest track-transients-are-removed-test
  (is (= {:db {:A #{}}
          :changes {[[:from :A]] {:added [], :deleted []}}}
         (rel/track-transact (rel/watch {} [[:from :A]]) {:A [{:a 1}]} [:delete :A]))))

(deftest update-test
  (let [A [[:from :A]]
        db (rel/transact {} {A [{:a 42} {:a 44}]})]
    (is (= #{{:a 43}, {:a 45}} (set (rel/what-if db A [:update A {:a [inc :a]}]))))
    (is (= #{{:a 43}, {:a 44}} (set (rel/what-if db A [:update A {:a [inc :a]} [= :a 42]]))))
    (is (= #{{:a 42}, {:a 44}} (set (rel/what-if db A [:update A {:a [inc :a]} [= :a 41]]))))
    (is (= #{{:a 43}, {:a 44}} (set (rel/what-if db A [:update A {:a 43} [= :a 42]]))))
    (is (= #{{:a 43}, {:a 44}} (set (rel/what-if db A [:update A #(assoc % :a 43) [= :a 42]]))))))

(deftest update-set-nil-test
  (let [db (rel/transact {} {:Counter [{:n 0}]})]
    (is (= {:Counter #{{:n nil}}} (rel/transact db [:update :Counter {:n nil}])))))

(deftest delete-test
  (let [A [[:from :A]]
        db (rel/transact {} {A [{:a 42} {:a 44}]})]
    (is (= nil (rel/what-if db A [:delete A])))
    (is (= [{:a 42}] (rel/what-if db A [:delete A [= :a 44]])))
    (is (= [{:a 42}] (rel/what-if db A [:delete A [< 43 :a] [:or [= 1 2] [= 1 1]]])))))

(deftest env-test
  (let [A [[:from :A]]
        A2 [[:from A]
            [:extend [:b [str [rel/env :b]]]]
            [:where [any? [rel/env :b]]]
            [:set]]]

    (is (= [{:a 1, :b ""}]
           (-> {}
               (rel/with-env {})
               (rel/transact {A [{:a 1}]})
               (rel/q A2))))

    (is (= [{:a 1, :b "1"}]
           (-> {}
               (rel/with-env {:b 1})
               (rel/transact {A [{:a 1}]})
               (rel/q A2))))

    (is (= [{:a 1, :b "1"}]
           (-> {}
               (rel/transact {A [{:a 1}]})
               (rel/with-env {:b 1})
               (rel/q A2))))

    (is (= [{:a 1, :b "2"}]
           (-> {}
               (rel/transact {A [{:a 1}]})
               (rel/with-env {:b 1})
               (rel/update-env assoc :b 2)
               (rel/q A2))))

    (is (= [{:a 1, :b ""}]
           (-> {}
               (rel/transact {A [{:a 1}]})
               (rel/with-env {:b 1})
               (rel/update-env dissoc :b)
               (rel/q A2))))))

(deftest top-by-test
  (let [A [[:from :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/top-by 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]

    (is (= [{:top (vec (take 5 (reverse (rows 16))))}] (rel/what-if {} A2 {A (rows 16)})))
    (is (= [{:top (vec (take 5 (reverse (rows 48))))}] (rel/what-if {} A2 {A (rows 48)})))
    (is (= [{:top (vec (take 5 (reverse (rows 72))))}] (rel/what-if {} A2 {A (rows 72)})))
    (is (= [{:top (vec (take 5 (reverse (rows 100))))}] (rel/what-if {} A2 {A (rows 100)})))))

(deftest bottom-by-test
  (let [A [[:from :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/bottom-by 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]

    (is (= [{:top (vec (take 5 (rows 16)))}] (rel/what-if {} A2 {A (rows 16)})))
    (is (= [{:top (vec (take 5 (rows 48)))}] (rel/what-if {} A2 {A (rows 48)})))
    (is (= [{:top (vec (take 5 (rows 72)))}] (rel/what-if {} A2 {A (rows 72)})))
    (is (= [{:top (vec (take 5 (rows 100)))}] (rel/what-if {} A2 {A (rows 100)})))))

(deftest top-by-collision-test
  (let [A [[:from :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/top-by 10 :a]]]
            [:extend [:top [set :top]]]]
        rows (fn [n] (vec (for [i (range n) row [{:a i} {:a i, :b 42}]] row)))]
    (is (= [{:top (set (take 10 (reverse (rows 16))))}] (rel/what-if {} A2 {A (rows 16)})))
    (is (= [{:top (set (take 10 (reverse (rows 48))))}] (rel/what-if {} A2 {A (rows 48)})))
    (is (= [{:top (set (take 10 (reverse (rows 72))))}] (rel/what-if {} A2 {A (rows 72)})))
    (is (= [{:top (set (take 10 (reverse (rows 100))))}] (rel/what-if {} A2 {A (rows 100)})))))

(deftest bottom-by-collision-test
  (let [A [[:from :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/bottom-by 10 :a]]]
            [:extend [:top [set :top]]]]
        rows (fn [n] (vec (for [i (range n) row [{:a i} {:a i, :b 42}]] row)))]
    (is (= [{:top (set (take 10 (rows 16)))}] (rel/what-if {} A2 {A (rows 16)})))
    (is (= [{:top (set (take 10 (rows 48)))}] (rel/what-if {} A2 {A (rows 48)})))
    (is (= [{:top (set (take 10 (rows 72)))}] (rel/what-if {} A2 {A (rows 72)})))
    (is (= [{:top (set (take 10 (rows 100)))}] (rel/what-if {} A2 {A (rows 100)})))))

(deftest top-test
  (let [A [[:from :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/top 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]

    (is (= [{:top (vec (take 5 (reverse (range 16))))}] (rel/what-if {} A2 {A (rows 16)})))
    (is (= [{:top (vec (take 5 (reverse (range 48))))}] (rel/what-if {} A2 {A (rows 48)})))
    (is (= [{:top (vec (take 5 (reverse (range 72))))}] (rel/what-if {} A2 {A (rows 72)})))
    (is (= [{:top (vec (take 5 (reverse (range 100))))}] (rel/what-if {} A2 {A (rows 100)})))))

(deftest bottom-test
  (let [A [[:from :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/bottom 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]
    (is (= [{:top (vec (take 5 (range 16)))}] (rel/what-if {} A2 {A (rows 16)})))
    (is (= [{:top (vec (take 5 (range 48)))}] (rel/what-if {} A2 {A (rows 48)})))
    (is (= [{:top (vec (take 5 (range 72)))}] (rel/what-if {} A2 {A (rows 72)})))
    (is (= [{:top (vec (take 5 (range 100)))}] (rel/what-if {} A2 {A (rows 100)})))))

(deftest strip-meta-test
  (let [A [[:from :A]]
        A1 [[:from :A] [:where [= :a 42]]]
        db (rel/transact (rel/mat {} A1) {A [{:a 42}, {:a 43}]})
        db (rel/watch db A1)]
    (is (= {:A #{{:a 42}, {:a 43}}} (rel/strip-meta db)))
    (is (nil? (meta (rel/strip-meta db))))
    (is (= {:foo 42} (meta (rel/strip-meta (vary-meta db assoc :foo 42)))))))

(deftest unique-violation-test
  (let [db (rel/transact {} {:A [{:a 42}]})
        db (rel/mat db [[:from :A] [:unique :a]])]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/what-if db :A [:insert :A {:a 42, :b 1}])))
    (is (= [{:a 42}] (rel/what-if db :A [:insert :A {:a 42}])))))

(deftest delayed-fk-check-test
  (let [db (rel/mat {} [[:from :A] [:fk [[:from :B]] {:a :a}]])]
    (is (= [{:a 1}] (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 1}]})))
    (is (= [{:a 1}] (rel/what-if db :A {:B [{:a 1}]} {:A [{:a 1}]})))
    (is (thrown? #?(:clj Throwable :cljs js/Error) #"Foreign key violation" (rel/what-if db :A {:A [{:a 1}]})))
    (is (thrown? #?(:clj Throwable :cljs js/Error) #"Foreign key violation" (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 2}]})))))

(deftest cascading-delete-test
  (let [db (rel/mat {} [[:from :A] [:fk [[:from :B]] {:a :a} {:cascade :delete}]])]
    (is (= nil (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 1}]} [:delete :B])))))

(deftest cascading-delete-really-requires-:delete-test
  (let [db (rel/mat {} [[:from :A] [:fk [[:from :B]] {:a :a} {:cascade true}]])]
    (is (thrown-with-msg? #?(:clj Throwable :cljs js/Error) #"Foreign key violation" (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 1}]} [:delete :B])))))

(deftest false-req-col-is-allowed-test
  (let [A [[:from :A {:req [:a]}]]
        db (rel/mat {} A)]
    (is (= {:A #{{:a false}}} (rel/transact db {A [{:a false}]})))))

(deftest self-join-glitch-bug-test1
  (let [A [[:from :A]]
        B [[:from A]
           [:agg
            []
            [:a [count [even? :a]]]]]
        db (rel/mat {} B)
        db (rel/transact db {A [{:a 1} {:a 2}, {:a 3} {:a 4}]})]

    (is (= [{:a 2}] (rel/q db B)))
    (is (= [{:a 2}] (rel/what-if db B [:delete-exact :A {:a 1}])))
    (is (= [{:a 2}] (rel/what-if db B [:delete-exact :A {:a 1}] {:A [{:a 1}]})))))

(deftest self-join-glitch-bug-test2
  (let [A [[:from :A]]
        B [[:from A]
           [:agg
            []
            [:a [count [even? :a]]]
            [:b count]]]
        db (rel/mat {} B)
        db (rel/transact db {A [{:a 1} {:a 2} {:a 3} {:a 4}]})]

    (is (= [{:a 2, :b 4}] (rel/q db B)))
    (is (= [{:a 2, :b 3}] (rel/what-if db B [:delete-exact :A {:a 1}])))
    (is (= [{:a 2, :b 4}] (rel/what-if db B [:delete-exact :A {:a 1}] {:A [{:a 1}]})))))

(deftest narrowing-delete-extend-glitch-test
  (let [A [[:from :A]]
        B [[:from A] [:extend [:a 1]]]
        db (rel/transact (rel/mat {} B) {:A [{:a 42} {:a 43}]})]
    (is (= [{:a 1}] (rel/q db B)))
    (is (= [{:a 1}] (rel/what-if db B [:delete-exact :A {:a 42}])))))

(deftest narrowing-query-extend-glitch-test
  (let [A [[:from :A]]
        B [[:from A] [:extend [:a 1]]]
        db (rel/transact {} {:A [{:a 42} {:a 43}]})]
    (is (= [{:a 1}] (rel/q db B)))
    (is (= [{:a 1}] (rel/what-if db B [:delete-exact :A {:a 42}])))))

(deftest narrowing-delete-select-glitch-test
  (let [A [[:from :A]]
        B [[:from A] [:select [:a 1]]]
        db (rel/transact (rel/mat {} B) {:A [{:a 42} {:a 43}]})]
    (is (= [{:a 1}] (rel/q db B)))
    (is (= [{:a 1}] (rel/what-if db B [:delete-exact :A {:a 42}])))))

(deftest narrowing-expand-glitch-test
  (let [A [[:from :A]]
        B [[:from A] [:expand [[:b] :b]]]
        db (rel/transact
             (rel/mat {} B)
             {A [{:b [{:b 1}]}
                 {:b [{:b 1}, {:b 2}]}]})]
    (is (= #{{:b 1} {:b 2}} (set (rel/q db B))))
    (is (= [{:b 1}] (rel/what-if db B [:delete-exact A {:b [{:b 1}, {:b 2}]}])))))

(deftest narrowing-join-glitch-test
  (let [A [[:from :A]]
        B [[:from :B]]
        J [[:from A] [:join B {:a :a}]]
        db (rel/transact
             (rel/mat {} J)
             {A [{:a 1} {:a 1, :b 2}]
              B [{:a 1, :b 2}]})]
    (is (= [{:a 1 :b 2}] (rel/q db J)))
    (is (= [{:a 1}] (rel/what-if db A [:delete-exact A {:a 1, :b 2}])))
    (is (= [{:a 1 :b 2}] (rel/what-if db J [:delete-exact A {:a 1, :b 2}])))))

(deftest narrowing-left-join-glitch-test
  (let [A [[:from :A]]
        B [[:from :B]]
        J [[:from A] [:left-join B {:a :a}]]
        db (rel/transact
             (rel/mat {} J)
             {A [{:a 1} {:a 1, :b 2}]
              B [{:a 1, :b 2}]})]
    (is (= [{:a 1 :b 2}] (rel/q db J)))
    (is (= [{:a 1 :b 2}] (rel/what-if db J [:delete-exact A {:a 1, :b 2}])))))

(deftest delayed-check-test
  (let [A [[:from :A]]
        B [[:from A] [:check [= :a 1]]]
        db (rel/mat {} B)]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/transact db  {:A [{:a 2}]})))
    (is (= {:A #{{:a 1}}} (rel/transact db  {:A [{:a 2}]} [:update :A {:a 1}])))
    (is (= {:A #{}} (rel/transact db  {:A [{:a 2}]} [:delete :A])))))

(deftest delayed-check-cascade-test
  (let [A [[:from :A]]
        B [[:from :B]]
        C [[:from A] [:fk B {:a :a} {:cascade :delete}]]
        D [[:from A] [:check [= :a 1]]]
        db (rel/mat {} C D)]

    (is (thrown? #?(:clj Throwable :cljs js/Error)
                 (rel/transact db {:A [{:a 2}]
                                   :B [{:a 2}]})))

    (is (= {:A #{}
            :B #{{:a 1}}} (rel/transact db
                                        {:A [{:a 2}], :B [{:a 1} {:a 2}]}
                                        [:delete :B [= :a 2]])))

    (is (= {:A #{{:a 1}}
            :B #{{:a 1}}} (rel/transact db {:A [{:a 2}], :B [{:a 2}]}
                                        [:update :A {:a 1}]
                                        [:update :B {:a 1}])))))

(deftest hash-lookup-test
  (let [index [[:from :A]
               [:hash :a [inc :a]]]
        db (rel/mat {} index)]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/q {} [[:lookup index "a" "b"]])))
    (is (= [{:a 1}] (rel/what-if db [[:lookup index 1 2]] {:A [{:a 1} {:a 2}]})))
    (is (= [{:a 1} {:a 1, :b 2}] (rel/what-if db [[:lookup index 1 2]] {:A [{:a 1} {:a 2} {:a 1, :b 2}]})))))

(deftest btree-lookup-test
  (let [index [[:from :A]
               [:btree :a [inc :a]]]
        db (rel/mat {} index)]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/q {} [[:lookup index "a" "b"]])))
    (is (= [{:a 1}] (rel/what-if db [[:lookup index 1 2]] {:A [{:a 1} {:a 2}]})))
    (is (= [{:a 1} {:a 1, :b 2}] (rel/what-if db [[:lookup index 1 2]] {:A [{:a 1} {:a 2} {:a 1, :b 2}]})))))

(deftest overwrite-binding-test
  (let [db (rel/transact {} {:A [{:a 1}]})]
    (is (= [{:a nil, :b 1}] (rel/q db [[:from :A] [:extend [:a 42] [:a nil] [:b 1]]])))))

(deftest bind-all-test
  (let [db (rel/transact {} {:A [{:a {:a nil, :b 2, :c 3}}]})]
    (is (= [{:a nil, :b 2, :c 3}] (rel/q db [[:from :A] [:extend [::rel/* :a]]])))
    (is (= [{:a nil, :b 2, :c 3}] (rel/q db [[:from :A] [:extend [:* :a]]])))))

(deftest from-as-table-alias-test
  (let [db (rel/transact {} {:A [{:a 1}]
                             :B [{:a 1, :b 2}]})]
    (is (= [{:a 1}] (rel/q db [[:from :A]])))
    (is (= [{:a 1, :b 2}] (rel/q db [[:from :A] [:join :B {:a :a}]])))))

(deftest sum-test
  (let [coll (shuffle (map #(array-map :a %) (range 1000)))
        db (rel/transact {} {:A coll})]
    (is (= [{:n 499500}] (rel/q db [[:from :A] [:agg [] [:n [rel/sum :a]]]])))
    (is (= [{:n 999000}] (rel/q db [[:from :A] [:agg [] [:n [rel/sum [* 2 :a]]]]])))))

(deftest self-join-test
  (let [db (rel/transact {} {:A [{:a 1, :b 1}]
                             :B [{:b 1, :c 1}]})
        Q [[:from :A]
           [:join [[:from :A] [:join :B {:b :b}]] {:b :b}]]]

    (is (= [{:a 1, :b 1, :c 1}] (rel/q db Q)))))

(deftest multi-join-test2
  (let [db (rel/transact {} {:A [{:a 1, :b 1}]
                             :B [{:b 1, :c 1}]
                             :C [{:c 1, :d 1}]})
        Q [[:from :A]
           [:join :B {:b :b}]
           [:join [[:from :A] [:join :B {:b :b}] [:join :C {:c :c}]] {:c :c}]
           [:join [[:from :A]] {[inc :a] [+ 1 :a]}]]]

    (is (= [{:a 1, :b 1, :c 1, :d 1}] (rel/q db Q)))))

(deftest agg-implicit-collision-test
  (let [db (rel/transact {} {:A [{:a 1, :b 2, :c 3, :d 1}
                                 {:a 1, :b 2, :c 3, :d 2}]})
        badq [[:from :A]
              [:agg [:b :c]
               [:c count]
               [:b [rel/sum [inc :b]]]]]
        goodq [[:from :A]
               [:agg [:b :c]
                [:d count]
                [:d2 [count true]]
                [:e [rel/sum [inc :b]]]]]]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/q db badq)))
    (is (= [{:b 2, :c 3, :d 2, :d2 2, :e 6}] (rel/q db goodq)))))

(deftest min-max-test
  (let [db (rel/transact {} {:A (for [n (range 1000)] {:a n})})]
    (is (= [{:mina 0}] (rel/q db [[:from :A] [:agg [] [:mina [min :a]]]])))
    (is (= [{:mina {:a 0}}] (rel/q db [[:from :A] [:agg [] [:mina [rel/min-by :a]]]])))
    (is (= [{:maxa 999}] (rel/q db [[:from :A] [:agg [] [:maxa [max :a]]]])))
    (is (= [{:maxa {:a 999}}] (rel/q db [[:from :A] [:agg [] [:maxa [rel/max-by :a]]]])))))

(deftest into-test
  (is (= [{:a 1} {:a 2}] (rel/q {} [[:const [{:a 2}]]] {:into [{:a 1}]})))
  (is (= #{{:a 1} {:a 2}} (rel/q {} [[:const [{:a 2}]]] {:into #{{:a 1}}}))))

(deftest avg-test
  (is (= [{:avg 0}] (rel/q {} [[:const [{:b 0}]]
                               [:agg [] [:avg [rel/avg :a]]]])))

  (is (= [{:avg 2}] (rel/q {} [[:const [{:a 2}]]
                               [:agg [] [:avg [rel/avg :a]]]])))

  (is (= [{:avg 2}] (rel/q {} [[:const [{:a 1} {:a 3}]]
                               [:agg [] [:avg [rel/avg :a]]]])))

  (is (= [{:avg #?(:clj 3/2 :cljs 1.5)}] (rel/q {} [[:const [{:a 0} {:a 3}]]
                                                    [:agg [] [:avg [rel/avg :a]]]])))

  (is (= [{:avg #?(:clj 3/2 :cljs 1.5)}] (rel/q {} [[:const [{:b 0} {:a 3}]]
                                                    [:agg [] [:avg [rel/avg :a]]]])))

  (is (= [{:avg 1.5M}] (rel/q {} [[:const [{:b 0} {:a 3.0M}]]
                                  [:agg [] [:avg [rel/avg :a]]]]))))

(deftest insert-or-replace-basic-example
  (let [db (rel/transact {} [:insert-or-replace :A {:a 42}])
        db (rel/mat db [[:from :A] [:unique :a]])
        db (rel/transact db [:insert-or-replace :A {:a 42, :b 42} {:a 42, :b 43}])]
    (is (= [{:a 42, :b 43}] (rel/q db :A)))
    (is (= #{{:a 42}, {:a 43}} (set (rel/what-if db :A [:insert-or-replace :A {:a 42} {:a 43}] [:insert-or-replace :A {:a 42}]))))
    (is (= #{{:a 42, :b 43}, {:a 43, :b 43}} (set (rel/what-if db :A [:insert-or-replace :A {:a 43, :b 43}]))))))

(deftest insert-or-replace-removes-old-row-test
  (let [db (rel/mat {} [[:from :A] [:unique :a]])]
    (is (= {:A #{{:a 1, :c 3}}} (rel/transact db {:A [{:a 1, :b 2}]} [:insert-or-replace :A {:a 1, :c 3}])))))

(deftest insert-or-replace-multiple-conflicts-test
  (let [db (rel/mat {} [[:from :A] [:unique :a]] [[:from :A] [:unique :b]])]
    (is (=
          {:A #{{:a 1, :b 2}}}
          (rel/transact db
                        {:A [{:a 1, :b 1} {:a 2, :b 2}]}
                        [:insert-or-replace :A {:a 1, :b 2}])))))

(deftest insert-or-update-test
  (let [db (rel/transact {} [:insert-or-update :A {:b [inc :a]} {:a 42}])
        db (rel/mat db [[:from :A] [:unique :a]])
        db2 (rel/transact db [:insert-or-update :A {:b [inc :a]} {:a 42, :b 45} {:a 43}])]
    (is (= {:A #{{:a 42}}} db))
    (is (= {:A #{{:a 42, :b 43} {:a 43}}} db2))))

(deftest insert-ignore-test
  (let [db (rel/transact {} [:insert-ignore :A {:a 42}])
        db (rel/mat db [[:from :A] [:unique :a]])
        db2 (rel/transact db [:insert-ignore :A {:a 42, :b 45} {:a 43}])]
    (is (= {:A #{{:a 42}}} db))
    (is (= {:A #{{:a 42} {:a 43}}} db2))))

(deftest insert-or-merge-test
  (let [db (rel/transact {} [:insert-or-merge :A :* {:a 42}])
        db (rel/mat db [[:from :A] [:unique :a]])
        db2 (rel/transact db [:insert-or-merge :A :* {:a 42, :b 45} {:a 43}])
        db3 (rel/transact db2 [:insert-or-merge :A [:c] {:a 42, :c 43} {:a 43, :b 42}])]
    (is (= {:A #{{:a 42}}} db))
    (is (= {:A #{{:a 42, :b 45} {:a 43}}} db2))
    (is (= {:A #{{:a 42, :b 45, :c 43} {:a 43}}} db3))))

(deftest relink-bug-test
  (let [db (rel/mat {} [[:from :A]
                                [:where [= :a 42]]])
        db (rel/transact db {:A [{:a 42}]})
        db (rel/mat db [[:from :A] [:select :a]])
        db (rel/transact db {:A [{:a 43}]})]
    (is (= [{:a 42}, {:a 43}] (rel/q db [[:from :A] [:select :a]] {:sort [:a]})))))

(deftest select-dual-test
  (is (= [{}] (rel/q {} [[:select]])))
  (is (= [{:a 42}] (rel/q {} [[:select [:a 42]]]))))

;; making sure doc examples work

;; agg.md

(deftest agg-doc-example-test
  (let [relvar
        [[:from :Order]
         [:agg
          ;; the first arg to :agg is the grouping vector
          ;; the columns to group by (can be empty for all rows)
          [:customer-id]

          ;; the rest of the args are aggregate extensions to the grouped
          ;; columns
          [:total-spend [rel/sum :total]]
          [:average-spend [rel/avg :total]]
          [:order-count count]]]

        state
        {:Order [{:customer-id 42, :total 12.0M}
                 {:customer-id 42, :total 25.0M}]}

        expected
        [{:customer-id 42, :average-spend 18.5M, :order-count 2, :total-spend 37.0M}]]

    (is (= expected (rel/what-if {} relvar state)))))

;; any.md

(deftest any-doc-example-test
  (let [relvar
        [[:from :Order]
         [:agg
          [:customer-id]
          [:has-high-valued-order [rel/any [<= 35.0M :total]]]]]

        state
        {:Order [{:customer-id 42, :total 10.0M}
                 {:customer-id 42, :total 12.0M}
                 {:customer-id 43, :total 50.0M}]}

        expected
        [{:customer-id 42, :has-high-valued-order false}
         {:customer-id 43, :has-high-valued-order true}]]

    (is (= (set expected) (set (rel/what-if {} relvar state))))))

;; avg.md

(deftest avg-doc-example-test
  (let [relvar
        [[:from :Order]
         [:agg [] [:aov [rel/avg :total]]]]

        state
        {:Order [{:total 10.0M}
                 {:total 25.0M}]}

        expected [{:aov 17.5M}]]
    (is (= expected (rel/what-if {} relvar state)))))

;; bottom.md

(deftest bottom-example-test
  (let [relvar
        [[:from :Player]
         [:agg [] [:lowest-scores [rel/bottom 5 :score]]]]

        state
        {:Player [{:score 1}
                  {:score 200}
                  {:score 4323}
                  {:score 5555}
                  {:score 4242}
                  {:score -123}
                  {:score 330}]}

        expected
        [{:lowest-scores [-123
                          1
                          200
                          330
                          4242]}]]

    (is (= expected (rel/what-if {} relvar state)))))

;; bottom-by.md

(deftest bottom-by-example-test
  (let [relvar
        [[:from :Player]
         [:agg [] [:lowest-scoring [rel/bottom-by 5 :score]]]]

        state
        {:Player [{:score 1
                   :name "alice"}
                  {:score 200
                   :name "bob"}
                  {:score 4323
                   :name "fred"}
                  {:score 5555
                   :name "hannah"}
                  {:score 4242
                   :name "george"}
                  {:score -123
                   :name "isabel"}
                  {:score 330
                   :name "dave"}]}

        expected [{:lowest-scoring [{:score -123, :name "isabel"}
                                    {:score 1, :name "alice"}
                                    {:score 200, :name "bob"}
                                    {:score 330, :name "dave"}
                                    {:score 4242, :name "george"}]}]]
    (is (= expected (rel/what-if {} relvar state)))))

;; btree.md

(deftest btree-example1-test
  (let [relvar [[:from :Player] [:btree :score :name]]
        db (rel/transact {} {:Player [{:score 1
                                       :name "alice"}
                                      {:score 1
                                       :name "harry"}
                                      {:score 200
                                       :name "bob"}]})
        db (rel/mat db relvar)
        idx (rel/index db relvar)]
    (is (sorted? idx))
    (is (sorted? (get idx 200)))
    (is (= {1 {"alice" #{{:score 1, :name "alice"}}
               "harry" #{{:score 1, :name "harry"}}}
            200 {"bob" #{{:score 200, :name "bob"}}}} idx))))

(deftest btree-example2-test
  (let [relvar [[:from :Event] [:btree :ts]]
        db (rel/transact {} {:Event [{:ts 0, :msg "hello"}
                                     {:ts 1, :msg ", world"}
                                     {:ts 2, :msg "!"}]})
        db (rel/mat db relvar)
        idx (rel/index db relvar)]
    (is (sorted? idx))
    (is (= {0 #{{:ts 0, :msg "hello"}}
            1 #{{:ts 1, :msg ", world"}}
            2 #{{:ts 2, :msg "!"}}} idx))))

;; change-tracking.md

(deftest change-tracking-example-test
  (let [;; given a relvar we are interested in watching
        ViewModel
        [[:from :User]
         [:where :selected]]

        ;; lets assume the state looks like this:
        st {:User [{:user "alice", :selected false}
                   {:user "bob", :selected false}]}

        db (rel/transact {} st)

        ;; enable change tracking for our relvar with watch
        ;; this returns a new relic database, don't worry its totally pure!
        db (rel/watch db ViewModel)

        ;; now we have a watched relvar, we can track-transact and receive changes to it.
        result (rel/track-transact db [:update :User {:selected true} [= :user "bob"]])
        ;; =>
        expected
        {
         ;; the first key is just the :db as with the transactions applied
         :db {:User #{{:user "alice", :selected false}, {:user "bob", :selected true}}},
         ;; you also get a :changes key, which is the map of {relvar changes} for all watched relvars
         :changes
         {
          ;; our watched ViewModel relvar from earlier
          [[:from :User]
           [:where :selected]]

          ;; added rows and deleted rows are returned.
          {:added [{:user "bob", :selected true}],
           :deleted []}}}

        ;; you can unwatch
        _ (rel/unwatch db ViewModel)]
    (is (= expected result))))

;; check.md
(deftest check-example-test
  (let [cerr (fn [person relvar]
               (try
                 (rel/what-if {} relvar [:insert :Person person])
                 nil
                 (catch #?(:clj Throwable :cljs js/Error) e
                   #?(:clj (.getMessage e) :cljs (.-message e)))))]

    (is (nil?
          (cerr
            {:name "fred"}
            [[:from :Person]
             [:check [string? :name]]])))

    (is (= "Check constraint violation"
           (cerr
             {:name 42}
             [[:from :Person]
              [:check [string? :name]]])))


    (is (= "All people must have a string :name"
           (cerr
             {:name 42}
             [[:from :Person]
              [:check {:pred [string? :name]
                       :error "All people must have a string :name"}]])))

    (is (= "Check constraint violation"
           (cerr
             {:name "Fred"}
             [[:from :Person]
              [:check [string? :name] [nat-int? :age]]])))

    (is (= "Check constraint violation"
           (cerr
             {:age 42}
             [[:from :Person]
              [:check [string? :name] [nat-int? :age]]])))

    (is (nil?
           (cerr
             {:age 42, :name "Fred"}
             [[:from :Person]
              [:check [string? :name] [nat-int? :age]]])))

    (is (= (str "Expected a string :name, got " (type 42))
           (cerr
             {:name 42}
             [[:from :Person]
              [:check {:pred [string? :name]
                       :error [str "Expected a string :name, got " [type :name]]}]])))))

;; const.md

(deftest const-example-test
  (let [relvar
        [[:const [{:a 1} {:a 2} {:a 3}]]
         [:where [even? :a]]]
        expected
        [{:a 2}]]
    (is (= expected (rel/q {} relvar)))))

;; constrain.md
;; todo ?!

;; count.md
(deftest count-example-test
  (let [relvar
        [[:from :Order]
         [:agg [:customer-id]
          [:number-of-orders count]
          [:number-of-refunded-orders [count :refunded]]]]

        state
        {:Order [{:customer-id 42, :order-id 0}, {:customer-id 42, :order-id 1, :refunded true}]}

        expected
        [{:customer-id 42, :number-of-orders 2, :number-of-refunded-orders 1}]]
    (is (= expected (rel/what-if {} relvar state)))))

;; count-distinct.md
(deftest count-distinct-example-test
  (let [relvar
        [[:from :Order]
         [:agg [] [:n-customers [rel/count-distinct :customer-id]]]]
        state
        {:Order [{:customer-id 42, :order-id 0}, {:customer-id 42, :order-id 1}, {:customer-id 43, :order-id 2}]}
        expected
        [{:n-customers 2}]]
    (is (= expected (rel/what-if {} relvar state)))))


;; delete.md
(deftest delete-example1-test
  (let [tx [:delete :Person [= :name "Tom"]]
        st {:Person [{:name "Tom"}
                     {:name "Alice"}]}]
    (is (= {:Person #{{:name "Alice"}}} (rel/transact {} st tx)))))

(deftest delete-example2-test
  (let [tx [:delete :Event [= :type "insert"] [< :ts 2]]
        st {:Event [{:type "insert", :ts 0}
                    {:type "delete" :ts 1}
                    {:type "insert" :ts 2}]}]
    (is (= {:Event #{{:type "delete" :ts 1}
                     {:type "insert" :ts 2}}} (rel/transact {} st tx)))))

;; sub-select.md

(deftest sub-select-example-test
  (let [q
        [[:from :Order]
         [:select :order-id [:items [rel/sel :OrderItem {:order-id :order-id}]]]]
        st
        {:Order #{{:order-id 0}}
         :OrderItem #{{:order-id 0, :product "eggs"}, {:order-id 0, :product "bread"} {:order-id 1, :product "cheese"}}}
        rs [{:order-id 0, :items #{{:order-id 0, :product "eggs"}, {:order-id 0, :product "bread"}}}]]
    (is (= rs (rel/what-if {} q st)))))

(deftest sub-select-no-clause-example-test
  (let [q
        [[:from :Order]
         [:select :order-id [:items [rel/sel :OrderItem]]]]
        st
        {:Order #{{:order-id 0}}
         :OrderItem #{{:order-id 0, :product "eggs"}, {:order-id 0, :product "bread"} {:order-id 1, :product "cheese"}}}
        rs [{:order-id 0, :items #{{:order-id 0, :product "eggs"}, {:order-id 0, :product "bread"}, {:order-id 1, :product "cheese"}}}]]
    (is (= rs (rel/what-if {} q st)))))

(deftest sub-select-first-example-test
  (let [q
        [[:from :Order]
         [:select [:egg [rel/sel1 [[:from :OrderItem] [:where [= :product "eggs"]]] {:order-id :order-id}]]]]
        st
        {:Order #{{:order-id 0}}
         :OrderItem #{{:order-id 0, :product "eggs"}, {:order-id 0, :product "bread"} {:order-id 1, :product "cheese"}}}
        rs [{:egg {:order-id 0, :product "eggs"}}]]
    (is (= rs (rel/what-if {} q st)))))

(deftest q-sort-test
  (let [aseq (map array-map (repeat :a) (range 100))
        db (rel/transact {} {:A aseq})]
    (is (= aseq (rel/q db :A {:sort [:a]})))
    (is (= (reverse aseq) (rel/q db :A {:rsort [:a]})))

    (is (= aseq (rel/q db [[:from :A] [:where true]] {:sort [:a]})))
    (is (= (reverse aseq) (rel/q db [[:from :A] [:where true]] {:rsort [:a]})))
    (is (= (reverse aseq) (rel/q db :A {:sort [[- :a]]})))
    (is (= (sort-by (juxt #(mod (:a %) 10) :a) aseq) (rel/q db :A {:sort [[mod :a 10] :a]})))
    (is (= (reverse (sort-by (juxt #(mod (:a %) 10) :a) aseq)) (rel/q db :A {:rsort [[mod :a 10] :a]})))))

(deftest comparison-test
  (testing "<"
    (is (true? (e/< 3 4)))
    (is (false? (e/< 3 3)))
    (is (false? (e/< 4 3)))
    (is (true? (e/< "a" "b")))
    (is (false? (e/< "b" "a"))))

  (testing "<="
    (is (true? (e/<= 3 4)))
    (is (true? (e/<= 3 3)))
    (is (false? (e/<= 4 3)))
    (is (true? (e/<= "a" "b")))
    (is (false? (e/<= "b" "a"))))

  (testing ">"
    (is (false? (e/> 3 4)))
    (is (false? (e/> 3 3)))
    (is (true? (e/> 4 3)))
    (is (false? (e/> "a" "b")))
    (is (true? (e/> "b" "a"))))

  (testing ">="
    (is (false? (e/>= 3 4)))
    (is (true? (e/>= 3 3)))
    (is (true? (e/>= 4 3)))
    (is (false? (e/>= "a" "b")))
    (is (true? (e/>= "b" "a")))))

(deftest sort-test
  (let [aseq (map array-map (repeat :a) (range 100))
        db (rel/transact {} {:A aseq})]
    (is (= aseq (rel/q db [[:from :A] [:sort [:a :asc]]])))
    (is (= aseq (rel/q db [[:from :A] [:sort [:a]]])))
    (is (= (sort-by :a > aseq) (rel/q db [[:from :A] [:sort [:a :desc]]])))

    (let [data [{:a "a"
                 :b 1}
                {:a "b"
                 :b 0}
                {:a "b"
                 :b 1}]]

      (is (= [{:a "a", :b 1} {:a "b", :b 1},{:a "b", :b 0}] (rel/q {} [[:const data] [:sort [:a :asc] [:b :desc]]])))
      (is (= [{:a "b", :b 0} {:a "b", :b 1},{:a "a", :b 1}] (rel/q {} [[:const data] [:sort [:a :desc] [:b :asc]]]))))))

(deftest sort-limit-test
  (let [aseq (map array-map (repeat :a) (range 100))
        db (rel/transact {} {:A aseq})]
    (is (= aseq (rel/q db [[:from :A] [:sort-limit 101 [:a :asc]]])))
    (is (= (take 10 aseq) (rel/q db [[:from :A] [:sort-limit 10 [:a :asc]]])))
    (is (= (take 10 aseq) (rel/q db [[:from :A] [:sort-limit 10 [:a]]])))
    (is (= (take 10 (sort-by :a > aseq)) (rel/q db [[:from :A] [:sort-limit 10 [:a :desc]]])))))

(deftest index-selection-test
  (let [db (rel/mat
             {}

             [[:from :A]
              [:hash :a]]

             [[:from :A]
              [:hash :b]]

             [[:from :A]
              [:hash :a :b]]

             [[:from :A]
              [:btree :a :b :c]]

             [[:from :A]
              [:unique :b]])
        g (dataflow/gg db)
        plan #(dataflow/choose-plan (dataflow/where-plans g :A %&))]

    (is (= [[:from :A] [:unique :b]] (:index (plan [= :b 42]))))
    (is (= [[:from :A] [:btree :a :b :c]] (:index (plan [< :b 42]))))
    (is (= [[:from :A] [:btree :a :b :c]] (:index (plan [= :a 41] [< :b 42]))))
    (is (= :scan (:type (plan [= :d 42]))))
    (is (= :lookup (:type (plan [= :d 42] [even? :a] [< :a 42]))))
    (is (= :lookup (:type (plan [contains? #{42, 43} :a]))))))

(deftest no-crash-on-hash-index-as-only-option-for-seq-test
  (let [db (rel/mat {} [[:from :a] [:hash :a]])
        db (rel/transact db {:a [{:a 42}]})]
    (is (= [{:a 42}] (rel/q db [[:from :a] [:where [< 41 :a]]])))))

(deftest incompatible-index-scans-test
  (let [i [[:from :a] [:hash :a]]
        db (rel/mat {} i)
        q [[:from :a]
           [:where [= :a 42] [< :a 42] [> :a 42] [> :a 43] [< :a 41]]]]
    (is (empty? (rel/what-if db q {:a [{:a 42} {:a 43} {:a 41}]})))))

(deftest nil-join-test
  (let [data {:a [{:a nil}]
              :b [{:b nil, :c "foo"}]}]
    (is (= nil (rel/what-if {} [[:from :a] [:join :b {:a :b}]] data)))
    (is (= nil (rel/what-if {} [[:from :b] [:join :a {:c :c}]] data)))
    (is (= [{:a nil, :b nil, :c "foo"}] (rel/what-if {} [[:from :a] [:join :b {[:or :a "foo"] :c}]] data)))))

(deftest index-only-returns-for-approved-operators-test
  (let [a [[:from :a] [:hash :a]]
        b [[:from :a] [:btree :a]]
        c [[:from :a] [:unique :a]]
        d [[:from :a] [:set]]
        e [[:from :a]]
        f [[:from :a] [:where true]]
        db (rel/mat {} a b c d e f)
        db (rel/transact db {:a [{:a 1}]})]

    (is (some? (rel/index db a)))
    (is (some? (rel/index db b)))
    (is (some? (rel/index db c)))
    (is (some? (rel/index db d)))
    (is (nil? (rel/index db e)))
    (is (nil? (rel/index db f)))))

(deftest no-meta-no-problem-test
  (let [db {:foo #{{:foo 42}}}]
    (is (= [{:foo 42}] (rel/q db :foo)))
    (is (= [{:foo 42}] (rel/q db [[:from :foo] [:where [= 42 :foo]]])))
    (is (= nil (rel/q db [[:from :foo] [:where [= 43 :foo]]])))))

(comment
  (clojure.test/run-all-tests #"com\.wotbrew\.relic(.*)-test"))