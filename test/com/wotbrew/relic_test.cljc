(ns com.wotbrew.relic-test
  (:require [clojure.test :refer [deftest is are]]
            [com.wotbrew.relic :as rel]
            [com.wotbrew.relic.impl.dataflow :as dataflow]))

(deftest basics-test
  (let [a [[:from :A]]
        b [[:from :B]]
        db (rel/transact {} {:A #{{:a 42}, {:a 43}, {:a 45}}
                             :B #{{:a 42, :b 42}, {:a 42, :b 43}, {:a 43, :b 44}}})]
    (are [x ret]

      (and (= (set ret)
              (set (rel/q db x))
              (set (rel/q (rel/materialize db x) x))
              (set (rel/q (rel/dematerialize (rel/materialize db x) x) x))))

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
      #{{:a 42}})))

(deftest const-test
  (is (= [{:a 42}] (rel/q {} [[:const #{{:a 42}}]])))
  (is (= [{:a 42}] (rel/q {} [[:const [{:a 42}]]]))))

(deftest base-relvar-test
  (let [A [[:table :A]]]
    (is (dataflow/table-relvar? A))
    (is (not (dataflow/table-relvar? [])))
    (is (not (dataflow/table-relvar? (conj A [:where [= 1 1]]))))))

(deftest state-statement-test
  (let [A [[:from :A]]
        db {}
        a0 {:a 42}
        a1 {:a 43}
        a2 {:a 44}]
    (is (= nil (rel/q db A)))
    (is (= db (rel/materialize db A)))
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
    (is (= db (rel/materialize db F)))
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

        db (rel/materialize {} R)]

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

        db (rel/materialize {} R)]

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

        db (rel/materialize {} R)]

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

        db (rel/materialize {} R)]

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
    (is (= #{{:a 43}} (-> (rel/materialize db [[:from A] [:where [= :a 43]]])
                          (rel/transact {A [{:a 42}, {:a 43}]})
                          (dataflow/gg)
                          (dataflow/get-node [[:from A] [:where [= :a 43]]])
                          :results)))

    (is (= [{:a 42}] (rel/what-if db [[:from A] [:where [= :a 42] {[:b :% [::rel/esc ::missing]] ::missing}]] {A [{:a 42}, {:a 43}]})))
    (is (= nil (rel/what-if db [[:from A] [:where {:a 42} {:a 43}]])))
    (is (= nil (rel/what-if db [[:from A] [:where [:or {:a 42} {:a 43}]]])))))

(deftest dematerialize-test
  (let [A [[:from :A]]
        A1 [[:from :A] [:where [= :a 42]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A1)]
    (is (= #{{:a 42}} (-> db (dataflow/gg) (dataflow/get-node A1) :results)))
    (is (nil? (-> db (rel/dematerialize A1) (dataflow/gg) (dataflow/get-node A1))))
    (is (= #{{:a 42}} (-> db (rel/dematerialize A1) dataflow/gg :A)))))

(deftest dematerialize-deletes-orphaned-transitives-test
  (let [A [[:from :A]]
        A1 [[:from :A] [:where [= :a 42]]]
        A2 [[:from :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A2)]
    (is (= #{{:a 43}} (-> db dataflow/gg (dataflow/get-node A2) :results)))
    (is (nil? (-> db (rel/dematerialize A2) dataflow/gg (dataflow/get-node A1))))
    (is (= #{{:a 42}} (-> db (rel/dematerialize A2) dataflow/gg :A)))))

(deftest dematerialize-keeps-materialized-transitives-test
  (let [A [[:from :A]]
        A1 [[:from :A] [:where [= :a 42]]]
        A2 [[:from :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A2 A1)]
   (is (= #{{:a 42}} (-> db dataflow/gg (dataflow/get-node A1) :results)))
   (is (= #{{:a 43}} (-> db dataflow/gg (dataflow/get-node A2) :results)))
   (is (some? (-> db (rel/dematerialize A2) dataflow/gg (dataflow/get-node A1))))
   (is (= #{{:a 42}} (-> db (rel/dematerialize A1) dataflow/gg (dataflow/get-node A1) :results)))
   (is (nil? (-> db (rel/dematerialize A2 A1) dataflow/gg (dataflow/get-node A1))))))

(deftest watch-table-test
  (let [A [[:from :A]]
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
        db (rel/materialize db A2)
        db (rel/watch db A2)
        db (rel/unwatch db A2)]

    (is (some? (-> db dataflow/gg (dataflow/get-node A1))))
    (is (= #{{:a 43}} (-> db dataflow/gg (dataflow/get-node A2) :results)))))

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

(deftest delete-test
  (let [A [[:from :A]]
        db (rel/transact {} {A [{:a 42} {:a 44}]})]
    (is (= nil (rel/what-if db A [:delete A])))
    (is (= [{:a 42}] (rel/what-if db A [:delete A [= :a 44]])))
    (is (= [{:a 42}] (rel/what-if db A [:delete A [< 43 :a] [:or [= 1 2] [= 1 1]]])))))

(deftest env-test
  (let [A [[:from :A]]
        A2 [[:from A]
            [:extend [:b [str [::rel/env :b]]]]
            [:where [any? [::rel/env :b]]]
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
        db (rel/transact (rel/materialize {} A1) {A [{:a 42}, {:a 43}]})
        db (rel/watch db A1)]
    (is (= {:A #{{:a 42}, {:a 43}}} (rel/strip-meta db)))
    (is (nil? (meta (rel/strip-meta db))))
    (is (= {:foo 42} (meta (rel/strip-meta (vary-meta db assoc :foo 42)))))))

(deftest unique-violation-test
  (let [db (rel/transact {} {:A [{:a 42}]})
        db (rel/materialize db [[:from :A] [:unique :a]])]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/what-if db :A [:insert :A {:a 42, :b 1}])))
    (is (= [{:a 42}] (rel/what-if db :A [:insert :A {:a 42}])))))

(deftest delayed-fk-check-test
  (let [db (rel/materialize {} [[:from :A] [:fk [[:from :B]] {:a :a}]])]
    (is (= [{:a 1}] (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 1}]})))
    (is (= [{:a 1}] (rel/what-if db :A {:B [{:a 1}]} {:A [{:a 1}]})))
    (is (thrown? #?(:clj Throwable :cljs js/Error) #"Foreign key violation" (rel/what-if db :A {:A [{:a 1}]})))
    (is (thrown? #?(:clj Throwable :cljs js/Error) #"Foreign key violation" (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 2}]})))))

(deftest cascading-delete-test
  (let [db (rel/materialize {} [[:from :A] [:fk [[:from :B]] {:a :a} {:cascade :delete}]])]
    (is (= nil (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 1}]} [:delete :B])))))

(deftest cascading-delete-really-requires-:delete-test
  (let [db (rel/materialize {} [[:from :A] [:fk [[:from :B]] {:a :a} {:cascade true}]])]
    (is (thrown-with-msg? #?(:clj Throwable :cljs js/Error) #"Foreign key violation" (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 1}]} [:delete :B])))))

(deftest false-req-col-is-allowed-test
  (let [A [[:from :A {:req [:a]}]]
        db (rel/materialize {} A)]
    (is (= {:A #{{:a false}}} (rel/transact db {A [{:a false}]})))))

(deftest self-join-glitch-bug-test1
  (let [A [[:from :A]]
        B [[:from A]
           [:agg
            []
            [:a [count [even? :a]]]]]
        db (rel/materialize {} B)
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
        db (rel/materialize {} B)
        db (rel/transact db {A [{:a 1} {:a 2}, {:a 3} {:a 4}]})]

    (is (= [{:a 2, :b 4}] (rel/q db B)))
    (is (= [{:a 2, :b 3}] (rel/what-if db B [:delete-exact :A {:a 1}])))
    (is (= [{:a 2, :b 4}] (rel/what-if db B [:delete-exact :A {:a 1}] {:A [{:a 1}]})))))

(deftest narrowing-delete-extend-glitch-test
  (let [A [[:from :A]]
        B [[:from A] [:extend [:a 1]]]
        db (rel/transact (rel/materialize {} B) {:A [{:a 42} {:a 43}]})]
    (is (= [{:a 1}] (rel/q db B)))
    (is (= [{:a 1}] (rel/what-if db B [:delete-exact :A {:a 42}])))))

(deftest narrowing-delete-select-glitch-test
  (let [A [[:from :A]]
        B [[:from A] [:select [:a 1]]]
        db (rel/transact (rel/materialize {} B) {:A [{:a 42} {:a 43}]})]
    (is (= [{:a 1}] (rel/q db B)))
    (is (= [{:a 1}] (rel/what-if db B [:delete-exact :A {:a 42}])))))

(deftest narrowing-expand-glitch-test
  (let [A [[:from :A]]
        B [[:from A] [:expand [[:b] :b]]]
        db (rel/transact
             (rel/materialize {} B)
             {A [{:b [{:b 1}]}
                 {:b [{:b 1}, {:b 2}]}]})]
    (is (= #{{:b 1} {:b 2}} (set (rel/q db B))))
    (is (= [{:b 1}] (rel/what-if db B [:delete-exact A {:b [{:b 1}, {:b 2}]}])))))

(deftest narrowing-join-glitch-test
  (let [A [[:from :A]]
        B [[:from :B]]
        J [[:from A] [:join B {:a :a}]]
        db (rel/transact
             (rel/materialize {} J)
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
             (rel/materialize {} J)
             {A [{:a 1} {:a 1, :b 2}]
              B [{:a 1, :b 2}]})]
    (is (= [{:a 1 :b 2}] (rel/q db J)))
    (is (= [{:a 1 :b 2}] (rel/what-if db J [:delete-exact A {:a 1, :b 2}])))))

(deftest delayed-check-test
  (let [A [[:from :A]]
        B [[:from A] [:check [= :a 1]]]
        db (rel/materialize {} B)]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/transact db  {:A [{:a 2}]})))
    (is (= {:A #{{:a 1}}} (rel/transact db  {:A [{:a 2}]} [:update :A {:a 1}])))
    (is (= {:A #{}} (rel/transact db  {:A [{:a 2}]} [:delete :A])))))

(deftest delayed-check-cascade-test
  (let [A [[:from :A]]
        B [[:from :B]]
        C [[:from A] [:fk B {:a :a} {:cascade true}]]
        D [[:from A] [:check [= :a 1]]]
        db (rel/materialize {} C D)]

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
        db (rel/materialize {} index)]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/q {} [[:lookup index "a" "b"]])))
    (is (= [{:a 1}] (rel/what-if db [[:lookup index 1 2]] {:A [{:a 1} {:a 2}]})))
    (is (= [{:a 1} {:a 1, :b 2}] (rel/what-if db [[:lookup index 1 2]] {:A [{:a 1} {:a 2} {:a 1, :b 2}]})))))

(deftest btree-lookup-test
  (let [index [[:from :A]
               [:btree :a [inc :a]]]
        db (rel/materialize {} index)]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/q {} [[:lookup index "a" "b"]])))
    (is (= [{:a 1}] (rel/what-if db [[:lookup index 1 2]] {:A [{:a 1} {:a 2}]})))
    (is (= [{:a 1} {:a 1, :b 2}] (rel/what-if db [[:lookup index 1 2]] {:A [{:a 1} {:a 2} {:a 1, :b 2}]})))))

(deftest overwrite-binding-test
  (let [db (rel/transact {} {:A [{:a 1}]})]
    (is (= [{:b 1}] (rel/q db [[:from :A] [:extend [:a 42] [:a nil] [:b 1]]])))))

(deftest bind-all-test
  (let [db (rel/transact {} {:A [{:a {:a nil, :b 2, :c 3}}]})]
    (is (= [{:b 2, :c 3}] (rel/q db [[:from :A] [:extend [::rel/* :a]]])))
    (is (= [{:b 2, :c 3}] (rel/q db [[:from :A] [:extend [:* :a]]])))))

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
    (is (= [{:mina 0}] (rel/q db [[:from :A] [:agg [] [:mina [rel/min :a]]]])))
    (is (= [{:mina {:a 0}}] (rel/q db [[:from :A] [:agg [] [:mina [rel/min-by :a]]]])))
    (is (= [{:maxa 999}] (rel/q db [[:from :A] [:agg [] [:maxa [max :a]]]])))
    (is (= [{:maxa 999}] (rel/q db [[:from :A] [:agg [] [:maxa [rel/max :a]]]])))
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

  (is (= [{:avg 3/2}] (rel/q {} [[:const [{:a 0} {:a 3}]]
                                 [:agg [] [:avg [rel/avg :a]]]])))

  (is (= [{:avg 3/2}] (rel/q {} [[:const [{:b 0} {:a 3}]]
                                 [:agg [] [:avg [rel/avg :a]]]])))

  (is (= [{:avg 1.5M}] (rel/q {} [[:const [{:b 0} {:a 3.0M}]]
                                  [:agg [] [:avg [rel/avg :a]]]]))))

(deftest insert-or-replace-basic-example
  (let [db (rel/transact {} [:insert-or-replace :A {:a 42}])
        db (rel/materialize db [[:from :A] [:unique :a]])
        db (rel/transact db [:insert-or-replace :A {:a 42, :b 42} {:a 42, :b 43}])]
    (is (= [{:a 42, :b 43}] (rel/q db :A)))
    (is (= #{{:a 42}, {:a 43}} (set (rel/what-if db :A [:insert-or-replace :A {:a 42} {:a 43}] [:insert-or-replace :A {:a 42}]))))
    (is (= #{{:a 42, :b 43}, {:a 43, :b 43}} (set (rel/what-if db :A [:insert-or-replace :A {:a 43, :b 43}]))))))

(deftest insert-or-replace-removes-old-row-test
  (let [db (rel/materialize {} [[:from :A] [:unique :a]])]
    (is (= {:A #{{:a 1, :c 3}}} (rel/transact db {:A [{:a 1, :b 2}]} [:insert-or-replace :A {:a 1, :c 3}])))))

(deftest insert-or-replace-multiple-conflicts-test
  (let [db (rel/materialize {} [[:from :A] [:unique :a]] [[:from :A] [:unique :b]])]
    (is (= {:A #{{:a 1, :b 2}}}
           (rel/transact db
                         {:A [{:a 1, :b 1} {:a 2, :b 2}]}
                         [:insert-or-replace :A {:a 1, :b 2}])))))

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
        db (rel/materialize db relvar)
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
        db (rel/materialize db relvar)
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

(comment
  (clojure.test/run-all-tests #"relic"))