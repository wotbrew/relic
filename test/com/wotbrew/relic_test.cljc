(ns com.wotbrew.relic-test
  (:require [clojure.test :refer [deftest is are]]
            [com.wotbrew.relic :as rel]
            [com.wotbrew.relic.dataflow :as dataflow]))

(deftest basics-test
  (let [a [[:table :A]]
        b [[:table :B]]
        db (rel/transact {} {a #{{:a 42}, {:a 43}, {:a 45}}
                             b #{{:a 42, :b 42}, {:a 42, :b 43}, {:a 43, :b 44}}})]
    (are [x ret]

      (and (= (set ret) (set (rel/q db x))))

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
      #{{:a 42, :b 42}})))

(deftest const-test
  (is (= #{{:a 42}} (rel/q {} [[:const #{{:a 42}}]])))
  (is (= [{:a 42}] (rel/q {} [[:const [{:a 42}]]]))))

(deftest base-relvar-test
  (let [A [[:table :A]]]
    (is (dataflow/table-relvar? A))
    (is (not (dataflow/table-relvar? [])))
    (is (not (dataflow/table-relvar? (conj A [:where [= 1 1]]))))))

(deftest state-statement-test
  (let [A [[:table :A]]
        db {}
        a0 {:a 42}
        a1 {:a 43}
        a2 {:a 44}]
    (is (= nil (rel/q db A)))
    (is (= db (rel/materialize db A)))
    (is (= {:A #{}} (rel/transact db {A []})))
    (is (= #{a0} (rel/what-if db A {A [a0]})))
    (is (= {:A #{a0}} (rel/transact db [:insert A a0])))
    (is (= {:A #{}} (rel/transact db [:insert A a0] [:delete A a0])))
    (is (= {} (rel/transact db [:delete A a0])))
    (is (= #{a0} (rel/what-if db (vec (concat A A)) {A [a0]})))
    (is (= #{a0 a1 a2} (rel/what-if db A {A [a0 a1 a2 a0 a1]})))
    (is (= #{a2} (rel/what-if db A {A [a2 a2 a2]})))
    (is (= #{a2} (rel/what-if db A {A [a0 a1]} {A [a2]} [:delete-exact A a0 a1])))))

(deftest from-statement-test
  (let [A [[:table :A]]
        F [[:from A]]
        a0 {:a 42}
        a1 {:a 43}
        a2 {:a 44}
        db {}]
    (is (= nil (rel/q db F)))
    (is (= db (rel/materialize db F)))
    (is (= #{a0} (rel/what-if db F {A [a0]})))
    (is (= #{a0} (rel/what-if db (vec (concat A F)) {A [a0]})))
    (is (= #{a0 a1 a2} (rel/what-if db F {A [a0 a1 a2 a0 a1]})))
    (is (= #{a2} (rel/what-if db F {A [a2 a2 a2]})))
    (is (= #{a2} (rel/what-if db F {A [a0 a1]} {A [a2]} [:delete-exact A a0 a1])))))

(deftest where-expr-test
  (are [result row expr]
    (if result
      (= #{row} (set (rel/what-if {} [[:table :A] [:where expr]] {[[:table :A]] [row]})))
      (empty? (rel/what-if {} [[:table :A] [:where expr]] {[[:table :A]] [row]})))

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
  (let [A [[:table :A]]
        R [[:from A] [:agg [] [:n [rel/sum :b]]]]
        aid (volatile! 0)
        a (fn [b] {:a (vswap! aid inc), :b b})

        a0 (a 1)
        a1 (a 1)
        a2 (a 1)

        db (rel/materialize {} R)]

    (is (= nil (rel/q db R)))
    (is (= #{{:n 3}} (rel/what-if db R {A [a0 a1 a2]})))
    (is (= #{{:n 2}} (rel/what-if db R {A [a0 a1 a2]} [:delete A a0])))
    (is (= #{{:n 1}} (rel/what-if db R {A [a1]} [:delete A a1] [:insert A a1])))
    (is (= #{{:n 2}} (rel/what-if db R {A [a1, a2]} [:delete A a1] [:insert A a1])))))

(deftest join-test
  (let [A [[:table :A]]
        B [[:table :B]]
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
    (is (= #{} (rel/what-if db R {A [a0]})))
    (is (= #{} (rel/what-if db R {B [b0]})))
    (is (= #{ab0} (rel/what-if {} R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (rel/what-if db R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (rel/what-if db R [:insert B b0], [:insert A a0])))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]})))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))))

(deftest join-product-test
  (let [A [[:table :A]]
        B [[:table :B]]
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
    (is (= #{} (rel/what-if db R {A [a0]})))
    (is (= #{} (rel/what-if db R {B [b0]})))
    (is (= #{ab0} (rel/what-if db R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (rel/what-if db R [:insert B b0], [:insert A a0])))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]})))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))
    (is (= #{ab0, (merge a0 b1)} (rel/what-if db R {A [a0] B [b0, b1]})))
    (is (= #{(merge a0, b0), (merge a0 b1), (merge a1, b0), (merge a1 b1)} (rel/what-if db R {A [a0, a1] B [b0, b1]})))))

(deftest left-join-test
  (let [A [[:table :A]]
        B [[:table :B]]
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
    (is (= #{a0} (rel/what-if db R {A [a0]})))
    (is (= #{} (rel/what-if db R {B [b0]})))
    (is (= #{ab0} (rel/what-if db R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (rel/what-if db R [:insert B b0], [:insert A a0])))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]})))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))
    (is (= #{a0} (rel/what-if db R {A [a0] B [b0]} [:delete B b0])))
    (is (= #{a0} (rel/what-if db R {A [a0]} [:delete B b0])))
    (is (= #{a0, ab1} (rel/what-if db R {A [a0, a1], B [b0, b1]} [:delete B b0])))))

(deftest migrate-table-state-test
  (let [db {}
        A [[:table :A {:req [:a]}]]
        A2 [[:table :A {:req [:a, :b]}]]
        B [[:table :A {:req [:a]}] [:select :b]]
        B2 [[:table :A {:req [:a :b]}] [:select :b]]
        db (rel/transact db {A [{:a 1, :b 1}]})
        db (rel/transact db {A2 [{:a 2, :b 2}]})]

    (is (= #{{:a 1, :b 1}, {:a 2, :b 2}} (rel/q db A)))
    (is (= #{{:b 1} {:b 2}} (rel/q db B)))

    (is (= #{{:a 1, :b 1}, {:a 2, :b 2}} (rel/q db A2)))
    (is (= nil (dataflow/get-id (dataflow/gg db) A2)))
    (is (= #{{:a 1, :b 1}, {:a 2, :b 2}} (rel/q db :A)))
    (is (= #{{:b 1} {:b 2}} (rel/q db B2)))))

(deftest where-lookup-test
  (let [A [[:table :A]]
        db {}]
    (is (= [{:a 42}] (rel/what-if db [[:from A] [:where {:a 42}]] {A [{:a 42}, {:a 43}]})))
    (is (= #{{:a 43}} (-> (rel/materialize db [[:from A] [:where {:a 43}]])
                          (rel/transact {A [{:a 42}, {:a 43}]})
                          (dataflow/gg)
                          (dataflow/get-node [[:from A] [:where {:a 43}]])
                          :results)))

    (is (= [{:a 42}] (rel/what-if db [[:from A] [:where {:a 42} {[:b ::rel/% [::rel/esc ::missing]] ::missing}]] {A [{:a 42}, {:a 43}]})))
    (is (= nil (rel/what-if db [[:from A] [:where {:a 42} {:a 43}]])))
    (is (= nil (rel/what-if db [[:from A] [:where [:or {:a 42} {:a 43}]]])))))

(deftest dematerialize-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A1)]
    (is (= #{{:a 42}} (-> db (dataflow/gg) (dataflow/get-node A1) :results)))
    (is (nil? (-> db (rel/dematerialize A1) (dataflow/gg) (dataflow/get-node A1))))
    (is (= #{{:a 42}} (-> db (rel/dematerialize A1) dataflow/gg :A)))))

(deftest dematerialize-deletes-orphaned-transitives-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        A2 [[:table :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A2)]
    (is (= #{{:a 43}} (-> db dataflow/gg (dataflow/get-node A2) :results)))
    (is (nil? (-> db (rel/dematerialize A2) dataflow/gg (dataflow/get-node A1))))
    (is (= #{{:a 42}} (-> db (rel/dematerialize A2) dataflow/gg :A)))))

(deftest dematerialize-keeps-materialized-transitives-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        A2 [[:table :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A2 A1)]
    (is (= #{{:a 42}} (-> db dataflow/gg (dataflow/get-node A1) :results)))
    (is (= #{{:a 43}} (-> db dataflow/gg (dataflow/get-node A2) :results)))
    (is (some? (-> db (rel/dematerialize A2) dataflow/gg (dataflow/get-node A1))))
    (is (= #{{:a 42}} (-> db (rel/dematerialize A1) dataflow/gg (dataflow/get-node A1) :results)))
    (is (nil? (-> db (rel/dematerialize A2 A1) dataflow/gg (dataflow/get-node A1))))))

(deftest watch-table-test
  (let [A [[:table :A]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/watch db A)
        {db2 :db, :keys [changes]} (rel/track-transact db {A [{:a 43}]})]
    (is (seq changes))
    (is (= {A {:added [{:a 43}], :deleted []}} changes))
    (is (= {:A #{{:a 42}, {:a 43}}} db2))
    (is (= {A {:added [{:a 43}], :deleted [{:a 42}]}} (:changes (rel/track-transact db [:delete A {:a 42}] {A [{:a 43}]}))))))

(deftest watch-view-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/watch db A1)
        {db2 :db, :keys [changes]} (rel/track-transact db {A [{:a 43}]})]
    (is (seq changes))
    (is (= {A1 {:added [{:a 44}], :deleted []}} changes))
    (is (= {:A #{{:a 42}, {:a 43}}} db2))
    (is (= {A1 {:added [{:a 44}], :deleted [{:a 43}]}} (:changes (rel/track-transact db [:delete A {:a 42}] {A [{:a 43}]}))))))

(deftest unwatch-removes-mat-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/watch db A1)
        db (rel/unwatch db A1)]
    (is (nil? (-> db dataflow/gg (dataflow/get-node A1))))))

(deftest unwatch-keeps-explicitly-mat-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        A2 [[:table :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A2)
        db (rel/watch db A2)
        db (rel/unwatch db A2)]

    (is (some? (-> db dataflow/gg (dataflow/get-node A1))))
    (is (= #{{:a 43}} (-> db dataflow/gg (dataflow/get-node A2) :results)))))

(deftest track-transients-are-removed-test
  (is (= {:db {:A #{}}
          :changes {[[:table :A]] {:added [], :deleted []}}}
         (rel/track-transact (rel/watch {} [[:table :A]]) {:A [{:a 1}]} [:delete :A]))))

(deftest update-test
  (let [A [[:table :A]]
        db (rel/transact {} {A [{:a 42} {:a 44}]})]
    (is (= #{{:a 43}, {:a 45}} (rel/what-if db A [:update A {:a [inc :a]}])))
    (is (= #{{:a 43}, {:a 44}} (rel/what-if db A [:update A {:a [inc :a]} {:a 42}])))
    (is (= #{{:a 42}, {:a 44}} (rel/what-if db A [:update A {:a [inc :a]} {:a 41}])))
    (is (= #{{:a 43}, {:a 44}} (rel/what-if db A [:update A {:a 43} [= :a 42]])))
    (is (= #{{:a 43}, {:a 44}} (rel/what-if db A [:update A #(assoc % :a 43) [= :a 42]])))))

(deftest delete-test
  (let [A [[:table :A]]
        db (rel/transact {} {A [{:a 42} {:a 44}]})]
    (is (= #{} (rel/what-if db A [:delete A])))
    (is (= #{{:a 42}} (rel/what-if db A [:delete A {:a 44}])))
    (is (= #{{:a 42}} (rel/what-if db A [:delete A [< 43 :a] [:or [= 1 2] [= 1 1]]])))))

(deftest env-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:extend [:b [str [::rel/env :b]]]]
            [:where [any? [::rel/env :b]]]]]

    (is (= #{{:a 1, :b ""}}
           (-> {}
               (rel/with-env {})
               (rel/transact {A [{:a 1}]})
               (rel/q A2))))

    (is (= #{{:a 1, :b "1"}}
           (-> {}
               (rel/with-env {:b 1})
               (rel/transact {A [{:a 1}]})
               (rel/q A2))))

    (is (= #{{:a 1, :b "1"}}
           (-> {}
               (rel/transact {A [{:a 1}]})
               (rel/with-env {:b 1})
               (rel/q A2))))

    (is (= #{{:a 1, :b "2"}}
           (-> {}
               (rel/transact {A [{:a 1}]})
               (rel/with-env {:b 1})
               (rel/update-env assoc :b 2)
               (rel/q A2))))

    (is (= #{{:a 1, :b ""}}
           (-> {}
               (rel/transact {A [{:a 1}]})
               (rel/with-env {:b 1})
               (rel/update-env dissoc :b)
               (rel/q A2))))))

(deftest top-by-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/top-by 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]

    (is (= [{:top (vec (take 5 (reverse (rows 16))))}] (rel/what-if {} A2 {A (rows 16)})))
    (is (= [{:top (vec (take 5 (reverse (rows 48))))}] (rel/what-if {} A2 {A (rows 48)})))
    (is (= [{:top (vec (take 5 (reverse (rows 72))))}] (rel/what-if {} A2 {A (rows 72)})))
    (is (= [{:top (vec (take 5 (reverse (rows 100))))}] (rel/what-if {} A2 {A (rows 100)})))))

(deftest bottom-by-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/bottom-by 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]

    (is (= [{:top (vec (take 5 (rows 16)))}] (rel/what-if {} A2 {A (rows 16)})))
    (is (= [{:top (vec (take 5 (rows 48)))}] (rel/what-if {} A2 {A (rows 48)})))
    (is (= [{:top (vec (take 5 (rows 72)))}] (rel/what-if {} A2 {A (rows 72)})))
    (is (= [{:top (vec (take 5 (rows 100)))}] (rel/what-if {} A2 {A (rows 100)})))))

(deftest top-by-collision-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/top-by 10 :a]]]
            [:extend [:top [set :top]]]]
        rows (fn [n] (vec (for [i (range n) row [{:a i} {:a i, :b 42}]] row)))]
    (is (= #{{:top (set (take 10 (reverse (rows 16))))}} (rel/what-if {} A2 {A (rows 16)})))
    (is (= #{{:top (set (take 10 (reverse (rows 48))))}} (rel/what-if {} A2 {A (rows 48)})))
    (is (= #{{:top (set (take 10 (reverse (rows 72))))}} (rel/what-if {} A2 {A (rows 72)})))
    (is (= #{{:top (set (take 10 (reverse (rows 100))))}} (rel/what-if {} A2 {A (rows 100)})))))

(deftest bottom-by-collision-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/bottom-by 10 :a]]]
            [:extend [:top [set :top]]]]
        rows (fn [n] (vec (for [i (range n) row [{:a i} {:a i, :b 42}]] row)))]
    (is (= #{{:top (set (take 10 (rows 16)))}} (rel/what-if {} A2 {A (rows 16)})))
    (is (= #{{:top (set (take 10 (rows 48)))}} (rel/what-if {} A2 {A (rows 48)})))
    (is (= #{{:top (set (take 10 (rows 72)))}} (rel/what-if {} A2 {A (rows 72)})))
    (is (= #{{:top (set (take 10 (rows 100)))}} (rel/what-if {} A2 {A (rows 100)})))))

(deftest top-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/top 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]

    (is (= [{:top (vec (take 5 (reverse (range 16))))}] (rel/what-if {} A2 {A (rows 16)})))
    (is (= [{:top (vec (take 5 (reverse (range 48))))}] (rel/what-if {} A2 {A (rows 48)})))
    (is (= [{:top (vec (take 5 (reverse (range 72))))}] (rel/what-if {} A2 {A (rows 72)})))
    (is (= [{:top (vec (take 5 (reverse (range 100))))}] (rel/what-if {} A2 {A (rows 100)})))))

(deftest bottom-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [rel/bottom 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]
    (is (= [{:top (vec (take 5 (range 16)))}] (rel/what-if {} A2 {A (rows 16)})))
    (is (= [{:top (vec (take 5 (range 48)))}] (rel/what-if {} A2 {A (rows 48)})))
    (is (= [{:top (vec (take 5 (range 72)))}] (rel/what-if {} A2 {A (rows 72)})))
    (is (= [{:top (vec (take 5 (range 100)))}] (rel/what-if {} A2 {A (rows 100)})))))

(deftest strip-meta-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        db (rel/transact (rel/materialize {} A1) {A [{:a 42}, {:a 43}]})
        db (rel/watch db A1)]
    (is (= {:A #{{:a 42}, {:a 43}}} (rel/strip-meta db)))
    (is (nil? (meta (rel/strip-meta db))))
    (is (= {:foo 42} (meta (rel/strip-meta (vary-meta db assoc :foo 42)))))))

(deftest unique-violation-test
  (let [db (rel/transact {} {:A [{:a 42}]})
        db (rel/materialize db [[:table :A] [:unique :a]])]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/what-if db :A [:insert :A {:a 42, :b 1}])))
    (is (= #{{:a 42}} (rel/what-if db :A [:insert :A {:a 42}])))))

(deftest upsert-test
  (let [db (rel/transact {} [:upsert :A {:a 42}])
        db (rel/materialize db [[:table :A] [:unique :a]])
        db (rel/transact db [:upsert :A {:a 42, :b 42} {:a 42, :b 43}])]
    (is (= #{{:a 42, :b 43}} (rel/q db :A)))
    (is (= #{{:a 42}, {:a 43}} (rel/what-if db :A [:upsert :A {:a 42}] [:upsert :A {:a 43}] [:upsert :A {:a 42}])))
    (is (= #{{:a 42, :b 43}, {:a 43, :b 43}} (rel/what-if db :A [:upsert :A {:a 43, :b 43}])))))

(deftest delayed-fk-check-test
  (let [db (rel/materialize {} [[:table :A] [:fk [[:table :B]] {:a :a}]])]
    (is (= #{{:a 1}} (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 1}]})))
    (is (= #{{:a 1}} (rel/what-if db :A {:B [{:a 1}]} {:A [{:a 1}]})))
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/what-if db :A {:A [{:a 1}]})))
    (is (thrown? #?(:clj Throwable :cljs js/Error) (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 2}]})))))

(deftest cascading-delete-test
  (let [db (rel/materialize {} [[:table :A] [:fk [[:table :B]] {:a :a} {:cascade true}]])]
    (is (= #{} (rel/what-if db :A {:A [{:a 1}]} {:B [{:a 1}]} [:delete :B])))))

(deftest inline-constraint-test
  (let [tmat (fn [t & tx] (apply rel/transact (rel/materialize {} t) tx))]
    (is (thrown? #?(:clj Throwable :cljs js/Error) (tmat [[:table :A {:req [:a]}]] {:A [{}]})))
    (is (= {:A #{{:a 42}}} (tmat [[:table :A {:req [:a]}]] {:A [{:a 42}]})))
    (is (thrown? #?(:clj Throwable :cljs js/Error) (tmat {[[:table :A {:req [:a], :check [[string? :b]]}]] {:A [{:a 42, :b 'not-a-string}]}})))
    (is (= {:A #{{:a 42, :b "hello"}}} (tmat [[:table :A {:req [:a], :check [[string? :b]]}]] {:A [{:a 42, :b "hello"}]})))
    (is (thrown? #?(:clj Throwable :cljs js/Error) (tmat [[:table :A {:unique [[:a]]}]] {:A [{:a 42} {:a 42, :b 1}]})))
    (is (= {:A #{{:a 42}, {:a 43, :b 1}}} (tmat [[:table :A {:unique [[:a]]}]] {:A [{:a 42} {:a 43, :b 1}]})))
    (is (thrown? #?(:clj Throwable :cljs js/Error) (tmat [[:table :A {:fk [[[[:table :B]] {:a :a}]]}]] {:A [{:a 1}]})))
    (is (= {:A #{{:a 1}}
            :B #{{:a 1}}} (tmat [[:table :A {:fk [[[[:table :B]] {:a :a}]]}]] {:A [{:a 1}], :B [{:a 1}]})))))

(deftest false-req-col-is-allowed-test
  (let [A [[:table :A {:req [:a]}]]
        db (rel/materialize {} A)]
    (is (= {:A #{{:a false}}} (rel/transact db {A [{:a false}]})))))

(deftest self-join-glitch-bug-test1
  (let [A [[:table :A]]
        B [[:from A]
           [:agg
            []
            [:a [count [even? :a]]]]]
        db (rel/materialize {} B)
        db (rel/transact db {A [{:a 1} {:a 2}, {:a 3} {:a 4}]})]

    (is (= #{{:a 2}} (rel/q db B)))
    (is (= #{{:a 2}} (rel/what-if db B [:delete-exact :A {:a 1}])))
    (is (= #{{:a 2}} (rel/what-if db B [:delete-exact :A {:a 1}] {:A [{:a 1}]})))))

(deftest self-join-glitch-bug-test2
  (let [A [[:table :A]]
        B [[:from A]
           [:agg
            []
            [:a [count [even? :a]]]
            [:b count]]]
        db (rel/materialize {} B)
        db (rel/transact db {A [{:a 1} {:a 2}, {:a 3} {:a 4}]})]

    (is (= #{{:a 2, :b 4}} (rel/q db B)))
    (is (= #{{:a 2, :b 3}} (rel/what-if db B [:delete-exact :A {:a 1}])))
    (is (= #{{:a 2, :b 4}} (rel/what-if db B [:delete-exact :A {:a 1}] {:A [{:a 1}]})))))

(deftest narrowing-delete-extend-glitch-test
  (let [A [[:table :A]]
        B [[:from A] [:extend [:a 1]]]
        db (rel/transact (rel/materialize {} B) {:A [{:a 42} {:a 43}]})]
    (is (= #{{:a 1}} (rel/q db B)))
    (is (= #{{:a 1}} (rel/what-if db B [:delete-exact :A {:a 42}])))))

(deftest narrowing-delete-select-glitch-test
  (let [A [[:table :A]]
        B [[:from A] [:select [:a 1]]]
        db (rel/transact (rel/materialize {} B) {:A [{:a 42} {:a 43}]})]
    (is (= #{{:a 1}} (rel/q db B)))
    (is (= #{{:a 1}} (rel/what-if db B [:delete-exact :A {:a 42}])))))

(deftest narrowing-expand-glitch-test
  (let [A [[:table :A]]
        B [[:from A] [:expand [[:b] :b]]]
        db (rel/transact
             (rel/materialize {} B)
             {A [{:b [{:b 1}]}
                 {:b [{:b 1}, {:b 2}]}]})]
    (is (= #{{:b 1} {:b 2}} (rel/q db B)))
    (is (= #{{:b 1}} (rel/what-if db B [:delete-exact A {:b [{:b 1}, {:b 2}]}])))))

(deftest narrowing-join-glitch-test
  (let [A [[:table :A]]
        B [[:table :B]]
        J [[:from A] [:join B {:a :a}]]
        db (rel/transact
             (rel/materialize {} J)
             {A [{:a 1} {:a 1, :b 2}]
              B [{:a 1, :b 2}]})]
    (is (= #{{:a 1 :b 2}} (rel/q db J)))
    (is (= #{{:a 1}} (rel/what-if db A [:delete-exact A {:a 1, :b 2}])))
    (is (= #{{:a 1 :b 2}} (rel/what-if db J [:delete-exact A {:a 1, :b 2}])))))

(deftest narrowing-left-join-glitch-test
  (let [A [[:table :A]]
        B [[:table :B]]
        J [[:from A] [:left-join B {:a :a}]]
        db (rel/transact
             (rel/materialize {} J)
             {A [{:a 1} {:a 1, :b 2}]
              B [{:a 1, :b 2}]})]
    (is (= #{{:a 1 :b 2}} (rel/q db J)))
    (is (= #{{:a 1 :b 2}} (rel/what-if db J [:delete-exact A {:a 1, :b 2}])))))

(comment
  (clojure.test/run-all-tests #"relic"))