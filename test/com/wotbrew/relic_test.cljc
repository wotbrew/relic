(ns com.wotbrew.relic-test
  (:require [clojure.test :refer :all])
  (:require [com.wotbrew.relic :as rel]
            [com.wotbrew.relic :as r]))

(deftest basics-test
  (let [a [[:table :A]]
        b [[:table :B]]
        db (rel/transact {} {a #{{:a 42}, {:a 43}, {:a 45}}
                             b #{{:a 42, :b 42}, {:a 42, :b 43}, {:a 43, :b 44}}})]
    (are [x ret]

      (and (= ret (rel/q db x))
           (= ret (get (meta (rel/materialize db x)) x)))

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
       [:extend [:b [inc :a]]]]
      ;; =>
      #{{:a 42, :b 43}}

      [[:from a]
       [:where [even? :a]]
       [:extend [:b (rel/esc [:a])]]]
      ;; =>
      #{{:a 42, :b [:a]}}

      [[:from a]
       [:where [even? :a]]
       [:expand [:b [range :a [+ :a 2]]]]]
      ;; =>
      #{{:a 42 :b 42} {:a 42, :b 43}}

      [[:from a]
       [:extend [:b [inc :a]]]
       [:project :b]]
      ;; =>
      #{{:b 43} {:b 44}, {:b 46}}

      [[:from a]
       [:extend [:b [inc :a]]]
       [:project-away :a]]
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
      #{{:a 42} {:b 43}})))

(deftest base-relvar-test
  (let [A [[:table :A]]]
    (is (rel/table-relvar? A))
    (is (not (rel/table-relvar? [])))
    (is (not (rel/table-relvar? (conj A [:where [= 1 1]]))))))

(deftest state-statement-test
  (let [A [[:table :A]]
        db {}
        a0 {:a 42}
        a1 {:a 43}
        a2 {:a 44}]
    (is (= nil (rel/q db A)))
    (is (= db (rel/materialize db A)))
    (is (= db (rel/transact db {A []})))
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
      (= #{row} (rel/what-if {} [[:table :A] [:where expr]] {[[:table :A]] [row]}))
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
    true {:a :b} [= (rel/esc :b) :a]
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
    (is (= nil (rel/what-if db R {A [a0]})))
    (is (= nil (rel/what-if db R {B [b0]})))
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
    (is (= nil (rel/what-if db R {A [a0]})))
    (is (= nil (rel/what-if db R {B [b0]})))
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
    (is (= nil (rel/what-if db R {B [b0]})))
    (is (= #{ab0} (rel/what-if db R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (rel/what-if db R [:insert B b0], [:insert A a0])))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]})))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= #{ab0} (rel/what-if db R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))
    (is (= #{a0} (rel/what-if db R {A [a0] B [b0]} [:delete B b0])))
    (is (= #{a0} (rel/what-if db R {A [a0]} [:delete B b0])))
    (is (= #{a0, ab1} (rel/what-if db R {A [a0, a1], B [b0, b1]} [:delete B b0])))))

(deftest map-unique-index1-nil-test
  (let [i (rel/map-unique-index1 {} :a (fn [a b] b))
        nil1 {:a nil}
        nil2 {:a nil, :b 1}
        i1 (rel/index-row i nil1)
        i2 (rel/index-row i1 nil2)]
    (is (= {nil #{{:a nil}}} i1))
    (is (= {42 {:a 42}, nil #{nil1}} (rel/index-row i1 {:a 42})))
    (is (= {} (rel/unindex-row i nil1)))
    (is (= {} (rel/unindex-row i1 nil1)))
    (is (= [{:a 42} nil1] (rel/seek-n (rel/index-row i1 {:a 42}) nil)))
    (is (empty? (rel/seek-n i [nil])))
    (is (= #{nil1} (rel/seek-n i1 [nil])))
    (is (= #{nil1 nil2} (rel/seek-n i2 [nil])))))

(deftest migrate-table-state-test
  #_ (let [db {}
           A [[:table :A {:req [:a]}]]
           A2 [[:table :A {:req [:a, :b]}]]
           B [[:table :A {:req [:a]}] [:project :b]]
           B2 [[:table :A {:req [:a :b]}] [:project :b]]
           db (rel/transact db {A [{:a 1}]})
           db (rel/transact db {A2 [{:a 2, :b 2}]})]

       ;; what should happen too old table references
       (is (= nil (rel/q db A)))
       (is (= nil (rel/q db B)))

       (is (= #{{:a 1}, {:a 2, :b 2}} (rel/q db A2)))
       (is (= #{{:a 1}, {:a 2, :b 2}} ((meta db) A2)))
       (is (= #{{:a 1}, {:a 2, :b 2}} (rel/q db :A)))
       (is (= #{{} {:b 2}} (rel/q db B2)))))

(deftest where-lookup-test
  (let [A [[:table :A]]
        db {}]
    (is (= #{{:a 42}} (rel/what-if db [[:from A] [:where {:a 42}]] {A [{:a 42}, {:a 43}]})))
    (is (= #{{:a 43}} (-> (rel/materialize db [[:from A] [:where {:a 43}]])
                          (rel/transact {A [{:a 42}, {:a 43}]})
                          meta
                          (get [[:from A] [:where {:a 43}]]))))

    (is (= #{{:a 42}} (rel/what-if db [[:from A] [:where {:a 42} {[:b ::rel/% (rel/esc ::missing)] ::missing}]] {A [{:a 42}, {:a 43}]})))
    (is (= nil (rel/what-if db [[:from A] [:where {:a 42} {:a 43}]])))
    (is (= nil (rel/what-if db [[:from A] [:where [:or {:a 42} {:a 43}]]])))))

(deftest dematerialize-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A1)]
    (is (= #{{:a 42}} (-> db meta (get A1))))
    (is (not (contains? (-> db (rel/dematerialize A1) meta) A1)))
    (is (= #{{:a 42}} (-> db (rel/dematerialize A1) meta (get A))))))

(deftest dematerialize-deletes-orphaned-transitives-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        A2 [[:table :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A2)]
    (is (= #{{:a 43}} (-> db meta (get A2))))
    (is (not (contains? (-> db (rel/dematerialize A2) meta) A1)))
    (is (= #{{:a 42}} (-> db (rel/dematerialize A2) meta (get A))))))

(deftest dematerialize-keeps-materialized-transitives-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        A2 [[:table :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A2 A1)]
    (is (= #{{:a 42}} (-> db meta (get A1))))
    (is (= #{{:a 43}} (-> db meta (get A2))))
    (is (contains? (-> db (rel/dematerialize A2) meta) A1))
    (is (= #{{:a 42}} (-> db (rel/dematerialize A1) meta (get A1))))
    (is (not (contains? (-> db (rel/dematerialize A2 A1) meta) A1)))))

(deftest watch-table-test
  (let [A [[:table :A]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/watch db A)
        {:keys [changes, result]} (rel/track-transact db {A [{:a 43}]})]
    (is (seq changes))
    (is (= {A {:added [{:a 43}], :deleted []}} changes))
    (is (= {:A #{{:a 42}, {:a 43}}} result))
    (is (= {A {:added [{:a 43}], :deleted [{:a 42}]}} (:changes (rel/track-transact db [:delete A {:a 42}] {A [{:a 43}]}))))))

(deftest watch-view-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/watch db A1)
        {:keys [changes, result]} (rel/track-transact db {A [{:a 43}]})]
    (is (seq changes))
    (is (= {A1 {:added [{:a 44}], :deleted []}} changes))
    (is (= {:A #{{:a 42}, {:a 43}}} result))
    (is (= {A1 {:added [{:a 44}], :deleted [{:a 43}]}} (:changes (rel/track-transact db [:delete A {:a 42}] {A [{:a 43}]}))))))

(deftest unwatch-removes-mat-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/watch db A1)
        db (rel/unwatch db A1)]
    (is (not (contains? (-> db meta) A1)))))

(deftest unwatch-keeps-explicitly-mat-test
  (let [A [[:table :A]]
        A1 [[:table :A] [:where [= :a 42]]]
        A2 [[:table :A] [:where [= :a 42]] [:extend [:a [inc :a]]]]
        db (rel/transact {} {A [{:a 42}]})
        db (rel/materialize db A2)
        db (rel/watch db A2)
        db (rel/unwatch db A2)]

    (is (some? (-> db meta ::rel/graph (get A1))))
    (is (= #{{:a 43}} (-> db meta (get A2))))))

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
            [:agg [] [:top [r/top-by 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]

    (is (= #{{:top (vec (take 5 (reverse (rows 16))))}} (rel/what-if {} A2 {A (rows 16)})))
    (is (= #{{:top (vec (take 5 (reverse (rows 48))))}} (rel/what-if {} A2 {A (rows 48)})))
    (is (= #{{:top (vec (take 5 (reverse (rows 72))))}} (rel/what-if {} A2 {A (rows 72)})))
    (is (= #{{:top (vec (take 5 (reverse (rows 100))))}} (rel/what-if {} A2 {A (rows 100)})))))

(deftest bottom-by-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [r/bottom-by 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]

    (is (= #{{:top (vec (take 5 (rows 16)))}} (rel/what-if {} A2 {A (rows 16)})))
    (is (= #{{:top (vec (take 5 (rows 48)))}} (rel/what-if {} A2 {A (rows 48)})))
    (is (= #{{:top (vec (take 5 (rows 72)))}} (rel/what-if {} A2 {A (rows 72)})))
    (is (= #{{:top (vec (take 5 (rows 100)))}} (rel/what-if {} A2 {A (rows 100)})))))

(deftest top-by-collision-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [r/top-by 10 :a]]]
            [:extend [:top [set :top]]]]
        rows (fn [n] (vec (for [i (range n) row [{:a i} {:a i, :b 42}]] row)))]
    (is (= #{{:top (set (take 10 (reverse (rows 16))))}} (rel/what-if {} A2 {A (rows 16)})))
    (is (= #{{:top (set (take 10 (reverse (rows 48))))}} (rel/what-if {} A2 {A (rows 48)})))
    (is (= #{{:top (set (take 10 (reverse (rows 72))))}} (rel/what-if {} A2 {A (rows 72)})))
    (is (= #{{:top (set (take 10 (reverse (rows 100))))}} (rel/what-if {} A2 {A (rows 100)})))))

(deftest bottom-by-collision-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [r/bottom-by 10 :a]]]
            [:extend [:top [set :top]]]]
        rows (fn [n] (vec (for [i (range n) row [{:a i} {:a i, :b 42}]] row)))]
    (is (= #{{:top (set (take 10 (rows 16)))}} (rel/what-if {} A2 {A (rows 16)})))
    (is (= #{{:top (set (take 10 (rows 48)))}} (rel/what-if {} A2 {A (rows 48)})))
    (is (= #{{:top (set (take 10 (rows 72)))}} (rel/what-if {} A2 {A (rows 72)})))
    (is (= #{{:top (set (take 10 (rows 100)))}} (rel/what-if {} A2 {A (rows 100)})))))

(deftest top-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [r/top 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]

    (is (= #{{:top (vec (take 5 (reverse (range 16))))}} (rel/what-if {} A2 {A (rows 16)})))
    (is (= #{{:top (vec (take 5 (reverse (range 48))))}} (rel/what-if {} A2 {A (rows 48)})))
    (is (= #{{:top (vec (take 5 (reverse (range 72))))}} (rel/what-if {} A2 {A (rows 72)})))
    (is (= #{{:top (vec (take 5 (reverse (range 100))))}} (rel/what-if {} A2 {A (rows 100)})))))

(deftest bottom-test
  (let [A [[:table :A]]
        A2 [[:from A]
            [:agg [] [:top [r/bottom 5 :a]]]]
        rows (fn [n] (vec (for [i (range n)] {:a i})))]
    (is (= #{{:top (vec (take 5 (range 16)))}} (rel/what-if {} A2 {A (rows 16)})))
    (is (= #{{:top (vec (take 5 (range 48)))}} (rel/what-if {} A2 {A (rows 48)})))
    (is (= #{{:top (vec (take 5 (range 72)))}} (rel/what-if {} A2 {A (rows 72)})))
    (is (= #{{:top (vec (take 5 (range 100)))}} (rel/what-if {} A2 {A (rows 100)})))))