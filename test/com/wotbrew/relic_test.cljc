(ns com.wotbrew.relic-test
  (:require [clojure.test :refer :all])
  (:require [com.wotbrew.relic :as r]))

(deftest basics-test
  (let [a [[:table :A]]
        b [[:table :B]]
        db (r/transact {} {a #{{:a 42}, {:a 43}, {:a 45}}
                           b #{{:a 42, :b 42}, {:a 42, :b 43}, {:a 43, :b 44}}})]
    (are [x ret]

      (and (= ret (r/q db x))
           (= ret (get (meta (r/materialize db x)) x)))

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
       [:extend [:b (r/esc [:a])]]]
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
      
      [[:from a] [:join b {:a :a}] [:agg [] [:s [r/sum :a :b]]]]
      ;; =>
      #{{:s 256}}

      [[:from a] [:join b {:a :a}]
       [:agg [:a] [:n count]]
       [:select [:x [+ :a :n]]]]
      ;; =>
      #{{:x 44}})))

(deftest base-relvar-test
  (let [A [[:table :A]]]
    (is (r/table-relvar? A))
    (is (not (r/table-relvar? [])))
    (is (not (r/table-relvar? (conj A [:where [= 1 1]]))))))

(deftest state-statement-test
  (let [A [[:table :A]]
        db {}
        a0 {:a 42}
        a1 {:a 43}
        a2 {:a 44}]
    (is (= nil (r/q db A)))
    (is (= db (r/materialize db A)))
    (is (= db (r/transact db {A []})))
    (is (= #{a0} (r/what-if db A {A [a0]})))
    (is (= {:A #{a0}} (r/transact db [:insert A a0])))
    (is (= {:A #{}} (r/transact db [:insert A a0] [:delete A a0])))
    (is (= {} (r/transact db [:delete A a0])))
    (is (= #{a0} (r/what-if db (vec (concat A A)) {A [a0]})))
    (is (= #{a0 a1 a2} (r/what-if db A {A [a0 a1 a2 a0 a1]})))
    (is (= #{a2} (r/what-if db A {A [a2 a2 a2]})))
    (is (= #{a2} (r/what-if db A {A [a0 a1]} {A [a2]} [:delete A a0 a1])))))

(deftest from-statement-test
  (let [A [[:table :A]]
        F [[:from A]]
        a0 {:a 42}
        a1 {:a 43}
        a2 {:a 44}
        db {}]
    (is (= nil (r/q db F)))
    (is (= db (r/materialize db F)))
    (is (= #{a0} (r/what-if db F {A [a0]})))
    (is (= #{a0} (r/what-if db (vec (concat A F)) {A [a0]})))
    (is (= #{a0 a1 a2} (r/what-if db F {A [a0 a1 a2 a0 a1]})))
    (is (= #{a2} (r/what-if db F {A [a2 a2 a2]})))
    (is (= #{a2} (r/what-if db F {A [a0 a1]} {A [a2]} [:delete A a0 a1])))))

(deftest where-expr-test
  (are [result row expr]
    (if result
      (= #{row} (r/what-if {} [[:table :A] [:where expr]] {[[:table :A]] [row]}))
      (empty? (r/what-if {} [[:table :A] [:where expr]] {[[:table :A]] [row]})))

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
    true {:a :b} [= (r/esc :b) :a]
    true {:a 42, :b 42} [= :a :b]
    false {:a 43, :b 42} [= :a :b]
    true {:a 43, :b 42} [= [inc :a] [+ :b 2]]))

(deftest agg-test
  (let [A [[:table :A]]
        R [[:from A] [:agg [] [:n [r/sum :b]]]]
        aid (volatile! 0)
        a (fn [b] {:a (vswap! aid inc), :b b})

        a0 (a 1)
        a1 (a 1)
        a2 (a 1)

        db (r/materialize {} R)]

    (is (= nil (r/q db R)))
    (is (= #{{:n 3}} (r/what-if db R {A [a0 a1 a2]})))
    (is (= #{{:n 2}} (r/what-if db R {A [a0 a1 a2]} [:delete A a0])))
    (is (= #{{:n 1}} (r/what-if db R {A [a1]} [:delete A a1] [:insert A a1])))
    (is (= #{{:n 2}} (r/what-if db R {A [a1, a2]} [:delete A a1] [:insert A a1])))))

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

        db (r/materialize {} R)]

    (is (= nil (r/q db R)))
    (is (= nil (r/what-if db R {A [a0]})))
    (is (= nil (r/what-if db R {B [b0]})))
    (is (= #{ab0} (r/what-if db R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (r/what-if db R [:insert B b0], [:insert A a0])))
    (is (= #{ab0} (r/what-if db R {A [a0] B [b0]})))
    (is (= #{ab0} (r/what-if db R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= #{ab0} (r/what-if db R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))))

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

        db (r/materialize {} R)]

    (is (= nil (r/q db R)))
    (is (= nil (r/what-if db R {A [a0]})))
    (is (= nil (r/what-if db R {B [b0]})))
    (is (= #{ab0} (r/what-if db R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (r/what-if db R [:insert B b0], [:insert A a0])))
    (is (= #{ab0} (r/what-if db R {A [a0] B [b0]})))
    (is (= #{ab0} (r/what-if db R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= #{ab0} (r/what-if db R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))
    (is (= #{ab0, (merge a0 b1)} (r/what-if db R {A [a0] B [b0, b1]})))
    (is (= #{(merge a0, b0), (merge a0 b1), (merge a1, b0), (merge a1 b1)} (r/what-if db R {A [a0, a1] B [b0, b1]})))))

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

        db (r/materialize {} R)]

    (is (= nil (r/q db R)))
    (is (= #{a0} (r/what-if db R {A [a0]})))
    (is (= nil (r/what-if db R {B [b0]})))
    (is (= #{ab0} (r/what-if db R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (r/what-if db R [:insert B b0], [:insert A a0])))
    (is (= #{ab0} (r/what-if db R {A [a0] B [b0]})))
    (is (= #{ab0} (r/what-if db R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= #{ab0} (r/what-if db R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))
    (is (= #{a0} (r/what-if db R {A [a0] B [b0]} [:delete B b0])))
    (is (= #{a0} (r/what-if db R {A [a0]} [:delete B b0])))
    (is (= #{a0, ab1} (r/what-if db R {A [a0, a1], B [b0, b1]} [:delete B b0])))))

(deftest map-unique-index1-nil-test
  (let [i (r/map-unique-index1 {} :a (fn [a b] b))
        nil1 {:a nil}
        nil2 {:a nil, :b 1}
        i1 (r/index-row i nil1)
        i2 (r/index-row i1 nil2)]
    (is (= {nil #{{:a nil}}} i1))
    (is (= {42 {:a 42}, nil #{nil1}} (r/index-row i1 {:a 42})))
    (is (= {} (r/unindex-row i nil1)))
    (is (= {} (r/unindex-row i1 nil1)))
    (is (= [{:a 42} nil1] (r/seek-n (r/index-row i1 {:a 42}) nil)))
    (is (empty? (r/seek-n i [nil])))
    (is (= #{nil1} (r/seek-n i1 [nil])))
    (is (= #{nil1 nil2} (r/seek-n i2 [nil])))))

(deftest migrate-table-state-test
 #_ (let [db {}
        A [[:table :A {:req [:a]}]]
        A2 [[:table :A {:req [:a, :b]}]]
        B [[:table :A {:req [:a]}] [:project :b]]
        B2 [[:table :A {:req [:a :b]}] [:project :b]]
        db (r/transact db {A [{:a 1}]})
        db (r/transact db {A2 [{:a 2, :b 2}]})]

    ;; what should happen too old table references
    (is (= nil (r/q db A)))
    (is (= nil (r/q db B)))

    (is (= #{{:a 1}, {:a 2, :b 2}} (r/q db A2)))
    (is (= #{{:a 1}, {:a 2, :b 2}} ((meta db) A2)))
    (is (= #{{:a 1}, {:a 2, :b 2}} (r/q db :A)))
    (is (= #{{} {:b 2}} (r/q db B2)))))