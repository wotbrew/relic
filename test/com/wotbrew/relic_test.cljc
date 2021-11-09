(ns com.wotbrew.relic-test
  (:require [clojure.test :refer :all])
  (:require [com.wotbrew.relic :as r]))

(deftest basics-test
  (let [a [[:state :A]]
        b [[:state :B]]
        st (r/transact {} {a #{{:a 42}, {:a 43}, {:a 45}}
                           b #{{:a 42, :b 42}, {:a 42, :b 43}, {:a 43, :b 44}}})]
    (are [x ret]

      (and (= ret (r/q st x))
           (= ret (get (meta (r/materialize st x)) x)))

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
  (let [A [[:state :A]]]
    (is (r/base-relvar? A))
    (is (not (r/base-relvar? [])))
    (is (not (r/base-relvar? (conj A [:where [= 1 1]]))))))

(deftest state-statement-test
  (let [A [[:state :A]]
        st {}
        a0 {:a 42}
        a1 {:a 43}
        a2 {:a 44}]
    (is (= nil (r/q st A)))
    (is (= st (r/materialize st A)))
    (is (= st (r/transact st {A []})))
    (is (= #{a0} (r/what-if st A {A [a0]})))
    (is (= {A #{a0}} (r/transact st [:insert A a0])))
    (is (= {A #{}} (r/transact st [:insert A a0] [:delete A a0])))
    (is (= {} (r/transact st [:delete A a0])))
    (is (= #{a0} (r/what-if st (vec (concat A A)) {A [a0]})))
    (is (= #{a0 a1 a2} (r/what-if st A {A [a0 a1 a2 a0 a1]})))
    (is (= #{a2} (r/what-if st A {A [a2 a2 a2]})))
    (is (= #{a2} (r/what-if st A {A [a0 a1]} {A [a2]} [:delete A a0 a1])))))

(deftest from-statement-test
  (let [A [[:state :A]]
        F [[:from A]]
        a0 {:a 42}
        a1 {:a 43}
        a2 {:a 44}
        st {}]
    (is (= nil (r/q st F)))
    (is (= st (r/materialize st F)))
    (is (= #{a0} (r/what-if st F {A [a0]})))
    (is (= #{a0} (r/what-if st (vec (concat A F)) {A [a0]})))
    (is (= #{a0 a1 a2} (r/what-if st F {A [a0 a1 a2 a0 a1]})))
    (is (= #{a2} (r/what-if st F {A [a2 a2 a2]})))
    (is (= #{a2} (r/what-if st F {A [a0 a1]} {A [a2]} [:delete A a0 a1])))))

(deftest where-expr-test
  (are [result row expr]
    (if result
      (= #{row} (r/what-if {} [[:state :A] [:where expr]] {[[:state :A]] [row]}))
      (empty? (r/what-if {} [[:state :A] [:where expr]] {[[:state :A]] [row]})))

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
  (let [A [[:state :A]]
        R [[:from A] [:agg [] [:n [r/sum :b]]]]
        aid (volatile! 0)
        a (fn [b] {:a (vswap! aid inc), :b b})

        a0 (a 1)
        a1 (a 1)
        a2 (a 1)

        st (r/materialize {} R)]

    (is (= nil (r/q st R)))
    (is (= #{{:n 3}} (r/what-if st R {A [a0 a1 a2]})))
    (is (= #{{:n 2}} (r/what-if st R {A [a0 a1 a2]} [:delete A a0])))
    (is (= #{{:n 1}} (r/what-if st R {A [a1]} [:delete A a1] [:insert A a1])))
    (is (= #{{:n 2}} (r/what-if st R {A [a1, a2]} [:delete A a1] [:insert A a1])))))

(deftest join-test
  (let [A [[:state :A]]
        B [[:state :B]]
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

        st (r/materialize {} R)]

    (is (= nil (r/q st R)))
    (is (= nil (r/what-if st R {A [a0]})))
    (is (= nil (r/what-if st R {B [b0]})))
    (is (= #{ab0} (r/what-if st R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (r/what-if st R [:insert B b0], [:insert A a0])))
    (is (= #{ab0} (r/what-if st R {A [a0] B [b0]})))
    (is (= #{ab0} (r/what-if st R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= #{ab0} (r/what-if st R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))))

(deftest join-product-test
  (let [A [[:state :A]]
        B [[:state :B]]
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

        st (r/materialize {} R)]

    (is (= nil (r/q st R)))
    (is (= nil (r/what-if st R {A [a0]})))
    (is (= nil (r/what-if st R {B [b0]})))
    (is (= #{ab0} (r/what-if st R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (r/what-if st R [:insert B b0], [:insert A a0])))
    (is (= #{ab0} (r/what-if st R {A [a0] B [b0]})))
    (is (= #{ab0} (r/what-if st R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= #{ab0} (r/what-if st R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))
    (is (= #{ab0, (merge a0 b1)} (r/what-if st R {A [a0] B [b0, b1]})))
    (is (= #{(merge a0, b0), (merge a0 b1), (merge a1, b0), (merge a1 b1)} (r/what-if st R {A [a0, a1] B [b0, b1]})))))

(deftest left-join-test
  (let [A [[:state :A]]
        B [[:state :B]]
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

        st (r/materialize {} R)]

    (is (= nil (r/q st R)))
    (is (= #{a0} (r/what-if st R {A [a0]})))
    (is (= nil (r/what-if st R {B [b0]})))
    (is (= #{ab0} (r/what-if st R [:insert A a0], [:insert B b0])))
    (is (= #{ab0} (r/what-if st R [:insert B b0], [:insert A a0])))
    (is (= #{ab0} (r/what-if st R {A [a0] B [b0]})))
    (is (= #{ab0} (r/what-if st R {A [a0] B [b0]} [:delete A a0] [:insert A a0])))
    (is (= #{ab0} (r/what-if st R {A [a0] B [b0]} [:delete B b0] [:insert B b0])))
    (is (= #{a0} (r/what-if st R {A [a0] B [b0]} [:delete B b0])))
    (is (= #{a0, ab1} (r/what-if st R {A [a0, a1], B [b0, b1]} [:delete B b0])))))

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