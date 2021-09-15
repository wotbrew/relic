(ns com.wotbrew.relic-test
  (:require [clojure.test :refer :all])
  (:require [com.wotbrew.relic :as r]))

(deftest interpret-test
  (let [a [[:base :A [:a]]]
        b [[:base :B [:b]]]
        st {a #{{:a 42}, {:a 43}, {:a 45}}
            b #{{:a 42, :b 42}, {:a 42, :b 43}, {:a 43, :b 44}}}]
    (is (= #{} (r/interpret {} [])))
    (are [x ret] (= ret (r/interpret st x))
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


      [[:from a] [:join b {:a :a}] [:agg [:a] [:n :<- count]] [:select [:x :<- [+ :a :n]]]]
      ;; =>
      #{{:x 44}})))