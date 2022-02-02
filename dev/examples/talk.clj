(ns examples.talk
  (:require [com.wotbrew.relic :as r]))

(def Customer
  [[:table ::Customer]])

(def OrderItem
  [[:table ::OrderItem]])

(def Order
  [[:from OrderItem]
   [:agg
    [:customer :delivery-date]
    [:items [r/set-concat :%]]]])

(def constraints
  [[:from Customer]
   [:constrain
    [:check [string? :customer]]
    [:unique :customer]]

   {:relvar OrderItem
    :unique [:customer :delivery-date :sku]
    :fk {Customer {:customer :customer}}
    :check
    [[inst? :delivery-date]
     [string? :sku]
     [pos-int? :quantity]]}

   {:relvar Order
    :check [{:pred [<= [count :items] 10]
             :error [str "An order can have at most 10 items, found " [count :items]]}]}])

(comment (r/constrain st constraints))


;; relvars are just values, so def them if you want
(def Customer [[:table ::Customer]])

;; you can use relational operators to derive new relvars, join them, aggregate them and so on.
(def FilteredCustomers
  [[:from Customer]
   [:where [= 42 :age]]])

(def customers
  [{:customer "fred", :age 33}
   {:customer "alice", :age 42}])

;; state is just a map, introduce rows with transact.
(def st (r/transact {} {Customer customers}))

;; ask for the value of a relvar (relation) with q
(r/q st Customer)
;; =>
#{{:customer "fred", :age 33}, {:customer "alice", :age 42}}

(r/q st FilteredCustomers)
;; =>
#{{:customer "alice", :age 42}}