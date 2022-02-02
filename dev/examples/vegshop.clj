(ns examples.vegshop
  (:require [com.wotbrew.relic :as rel]))

(defn flatten-availability [availability]
  (for [[date in-stock] availability]
    {:availability/date date
     :availability/in-stock in-stock}))

(def ProductAvailability
  [[:from :Product]
   [:expand [[:availability/date
              :availability/in-stock]
             [flatten-availability :product/availability]]]
   [:without :product/availability]])

(def Item
  [[:from :Item]
   [:join :Product {:item/product :product/sku}]
   [:left-join ProductAvailability {:item/product :product/sku, :order/delivery-date :availability/date}]
   [:extend [:item/price [:if :availability/in-stock [* :item/quantity :product/price]]]]
   [:select
    :order/delivery-date
    :order/customer
    :item/product
    :item/quantity
    :item/price
    :product/type
    :product/price
    :availability/in-stock]])

(defn- recipe-discount [item-count]
  (cond
    (<= 3 item-count) 10
    (<= 2 item-count) 5
    :else 0))

(def Order
  [[:from Item]
   [:agg [:order/customer
          :order/delivery-date]

    [:order/price [rel/sum :item/price]]
    [:order/items [rel/set-concat [select-keys :% #{:item/product :item/quantity :item/price :availability/in-stock :product/price}]]]
    [:recipe-discount/eligible-items [rel/sum [:if [:and :availability/in-stock [= :product/type "recipe"]] :item/quantity]]]
    [:minspend/has-meat [rel/any [:and :availability/in-stock [= :product/type "meat"]]]]
    [:minspend/has-recipe [rel/any [:and :availability/in-stock [= :product/type "recipe"]]]]
    [:minspend/extras-spend [rel/sum [:if [:and :availability/in-stock [= :product/type "extra"]] :item/price]]]
    [:minspend/meat-spend [rel/sum [:if [:and :availability/in-stock [= :product/type "meat"]] :item/price]]]]

   [:extend
    [:recipe-discount/percentage [recipe-discount :recipe-discount/eligible-items]]
    [:order/discount [min :order/price [* 0.01M :recipe-discount/percentage :order/price]]]
    [:order/total [- :order/price :order/discount]]
    [:minspend/valid
     [boolean
      [:or :minspend/has-recipe
       [:if :minspend/has-meat
        [<= 15.0M :minspend/meat-spend]
        [<= 15.0M :minspend/extras-spend]]]]]]

   [:select

    :order/customer
    :order/delivery-date
    :order/total
    :order/discount
    :order/price
    :order/items

    :minspend/valid]])

(def NextOrder
  [[:from Order]
   [:where [::rel/env :now] [< [::rel/env :now] :order/delivery-date]]
   [:agg [:order/customer] [:order/delivery-date [min :order/delivery-date]]]
   [:join Order {:order/delivery-date :order/delivery-date :order/customer :order/customer}]])

(comment
  (def data
    {:Item
     (->> [{:c "fred"
            :d 0
            :items {"Egg" 2
                    "Bacon" 5
                    "Recipe1" 2}}
           {:c "fred"
            :d 1
            :items {"Egg" 2
                    "Bacon" 5
                    "Recipe1" 2}}
           {:c "fred"
            :d 2
            :items {"Egg" 2
                    "Bacon" 5
                    "Recipe1" 2}}
           {:c "alice"
            :d 2
            :items {"Bacon" 3
                    "Egg" 2
                    "Recipe1" 1}}]
          (mapcat (fn [{:keys [c, d, items]}]
                    (for [[i q] items]
                      {:order/customer c
                       :order/delivery-date d
                       :item/product i
                       :item/quantity q}))))
     :Product
     (->> [{:p "Bacon"
            :t "meat"
            :pr 3.50
            :a {0 true
                1 true
                2 true}}
           {:p "Recipe1"
            :t "recipe"
            :pr 10.0M
            :a {0 true
                1 false
                2 true}}
           {:p "Egg"
            :t "extra"
            :pr 1.69M
            :a {0 true
                1 true
                2 false}}]
          (map (fn [{:keys [p, pr, t, a]}]
                 {:product/sku p
                  :product/type t
                  :product/price pr
                  :product/availability a})))})

  (def db {})

  (rel/what-if (rel/with-env db {:now -1}) NextOrder data))