(ns examples.real-estate
  "Read along: http://curtclifton.net/papers/MoseleyMarks06a.pdf"
  (:require [com.wotbrew.relic :as r]))

(def Property
  [[:state ::Property]])

(def Offer
  [[:state ::Offer]])

(def Decision
  [[:state ::Decision]])

(def Room
  [[:state ::Room]])

(def Floor
  [[:state ::Floor]])

(def Commission
  [[:state ::Commission]])

(defn- price-band [price]
  (condp >= price
    300000M :low
    650000M :med
    1000000M :high
    :premium))

(defn- area-code [address]
  (second (re-find #"[ \t\n](\d{5})$" address)))

(defn- speed-band [date1 date2]
  (let [day-ms (* 1000 60 60 24)]
    (condp >= (/ (- (inst-ms date1) (inst-ms date2)) day-ms)
      10 :very-fast
      20 :fast
      30 :medium
      60 :slow
      :very-slow)))

(def RoomInfo
  [[:from Room]
   [:extend [:room-size [* :width :breadth]]]])

(def Acceptance
  [[:from Decision]
   [:where :accepted]
   [:project-away :accepted]])

(def Rejection
  [[:from Decision]
   [:where [not :accepted]]
   [:project-away :accepted]])

(def PropertyInfo
  [[:from Property]
   [:extend
    [:price-band [price-band :price]]
    [:area-code [area-code :address]]

    [[:number-of-rooms]
     [r/join-first
      [[:from RoomInfo]
       [:agg [:address] [:number-of-rooms count]]]
      {:address :address}]]

    [[:square-feet]
     [r/join-first
      [[:from RoomInfo]
       [:agg [:address] [:room-size [r/sum :room-size]]]]
      {:address :address}]]]])

(def CurrentOffer
  [[:from Offer]
   [:agg [:address :bidder-name :bidder-address]
    [:latest-date [r/greatest :offer-date]]]
   [:join Offer {:address :address
                 :bidder-name :bidder-name
                 :bidder-address :bidder-address
                 :latest-date :offer-date}]])

(def RawSales
  [[:from Acceptance]
   [:join CurrentOffer {:address :address
                        :bidder-name :bidder-name
                        :bidder-address :bidder-address}]
   [:project-away :offer-date :bidder-name :bidder-address]])

(def SoldProperty
  [[:from RawSales]
   [:project :address]])

(def UnsoldProperty
  [[:from Property]
   [:project :address]
   [:difference SoldProperty]])

(def SalesInfo
  [[:from RawSales]
   [:select
    :address
    :agent
    [:area-code [area-code :address]]
    [:sale-speed [speed-band :date-registered :decision-date]]
    [:price-band [price-band :offer-price]]]])

(def SalesCommissions
  [[:from SalesInfo]
   [:join Commission {:agent :agent}]
   [:project :address :agent :commission]])

;; external

(def OpenOffers
  [[:from CurrentOffer]
   [:join [[:from CurrentOffer]
           [:project-away :offer-price]
           [:difference [[:from Decision]
                         [:project-away :accepted :decision-date]]]]
    {:bidder-name :bidder-name
     :bidder-address :bidder-address
     :address :address}]])

(def PropertyForWebsite
  [[:from UnsoldProperty]
   [:join PropertyInfo {:address :address}]
   [:project :address :price :photo :number-of-rooms :square-feet]])

(def CommissionDue
  [[:from SalesCommissions]
   [:agg [:agent]
    [:total-commission [r/sum :commission]]]
   [:project :agent :total-commission]])

(defn- empty-state
  []
  ;; no share / seperate yet
  (r/materialize {} PropertyInfo))