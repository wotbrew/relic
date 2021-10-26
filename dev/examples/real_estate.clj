(ns examples.real-estate
  "Read along: http://curtclifton.net/papers/MoseleyMarks06a.pdf"
  (:require [com.wotbrew.relic :as r]
            [clojure.string :as str]))

(def Property
  [[:state ::Property
    {:req [:address
           :price
           :photo
           :agent
           :date-registered]}]])

(def Offer
  [[:state ::Offer
    {:req [:address
           :offer-price
           :offer-date
           :bidder-name
           :bidder-address]}]])

(def Decision
  [[:state ::Decision
    {:req [:address
           :offer-date
           :bidder-name
           :bidder-address
           :decision-date
           :accepted]}]])

(def Room
  [[:state ::Room
    {:req [:address
           :room-name
           :width
           :breadth
           :type]}]])

(def Floor
  [[:state ::Floor
    {:req [:address
           :room-name
           :floor]}]])

(def Commission
  [[:state ::Commission
    {:req [:area-code
           :price-band
           :sale-speed
           :commission]}]])

(defn- price-band [price]
  (condp >= price
    300000M :low
    650000M :med
    1000000M :high
    :premium))

(defn- area-code [address]
  ;; we are just pretending
  (last (str/split address #" ")))

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
     (r/join-first
       [[:from RoomInfo]
        [:agg [:address] [:number-of-rooms count]]]
       {:address :address})]

    [[:square-feet]
     (r/join-first
       [[:from RoomInfo]
        [:agg [:address] [:room-size [r/sum :room-size]]]]
       {:address :address})]]])

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
   [:join Property {:address :address}]
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
   [:join Commission {:agent :agent
                      :sale-speed :sale-speed
                      :price-band :price-band}]
   [:project :address :agent :commission]])

;; external

(def OpenOffers
  [[:from CurrentOffer]
   [:join [[:from CurrentOffer]
           [:project-away :offer-price :latest-date]
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

(def db
  (let [address1 "abc def 55"
        alice-address "wonderland 42"]
    (-> {}
        (r/materialize PropertyInfo)
        (r/transact
          {Property [{:address address1
                      :price 344000M
                      :photo "foo.jpg"
                      :agent "bob"
                      :date-registered #inst "2021-10-26"}]
           Offer [{:address address1
                   :offer-date #inst "2021-10-27"
                   :offer-price 343000M
                   :bidder-name "alice"
                   :bidder-address alice-address}
                  {:address address1
                   :offer-date #inst "2021-10-26"
                   :offer-price 150000M
                   :bidder-name "alice"
                   :bidder-address alice-address}]
           Decision [{:address address1
                      :offer-date #inst "2021-10-27"
                      :bidder-name "alice"
                      :bidder-address alice-address
                      :decision-date #inst "2021-10-28"
                      :accepted true}]
           Room [{:address address1
                  :room-name "Room 1"
                  :width 10.0M
                  :breadth 10.0M
                  :type :living-room}]
           Floor [{:address address1
                   :room-name "Room 1"
                   :floor 0}]
           Commission [{:agent "bob"
                        :price-band :med
                        :area-code "55"
                        :sale-speed :very-fast
                        :commission 2000.0M}]}))))

(def q (partial r/query db))