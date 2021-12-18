(ns examples.real-estate
  "Read along: http://curtclifton.net/papers/MoseleyMarks06a.pdf"
  (:require [com.wotbrew.relic :as rel]
            [clojure.string :as str]))

;; *functional* relational programming
;; just use clojure functions & predicates on rows

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

;; derived relvars

(def RoomInfo
  [[:from ::Room]
   [:extend [:room-size [* :width :breadth]]]])

(def Acceptance
  [[:from ::Decision]
   [:where :accepted]
   [:without :accepted]])

(def Rejection
  [[:from ::Decision]
   [:where [not :accepted]]
   [:without :accepted]])

(def PropertyInfo
  [[:from ::Property]
   [:extend
    [:price-band [price-band :price]]
    [:area-code [area-code :address]]

    [[:number-of-rooms]
     [:$1
      [[:from RoomInfo]
       [:agg [:address] [:number-of-rooms count]]]
      {:address :address}]]

    [[:square-feet]
     [:$1
      [[:from RoomInfo]
       [:agg [:address] [:square-feet [rel/sum :room-size]]]]
      {:address :address}]]]])

(def CurrentOffer
  [[:from ::Offer]
   [:agg [:address :bidder-name :bidder-address]
    [:latest-date [rel/greatest :offer-date]]]
   [:join ::Offer {:address :address
                   :bidder-name :bidder-name
                   :bidder-address :bidder-address
                   :latest-date :offer-date}]])

(def RawSales
  [[:from Acceptance]
   [:join CurrentOffer {:address :address
                        :bidder-name :bidder-name
                        :bidder-address :bidder-address}]
   [:join ::Property {:address :address}]
   [:without :offer-date :bidder-name :bidder-address]])

(def SoldProperty
  [[:from RawSales]
   [:select :address]])

(def UnsoldProperty
  [[:from ::Property]
   [:select :address]
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
   [:join ::Commission {:agent :agent
                        :sale-speed :sale-speed
                        :price-band :price-band}]
   [:select :address :agent :commission]])

;; external

(def OpenOffers
  [[:from CurrentOffer]
   [:join [[:from CurrentOffer]
           [:without :offer-price :latest-date]
           [:difference [[:from ::Decision]
                         [:without :accepted :decision-date]]]]
    {:bidder-name :bidder-name
     :bidder-address :bidder-address
     :address :address}]])

(def PropertyForWebsite
  [[:from UnsoldProperty]
   [:join PropertyInfo {:address :address}]
   [:select :address :price :photo :number-of-rooms :square-feet]])

(def CommissionDue
  [[:from SalesCommissions]
   [:agg [:agent]
    [:total-commission [rel/sum :commission]]]
   [:select :agent :total-commission]])

;; constraints

(def PropertyKey
  [[:from ::Property]
   [:unique :address]])

(def OfferKey
  [[:from ::Offer]
   [:unique :address :offer-date :bidder-name :bidder-address]])

(def DecisionKey
  [[:from ::Decision]
   [:unique :address :offer-date :bidder-name :bidder-address]])

(def RoomKey
  [[:from ::Room]
   [:unique :address :room-name]])

(def FloorKey
  [[:from ::Floor]
   [:unique :address :room-name]])

(def CommissionKey
  [[:from ::Commission]
   [:unique :price-band :area-code :sale-speed]])

(def OfferToProperty
  [[:from ::Offer]
   [:fk ::Property {:address :address}]])

(def DecisionToOffer
  [[:from ::Decision]
   [:fk ::Offer {:address :address
                 :offer-date :offer-date
                 :bidder-name :bidder-name
                 :bidder-address :bidder-address}]])

(def RoomToProperty
  [[:from ::Room]
   [:fk ::Property {:address :address}]])

(def FloorToProperty
  [[:from ::Floor]
   [:fk ::Property {:address :address}]])

(def PropertyHasToHaveAtLeastOneRoom
  [[:from PropertyInfo]
   [:check [:? <= 1 :number-of-rooms]]])

(def CannotBidOnOwnProperty
  [[:from ::Offer]
   [:check [not= :address :bidder-address]]])

(def CannotSubmitOfferOnceSaleAgreed
  [[:from ::Offer]
   [:join [[:from Acceptance] [:select :address :decision-date]] {:address :address}]
   [:check {:pred [:? <= [compare :offer-date :decision-date] 0]
            :error "Offer cannot be submitted after acceptance."}]])

(def NoMoreThan50AdvertisedPremiumProperties
  [[:from PropertyForWebsite]
   [:agg [] [:premium-count [count [= [:_ :premium] [price-band :price]]]]]
   [:check {:pred [:<= :premium-count 50]
            :error "At most 50 properties can be advertised on the website."}]])

(def NoSingleBidderCanSubmitMoreThan10OffersOnAProperty
  [[:from ::Offer]
   [:agg [:address :bidder-address :bidder-name] [:number-of-offers count]]
   [:check {:pred [<= :number-of-offers 10]
            :error "No single bidder can submit more than 10 offers on a property."}]])

(defn constrain [db]
  (rel/materialize
    db
    PropertyKey
    OfferKey
    DecisionKey
    RoomKey
    FloorKey
    CommissionKey

    OfferToProperty
    DecisionToOffer
    RoomToProperty
    FloorToProperty
    PropertyHasToHaveAtLeastOneRoom

    CannotBidOnOwnProperty
    CannotSubmitOfferOnceSaleAgreed
    NoMoreThan50AdvertisedPremiumProperties
    NoSingleBidderCanSubmitMoreThan10OffersOnAProperty))

(defn new-database []
  (-> {}
      (constrain)
      (rel/materialize PropertyInfo)))

(def data
  (let [address1 "abc def 55"
        alice-address "wonderland 42"]
    {::Property [{:address address1
                  :price 344000M
                  :photo "foo.jpg"
                  :agent "bob"
                  :date-registered #inst "2021-10-26"}
                 {:address alice-address
                  :price 1690000M
                  :photo "foo.jpg"
                  :agent "bob"
                  :date-registered #inst "2021-10-27"}]
     ::Offer [{:address address1
               :offer-date #inst "2021-10-27"
               :offer-price 343000M
               :bidder-name "alice"
               :bidder-address alice-address}
              {:address address1
               :offer-date #inst "2021-10-26"
               :offer-price 150000M
               :bidder-name "alice"
               :bidder-address alice-address}]
     ::Decision [{:address address1
                  :offer-date #inst "2021-10-27"
                  :bidder-name "alice"
                  :bidder-address alice-address
                  :decision-date #inst "2021-10-28"
                  :accepted true}]
     ::Room [{:address address1
              :room-name "Room 1"
              :width 10.0M
              :breadth 10.0M
              :type :living-room}
             {:address alice-address
              :room-name "Room 1"
              :width 12.0M
              :breadth 14.5M
              :type :living-room}
             {:address alice-address
              :room-name "Kitchen"
              :width 13.0M
              :breadth 8.0M
              :type :kitchen}]
     ::Floor [{:address address1
               :room-name "Room 1"
               :floor 0}]
     ::Commission [{:agent "bob"
                    :price-band :med
                    :area-code "55"
                    :sale-speed :very-fast
                    :commission 2000.0M}]}))