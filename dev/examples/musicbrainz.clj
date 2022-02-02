(ns examples.musicbrainz
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [com.wotbrew.relic :as r]))

(defn- filename []
  (str (System/getenv "HOME") "/musicbrainzsample.csv"))

(defrecord TrackStateR [band country genre track album length ^double score year])

(defn read-csv []
  (with-open [reader (io/reader (filename))]
    (->> (csv/read-csv reader)
         (mapv (fn [[band, country, genre, album, track, length, score, year]]
                 (map->TrackStateR {:band band
                                    :country country
                                    :genre genre
                                    :track track
                                    :album album
                                    :length length
                                    :score (double (bigdec score))
                                    :year year}))))))

(def Track [[:table ::Track {:req [:band
                                   :country
                                   :genre
                                   :track
                                   :album
                                   :length
                                   :score
                                   :year]}]])

(defn top-track [] (r/max-by :score))

(def SelfTitledAlbum
  [[:from Track]
   [:where [= :band :album]]])

(def Band
  [[:from Track]
   [:agg [:band :country]
    [:band-score [r/sum :score]]
    [:top-track top-track]]])

(def Genre
  [[:from Track]
   [:join Band {:band :band}]
   [:agg [:genre]
    [:n-tracks count]
    [:score [r/sum :score]]
    [:top-track top-track]]])

(def Country
  [[:from Track]
   [:join Band {:band :band}]
   [:agg [:country]
    [:n-tracks count]
    [:score [r/sum :score]]
    [:top-track top-track]]])

(defn normalize [data]
  (-> {}
      (r/transact {Track data})))

(def st (atom (normalize (read-csv))))

(defn materialize! [& relvars] (time (apply swap! st r/mat relvars)) nil)

(defn transact! [& tx] (apply swap! st r/transact tx) nil)

(defn q
  [q]
  (r/q @st q))