(ns examples.git
  (:require [clojure.java.shell :as sh]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [com.wotbrew.relic :as rel])
  (:import (java.io StringReader)))

(def dir "/Users/danielstone/code/wotbrew/tarpit")

(defn- log
  ([] (log nil))
  ([since]
   (let [{:keys [out]} (sh/sh "git" "log" "--date=unix" "--since" (str (or since "1")) dir)]
     (loop [acc []
            buf nil
            lines (line-seq (io/reader (StringReader. out)))]
       (if-some [line (first lines)]
         (let [line (str/triml line)
               [_ commit] (re-matches #"commit (.{40})" line)
               [_ merge] (re-matches #"Merge:\s+(.+)" line)
               [_ author] (re-matches #"Author\:\s+(.+)" line)
               [_ date] (re-matches #"Date\:\s+(.+)" line)]
           (cond
             commit (recur (if buf (conj acc buf) acc) {:commit commit} (rest lines))
             author (recur acc (assoc buf :author author) (rest lines))
             date (recur acc (assoc buf :unix-ts (Long/parseLong date)) (rest lines))
             merge (recur acc (assoc buf :merge (str/split merge #"\s+")) (rest lines))
             (str/blank? line) (recur acc buf (rest lines))
             :else (recur acc (update buf :msg (fn [s] (if s (str s "\n" line) line))) (rest lines))))
         (if buf
           (conj acc buf)
           acc))))))

(defn db [] (rel/transact {} {:Commit (log)}))

(defn- short-sha [s] (apply str (take 7 s)) )

(def CommitInfo
  [[:from :Commit]
   [:extend [:short-sha [short-sha :commit]]]])

(def LastCommit
  [[:from :Commit]
   [:sort-limit 1 [:unix-ts :desc]]])

(def Author
  [[:from :Commit]
   [:where :author]
   [:select :author]])

(def AuthorInfo
  [[:from Author]
   [:extend
    [:name [second [re-find #"(.+) \<" :author]]]
    [:email [second [re-find #".+\<(.+)\>" :author]]]]])

(def AuthorCommitStats
  [[:from Author]
   [:join :Commit {:author :author}]
   [:agg [:author]
    [:commits [count]]
    [:merge-commits [count :merge]]]])

(defn poll [db]
  (let [{:keys [unix-ts]} (first (rel/q db LastCommit))]
    {:Commit (log unix-ts)}))