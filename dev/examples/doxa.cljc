(ns examples.doxa
  (:require [com.wotbrew.relic :as r]
            [datascript.core :as d]))

(let [next-eid (volatile! 0)]

  (defn random-man []
    {:db/id     (vswap! next-eid inc)
     :man/name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
     :man/last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
     :man/alias     (vec
                      (repeatedly (rand-int 10) #(rand-nth ["A. C. Q. W." "A. J. Finn" "A.A. Fair" "Aapeli" "Aaron Wolfe" "Abigail Van Buren" "Jeanne Phillips" "Abram Tertz" "Abu Nuwas" "Acton Bell" "Adunis"])))
     :man/age       (rand-int 100)
     :man/sex       (rand-nth [:male :female])
     :man/salary    (rand-int 100000)
     :man/friend    {:db/ref-id (rand-int 20000)}})

  (defn random-fruit []
    (let [n (rand-nth ["Avocado" "Grape" "Plum" "Apple" "Orange"])]
      {:fruit/id (vswap! next-eid inc)
       :fruit/name n
       :fruit/name2 n
       :fruit/price (rand-int 100)}))

  (defn random-vegetable []
    {:vegetable/id     (vswap! next-eid inc)
     :vegetable/name      (rand-nth ["Onion" "Cabbage" "Pea" "Tomatto" "Lettuce"])
     :vegetable/price     (rand-int 100)})

  (defn random-car []
    {:car/id     (vswap! next-eid inc)
     :car/name      (rand-nth ["Audi" "Mercedes" "BMW" "Ford" "Honda" "Toyota"])
     :car/price     (rand-int 100)})

  (defn random-animal []
    {:animal/id     (vswap! next-eid inc)
     :animal/name      (rand-nth ["Otter" "Dog" "Panda" "Lynx" "Cat" "Lion"])
     :animal/price     (rand-int 100)})

  (defn random-cat []
    {:cat/id     (vswap! next-eid inc)
     :cat/name      (rand-nth ["Traditional Persian" "Ocicat" "Munchkin cat" "Persian cat" "Burmese cat"])
     :cat/price     (rand-int 100)})

  (defn random-dog []
    {:dog/id     (vswap! next-eid inc)
     :dog/name      (rand-nth ["Croatian Shepherd" "Deutch Langhaar" "Miniature Pincher" "Italian Sighthound" "Jack Russell Terrier"])
     :dog/price     (rand-int 100)})

  (defn random-country []
    {:country/id     (vswap! next-eid inc)
     :country/name      (rand-nth ["Seychelles" "Greenland" "Iceland" "Bahrain" "Bhutan"])
     :country/price     (rand-int 100)})

  (defn random-language []
    {:language/id     (vswap! next-eid inc)
     :language/name      (rand-nth ["Malagasy" "Kashmiri" "Amharic" "Inuktitut" "Esperanto"])
     :language/price     (rand-int 100)})

  (defn random-planet []
    {:planet/id     (vswap! next-eid inc)
     :planet/name      (rand-nth ["Pluto" "Saturn" "Venus" "Mars" "Jupyter"])
     :planet/price     (rand-int 100)}))

(def people           (repeatedly random-man))
(def fruit            (repeatedly random-fruit))
(def vegetable        (repeatedly random-vegetable))
(def car              (repeatedly random-car))
(def animal           (repeatedly random-animal))
(def cat              (repeatedly random-cat))
(def dog              (repeatedly random-dog))
(def country          (repeatedly random-country))
(def language         (repeatedly random-language))

(def planet           (repeatedly random-planet))

(def people50k           (shuffle (take 50000 people)))

(def fruit10k            (shuffle (take 10000 fruit)))
(def vegetable10k        (shuffle (take 10000 vegetable)))
(def car10k              (shuffle (take 10000 car)))
(def animal10k           (shuffle (take 10000 animal)))
(def cat10k              (shuffle (take 10000 cat)))
(def dog10k              (shuffle (take 10000 dog)))
(def country10k          (shuffle (take 10000 country)))
(def language10k         (shuffle (take 10000 language)))
(def planet10k           (shuffle (take 10000 planet)))

(def People [[:table ::People]])
(def Fruit [[:table ::Fruit]])
(def Veg [[:table ::Veg]])
(def Car [[:table ::Car]])
(def Animal [[:table ::Animal]])
(def Cat [[:table ::Cat]])
(def Dog [[:table ::Dog]])
(def Country [[:table ::Country]])
(def Planet [[:table ::Planet]])
(def Language [[:table ::Language]])

(def data100k
  (->> {People people50k
        Fruit fruit10k
        Veg vegetable10k
        Car car10k
        Animal animal10k
        Cat cat10k
        Dog dog10k
        Country country10k
        Language language10k
        Planet planet10k}
       (map (fn [[k v]] [k (vec v)]))
       (into {})))

(def db
  (-> {}
      (r/mat People Fruit Veg Car Animal Cat Dog Country Planet (conj Fruit [:hash :fruit/name2]))))

(def db
  (do (println "init")
      (time (r/transact db data100k))))

(def ds-schema
  {:fruit/name2 {:db/index true}})

(def ds
  (d/create-conn ds-schema))

(def data100kds
  (let [eid-ctr (volatile! 0)]
    (vec
      (for [[_ rows] data100k
            row rows]
        (assoc row :db/id (vswap! eid-ctr inc))))))

(do (println "init (ds)")
    (time (d/transact ds data100kds))
    nil)


(defn qscan []
  (count (r/q db [[:from Fruit]
                  [:where
                   [= :fruit/name "Avocado"]
                   [< :fruit/price 50]]])))

(defn dscan []
  (count (d/q '[:find ?e
                :where
                [?e :fruit/name "Avocado"]
                [?e :fruit/price ?price]
                [(< ?price 50)]]
              @ds)))

(defn qidx []
  (count (r/q db [[:from Fruit]
                  [:where
                   [= :fruit/name2 "Avocado"]
                   [< :fruit/price 50]]])))

(defn didx []
  (count (d/q '[:find ?e
                :where
                [?e :fruit/name2 "Avocado"]
                [?e :fruit/price ?price]
                [(< ?price 50)]]
              @ds)))