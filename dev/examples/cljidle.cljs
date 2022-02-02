(ns examples.cljidle
  "Clojure Idle is a meme idle game to demonstrate the use of relic in the browser."
  (:require [com.wotbrew.relic :as rel]
            [reagent.dom :as rdom]
            [reagent.core :as r]))

;; ----
;; reference data

(def seed-state
  "This is the seed data for our database."
  {:Player [{:money 0, :wealth 0, :completed 0}]
   :Project [{:name "Install Clojure", :base-reward 10.0 :base-difficulty 10}]})

(def projects
  "The list of possible projects, an element is selected from random every time you complete a project."
  [{:name "Bug fix", :base-difficulty 10, :base-reward 10}
   {:name "Feature Request", :base-difficulty 15, :base-reward 20}
   {:name "Demo", :base-difficulty 25, :base-reward 15}
   {:name "Documentation", :base-difficulty 30, :base-reward 20}
   {:name "Improve tests", :base-difficulty 20, :base-reward 25}])

(def improvements
  "The list of possible improvements, to generate parens and $$$."
  [{:name "Keyboard macros"
    :clicks 1
    :base-cost 10}
   {:name "(repeatedly (write-code))"
    :pps 0.1
    :base-cost 30}
   {:name "Buy a macbook pro"
    :pps 1.0
    :base-cost 400}
   {:name "Write a component library"
    :pps 2.0
    :base-cost 1500}
   {:name "Create SaaS"
    :base-cost 10000
    :passive-income 100.0}
   {:name "Macros that write macros"
    :pps 20.0
    :base-cost 700000}])

(def upgrades
  "The list of possible upgrades, to improve your improvements."
  [{:name "Rewatch simple made easy"
    :cost 100
    :pps-mul 2.0}
   {:name "Fresh init.el"
    :cost 1000
    :pps-mul 2.0}
   {:name "Go to the conj"
    :cost 10000
    :pps-mul 4.0}])

;; -----
;; essential state and constraints

(def PlayerSchema
  "The player table will hold some counters"
  [[:from :Player]
   [:check

    ;; the amount of money we have to spend
    [number? :money]
    [<= 0 :money]

    ;; the amount of money we have to spend + in assets
    [number? :wealth]
    [<= 0 :wealth]

    ;; the number of projects completed
    [nat-int? :completed]]])

(defn exactly-one-row
  "Returns a constraint for query 'q', that ensures exactly one row exists."
  [q]
  [[:from q]
   [:agg [] [:n count]]
   [:check [= :n 1]]])

(def ExactlyOnePlayer (exactly-one-row :Player))

(def ProjectSchema
  "The product table will hold the project being worked on."
  [[:from :Project]
   [:check

    ;; the name of the project
    [string? :name]

    ;; used to determine the reward for completion
    [number? :base-reward]
    [pos? :base-reward]

    ;; used to determine how many parens are needed to complete the project
    [number? :base-difficulty]
    [pos? :base-difficulty]]])

(def ExactlyOneProject (exactly-one-row :Project))

(def WatchSchema
  "Watch is going to be used by our reagent integration to invalid reagent atoms as query results change."
  [[:from :Watch]
   [:constrain
    [:check
     [some? :q]
     [some? :ratom]]
    ;; only one row per :q, reuse the same atom if it is the same query.
    [:unique :q]]])

(def EffectSchema
  "Effect handlers are callbacks that will be invoked by our reagent integration when rows are added/deleted to a query.

  f will be called like this (f db added deleted)."
  [[:from :Effect]
   [:constrain
    [:check
     [some? :key]
     [some? :q]
     [fn? :f]]
    ;; only one effect per :key, so we can remove them later
    [:unique :key]]])

(def PlayerUpgradeSchema
  "Upgrades are purchased by inserting a row into the :Upgrade table."
  [[:from :Upgrade]
   [:constrain
    [:check [contains? (set (map :name upgrades)) :name]]
    [:unique :name]]])

(def PlayerImprovementSchema
  "Improvements are purchased by inserting or updating a row into the :Improvement table, with a :name and :qty. "
  [[:from :Improvement]
   [:constrain
    [:check
     [contains? (set (map :name improvements)) :name]
     [number? :qty]
     [pos? :qty]]

    [:unique :name]]])

;; ----
;; derived data

(defn- project-scale-factor [completed-projects]
  (+ 1 (long (/ completed-projects 5))))

(def ProjectScale
  [[:from :Player]
   [:select [:project-scale-factor [project-scale-factor :completed]]]])

(def Project
  [[:from :Project]
   [:join ProjectScale]
   [:select
    :name
    :progress
    ;; scale the reward / difficulty with the number of completed projects
    ;; so number goes up.
    [:reward [* :project-scale-factor :base-reward]]
    [:difficulty [* :project-scale-factor :base-difficulty]]
    ;;
    [:finished [>= :progress :difficulty]]]])

(def Upgrade
  [[:const upgrades]
   [:join :Player]
   [:select
    :name
    :cost
    [:affordable [<= :cost :money]]
    [:unlocked [some? [rel/sel1 :Upgrade {:name :name}]]]
    [:show [:and [not :unlocked] [<= :cost [* :wealth 2]]]]
    [:active-pps-mul [:if :unlocked :pps-mul]]]])

(def ParenMultiplier
  [[:from Upgrade]
   [:agg [] [:pps-mul [rel/sum :active-pps-mul]]]
   [:select [:pps-mul [max 1.0 :pps-mul]]]])

(defn- improvement-description [im]
  (cond
    (:pps im) (str "+" (:unit-pps im) " parens per second")
    (:clicks im) (str "+" (:clicks im) " parens per click")
    (:passive-income im) (str "+$" (:passive-income im) " per second")))

(defn improvement-cost [base-cost qty]
  (+ base-cost (* 0.1 base-cost qty)))

(def Improvement
  [[:const improvements]
   [:join :Player {} ParenMultiplier {}]
   [:left-join :Improvement {:name :name}]
   [:extend
    [:cost [improvement-cost :base-cost :qty]]
    [:show [<= :cost [* :wealth 2]]]
    [:affordable [<= :cost :money]]
    [:qty [:or :qty 0]]
    [:unit-pps [:? * :pps-mul :pps]]
    [:active-pps [:? * :qty :unit-pps]]
    [:active-passive-income [:? * :qty :passive-income]]
    [:click-boost [:? * :qty :clicks]]
    [:desc improvement-description]]])

(def ShownImprovement
  [[:from Improvement]
   [:where :show]
   [:sort [:base-cost :asc]]])

(def ShownUpgrade
  [[:from Upgrade]
   [:where :show]
   [:sort [:cost :asc]]])

(def Stats
  [[:from Improvement]
   [:union Upgrade]
   [:agg []
    [:pps [rel/sum :active-pps]]
    [:income [rel/sum :active-passive-income]]
    [:click-boost [rel/sum :click-boost]]]])

;; ----
;; here we materialize just the constraints

(def state
  (atom
    (-> {}
        (rel/transact seed-state)
        (rel/mat
          WatchSchema
          EffectSchema

          PlayerSchema
          ProjectSchema

          ExactlyOnePlayer
          ExactlyOneProject

          PlayerImprovementSchema
          PlayerUpgradeSchema))))

;; ----
;; our reagent integration
;; using :Watch and :Effect

(defn- fire-signals! [db changes]
  (let [feedback (volatile! [])]
    (reduce-kv
      (fn [_ q {:keys [added deleted]}]
        (when (or (seq added)
                  (seq deleted))
          (doseq [{:keys [ratom]} (rel/q db [[:from :Watch] [:where [= :q [:_ q]]]])]
            (reset! ratom (rel/q db q)))
          (doseq [{:keys [f]} (rel/q db [[:from :Effect] [:where [= :q [:_ q]]]])]
            (when-some [tx (f db added deleted)]
              (vswap! feedback conj tx)))))
      nil
      changes)
    (not-empty @feedback)))

(defn t!
  "Queues a transaction, fires any associated signals if their dependencies change."
  [& tx]
  (loop [tx tx]
    (let [{:keys [db changes]} (apply rel/track-transact @state tx)]
      (reset! state db)
      (let [feedback (fire-signals! db changes)]
        (when (seq feedback)
          (recur feedback)))))
  nil)

(defn listen! [k q f]
  (swap! state rel/watch q)
  (t! [:insert-or-replace :Effect {:q q, :key k, :f f}]))

(defn q!
  "Returns a reagent derefable for the results of query `q`. Optionally supply a function to apply to the results.

  e.g (q! :Customer first) "
  ([q] (q! q identity))
  ([q f]
   (let [new-atom (r/atom nil)
         _ (t! [:insert-ignore :Watch {:q q, :ratom new-atom}])
         db (swap! state rel/watch q)
         {:keys [ratom]} (rel/row db :Watch [= :q [:_ q]])
         _ (if (identical? ratom new-atom)
             (let [new (rel/q db q)]
               (reset! ratom new)))]
     (r/track (fn [] (f @ratom))))))

;; ---
;; event handlers

(listen! ::on-project-completed-issue-reward
  (conj Project [:where :finished])
  (fn [_ added]
    (when-some [{:keys [reward]} (first added)]
      (list
        [:replace-all :Project (rand-nth projects)]
        [:update :Player {:money [+ :money reward]
                          :wealth [+ :wealth reward]
                          :completed [inc :completed]}]))))

;; ---
;; transactions

(defn progress
  ([] (progress 1.0))
  ([n] (progress n 0.0))
  ([parens income]
   (list [:update :Project {:progress [+ :progress parens]}]
         (when (and income (pos? income))
           [:update :Player {:money [+ :money income]
                             :wealth [+ :wealth income]}]))))

(defn buy
  [improvement-name]
  (fn [db]
    (let [{:keys [affordable
                  name
                  cost]}
          (rel/row db Improvement [= :name improvement-name])]
      (when affordable
        (list
          [:insert-or-update :Improvement {:qty [inc :qty]}

           {:name name
            :qty 1}]

          [:update :Player {:money [- :money cost]}])))))

(defn unlock [upgrade-name]
  (fn [db]
    (let [{:keys [affordable cost]} (rel/row db Upgrade [= :name upgrade-name])]
      (when affordable
        (list [:insert-ignore :Upgrade {:name upgrade-name}]
              [:update :Player {:money [- :money cost]}])))))

;; ----
;; reagent code

(defn project []
  (let [p (q! Project first)]
    (fn []
      [:table.table
       [:tbody
        [:tr [:th "Project"] [:td (:name @p)]]
        [:tr [:th "Progress"] [:td (long (:progress @p 0)) "/" (:difficulty @p)]]
        [:tr [:th "Reward"] [:td "$" (:reward @p)]]]])))


(defn paren []
  (let [clicks (q! Stats first)]
    (fn []
      [:button.button {:style {:width "96px" :height "96px"} :on-click #(t! (progress (+ 1 (:click-boost @clicks))))}
       "()"])))

(defn money []
  (let [player (q! :Player first)]
    (fn []
      [:span "$" (.toFixed (:money @player 0) 2)])))

(defn pps []
  (let [ti (q! Stats first)]
    (fn []
      [:span (.toFixed (:pps @ti 0) 2) " parens per second"])))

(defn status []
  [:table.table.is-fullwidth
   [:thead
    [:tr
     [:th [paren]]
     [:th [money]]
     [:th [pps]]]]])

(defn improvement-list []
  []
  (let [im (q! ShownImprovement)]
    (fn []
      [:table.table
       [:tbody
        (for [im @im]
          ^{:key (:name im)}
          [:tr
           [:th (:name im)]
           [:th (:qty im)]
           [:td (:desc im)]
           [:td "$" (.toFixed (:cost im 0) 2)]
           [:td (if (:affordable im)
                  [:button.button {:on-click #(t! (buy (:name im)))} "Buy"]
                  [:button.button {:disabled true} "Buy"])]])]])))

(defn upgrade-list []
  (let [up (q! ShownUpgrade)]
    (fn []
      [:table.table
       [:tbody
        (for [up @up]
          ^{:key (:name up)}
          [:tr
           [:th (:name up)]
           [:td (:desc up)]
           [:td "$" (:cost up)]
           [:td (if (:affordable up)
                  [:button.button {:on-click #(t! (unlock (:name up)))} "Unlock"]
                  [:button.button {:disabled true} "Unlock"])]])]])))

(defn app []
  (fn []
    [:section.section
     [:div.container
      [:div.columns
       [:div.column
        [status]]
       [:div.column
        [project]]]
      [:div.columns
       [:div.column
        [improvement-list]]
       [:div.column
        [upgrade-list]]]]]))

(defn tick! []
  (let [db @state
        {:keys [pps income]} (rel/row db Stats)]
    (when pps
      (t! (progress pps income))
      (let [db @state]
        (set! (.-title js/document) (str "CLJ IDLE $" (.toFixed (:money (rel/row db :Player) 0) 2)))))))

(defn init []

  (when-not (.-cljidleTicking js/window)
    (.setInterval js/window (fn [] (tick!)) 1000)
    (set! (.-cljidleTicking js/window) true))

  (rdom/render
    [app]
    (js/document.getElementById "app")))

(init)

(comment

  (t! (progress 0 100))

  (init)

  ,)