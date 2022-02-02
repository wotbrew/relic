(ns examples.todo-spa
  (:require [reagent.core :as r]
            [com.wotbrew.relic :as rel]
            [clojure.string :as str]
            [reagent.dom :as rdom]))

(def state
  (atom {:result {}}))

(defn- watch
  ([relvar] (watch relvar identity))
  ([relvar f & args]
   (let [db (:result (swap! state (fn [{:keys [result]}] {:result (rel/watch result relvar)})))
         ratom (r/atom (rel/q db relvar))
         _ (add-watch state ratom (fn [_ _ _ {:keys [result, changes]}]
                                    (when (contains? changes relvar)
                                      (reset! ratom (apply f (rel/q result relvar) args)))))]
     ratom)))

(defn tx! [& tx]
  (swap! state (fn [{:keys [result]}] (apply rel/track-transact result tx)))
  nil)

(def TodoInput [[:table :TodoInput {:req [:text], :unique [[1]]}]])
(def Todo [[:table :Todo {:req [:id, :text, :done]}]])

(defn genid [] (str (random-uuid)))

(defn add-todo [s] [:insert :Todo {:id (genid), :text s, :done false, :added-at (.now (.-performance js/window))}])
(defn toggle [id] [:update :Todo {:done [not :done]} [= :id id]])
(defn save [id title] [:update :Todo {:title title} [= :id id]])
(defn delete [id] [:delete :Todo [= :id id]])

(defn complete-all [] [:update :Todo {:done true}])
(defn clear-done [] [:delete :Todo :done])

(def TodoInputViewModel
  [[:from TodoInput]
   [:extend
    [:savable [not [str/blank? :text]]]]])

(def todo-input-view-model
  (watch TodoInputViewModel first))

(defn todo-input []
  [:input {:type "text",
           :value (:text @todo-input-view-model)
           :id "todo-input"
           :class "input"
           :on-blur #(tx! [:upsert :TodoInput {:text ""}] (when (:savable @todo-input-view-model) (add-todo (:text @todo-input-view-model))))
           :on-change #(tx! [:upsert :TodoInput {:text (-> % .-target .-value)}])
           :on-key-down #(case (.-which %)
                           13 (tx! [:upsert :TodoInput {:text ""}] (when (:savable @todo-input-view-model) (add-todo (:text @todo-input-view-model))))
                           27 (tx! [:upsert :TodoInput {:text ""}])
                           nil)}])

(def Filter
  [[:table :Filter {:req [:filter], :unique [[1]]}]])

(defn- show-todo? [{:keys [filter, done]}]
  (case filter
    "all" true
    "done" done
    "active" (not done)
    false))

(def TodoListViewModel
  [[:from Todo]
   [:join Filter {}]
   [:where show-todo?]
   [:btree :added-at :id]])

(def todo-list-view-model (watch TodoListViewModel identity))

(defn todo-item [{:keys [id done text editing]}]
  [:li {:class (str (if done "completed ")
                    (if editing "editing"))}
   [:div.view
    [:input.toggle {:type "checkbox", :checked done, :on-change #(tx! (toggle id))}]
    [:label {:on-click #(tx! [:update :Todo {:editing true} [= :id id]])} text]
    [:button.destroy {:on-click #(tx! (delete id))} "(delete)"]
    (when editing
      [:input {:type "text",
               :value text
               :id "todo-input"
               :class "input"
               :on-blur #(tx! [:update :Todo {:editing false} [= :id id]])
               :on-change #(tx! [:update :Todo {:text (-> % .-target .-value)} [= :id id]])
               :on-key-down #(case (.-which %)
                               13 (tx! [:update :Todo {:text (-> % .-target .-value)} [= :id id]])
                               27 (tx! [:update :Todo {:text (-> % .-target .-value)} [= :id id]])
                               nil)}])]])

(defn todo-list []
  [:ul#todo-list
   (for [todo @todo-list-view-model]
     ^{:key (:id todo)} [todo-item todo])])

(def TodoSummary
  [[:from Todo]
   [:agg []
    [:all count]
    [:done [count :done]]]])

(def TodoStatsViewModel
  [[:from TodoSummary]
   [:join Filter {}]
   [:select
    :all
    :filter
    [:active [- :all :done]]]])

(def todo-stats-view-model
  (watch TodoStatsViewModel))

(defn todo-stats []
  [:div
   [:span#todo-count
    [:strong (:active @todo-stats-view-model)]]])

(def AppViewModel
  [[:from TodoSummary]
   [:select
    [:has-todos [pos? :all]]
    [:all-complete [= :done :all]]]])

(def app-view-model
  (watch AppViewModel first))

(defn todo-app []
  [:div
   [:section#todoapp
    [:header#header
     [:h1 "todos"]
     [todo-input]
     (when (:has-todos @app-view-model)
       [:div
        [:section#main
         [:input#toggle-all
          {:type "checkbox"
           :checked (:all-complete @app-view-model)
           :on-change #(tx! (complete-all))}]
         [:label {:for "toggle-all"} "Mark all as complete"]
         [todo-list]]])
     [:footer#footer
      [todo-stats]]]]])

(defn ^:export run []

  (tx!
    {Filter [{:filter "all"}]
     TodoInput [{:text ""}]}
    (add-todo "Fix :left-join")
    (add-todo "Prepare the slides")
    (add-todo "Use indexes in :where")
    (complete-all))

  (rdom/render [todo-app] (js/document.getElementById "app")))