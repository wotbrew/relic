(ns examples.relira
  (:require
    [com.wotbrew.relic :as rel]
    [reagent.core :as r]
    [reagent.dom :as rdom]
    ["@mui/material" :as mui]
    ["@mui/icons-material/Menu" :default iconMenu]))

(def box (r/adapt-react-class mui/Box))
(def app-bar (r/adapt-react-class mui/AppBar))
(def toolbar (r/adapt-react-class mui/Toolbar))
(def icon-button (r/adapt-react-class mui/IconButton))
(def typography (r/adapt-react-class mui/Typography))
(def menu-icon (r/adapt-react-class iconMenu))
(def button (r/adapt-react-class mui/Button))
(def modal (r/adapt-react-class mui/Modal))
(def dialog (r/adapt-react-class mui/Dialog))
(def dialog-title (r/adapt-react-class mui/DialogTitle))
(def dialog-content (r/adapt-react-class mui/DialogContent))
(def dialog-content-text (r/adapt-react-class mui/DialogContentText))
(def text-field (r/adapt-react-class mui/TextField))
(def dialog-actions (r/adapt-react-class mui/DialogActions))
(def alert (r/adapt-react-class mui/Alert))
(def autocomplete (r/adapt-react-class mui/Autocomplete))
(def select (r/adapt-react-class mui/Select))
(def menu-item (r/adapt-react-class mui/MenuItem))

(def initial-data
  {:App [{}]
   :CreatingProject [{:project-id ""}]
   :CreatingIssue [{:title ""}]})

(def initial-state
  {:db (rel/transact {} initial-data)
   :watches {}})

(def state
  (atom initial-state))

(comment
  (reset! state initial-state))

(defn watch
  ([relvar] (watch relvar identity {}))
  ([relvar f] (if (map? f) (watch relvar identity f) (watch relvar f {})))
  ([relvar f opts]
   (let [{:keys [watches db]} @state]
     (or (watches [relvar f opts])
         (let [db (rel/watch db relvar)
               _ (swap! state assoc :db db)
               ratom (r/atom (f (rel/q db relvar opts)))
               _ (add-watch
                   state
                   ratom
                   (fn [_ _ _ {:keys [db, changes]}]
                     (when (contains? changes relvar)
                       (reset! ratom (f (rel/q db relvar opts))))))
               _ (swap! state assoc-in [:watches [relvar f opts]] ratom)]
           ratom)))))

(defn t! [& tx]
  (let [curr @state
        ret (apply rel/track-transact (:db curr) tx)]
    (swap! state merge ret)
    nil))

(defn tf [& tx]
  (fn [] (apply t! tx)))

(defn q [relvar] (rel/q (:db @state) relvar))

(def CreatingProject
  [[:from :CreatingProject]
   [:extend
    [:project-exists [some? [rel/sel1 :Project {:project-id :project-id}]]]
    [:error [:if :project-exists "Project with id already exists"]]
    [:disable-submit [boolean [:or :error [empty? :project-id]]]]]])

(defn create-project []
  (let [creating-project (watch CreatingProject first)]
    (fn []
      [dialog
       {:open true
        :onClose (tf [:update :App {:open-modal nil}])}
       [dialog-title "Create project"]
       [dialog-content
        [text-field
         {:autoFocus true
          :margin "dense"
          :id "project-id"
          :label "Project Id"
          :type "text"
          :fullWidth true
          :variant "standard"
          :defaultValue (:project-id @creating-project)
          :onChange (fn [e] (t! [:update :CreatingProject {:project-id (-> e .-target .-value)}]))}]

        (when-some [err (:error @creating-project)]
          [alert {:severity "error"} err])]
       [dialog-actions
        [button {:disabled (:disable-submit @creating-project)
                 :onClick (tf [:insert :Project {:project-id (:project-id @creating-project)}]
                              [:update :CreatingProject (constantly {})]
                              [:update :App {:open-modal nil}])} "Create"]]])))
(def DefaultSelectedProject
  [[:from :Project]
   [:agg [] [:project-id [min :project-id]]]])

(def CreatingIssue
  [[:from :CreatingIssue]
   [:extend
    [:project-id [:project-id [:or [rel/sel1 :CreatingIssueSelectedProject {}] [rel/sel1 DefaultSelectedProject {}]]]]
    [:error [:if [nil? :project-id] "Must select a project id"]]
    [:disable-submit [boolean [:or :error [empty? :title]]]]]])

(def ProjectId
  [[:from :Project]
   [:select :project-id]
   [:sort :project-id]])

(defn create-issue []
  (let [projects (watch ProjectId)
        creating-issue (watch CreatingIssue first)]
    (fn []
      [dialog
       {:open true
        :onClose (tf [:update :App {:open-modal nil}])}
       [dialog-title "Create issue"]
       [dialog-content

        (when (seq @projects)
          [select
           {:label "Project"
            :value (or (:project-id @creating-issue) (:project-id (first @projects)))
            :onChange (fn [e] (t! [:replace-all :CreatingIssueSelectedProject {:project-id (-> e .-target .-value)}]))}
           (for [{:keys [project-id]} @projects]
             ^{:key project-id} [menu-item {:value project-id} project-id])])

        [text-field
         {:autoFocus true
          :margin "dense"
          :label "Title"
          :type "text"
          :fullWidth true
          :variant "standard"
          :onChange (fn [e] (t! [:update :CreatingIssue {:title (-> e .-target .-value)}]))}]

        (when-some [err (:error @creating-issue)]
          [alert {:severity "error"} err])]

       [dialog-actions
        [button {:disabled (:disable-submit @creating-issue)
                 :onClick (tf [:insert :Issue {:project-id (:project-id @creating-issue)
                                               :title (:title @creating-issue)}]
                              [:update :CreatingIssue {:title ""}]
                              [:update :App {:open-modal nil}])}

         "Create"]
        [button {:disabled (:disable-submit @creating-issue)
                 :onClick (tf [:insert :Issue {:project-id (:project-id @creating-issue)
                                               :title (:title @creating-issue)}]
                              [:update :CreatingIssue {:title ""}])}
         "Create Another"]]])))

(def ProjectStats
  [[:from :Project]
   [:agg [] [:project-count count]]])

(def App
  [[:from :App]
   [:left-join ProjectStats {}]
   [:select
    :open-modal
    [:can-create-issue [:? pos? :project-count]]
    [:disable-create-issue [not :can-create-issue]]]])

(defn nav []
  (let [projects (watch ProjectId)
        app (watch App first)]
    (fn []
      [box {:sx #js {:flexGrow 1}}
       [app-bar {:position "static"}
        [toolbar [icon-button {:size "large"
                               :edge "start"
                               :color "inherit"
                               :aria-label "menu"
                               :sx #js {:mr 2}}
                  [menu-icon]]
         [typography {:variant "h6" :component "div"}
          "RelIRA"]
         (for [project @projects]
           ^{:key (:project-id project)}
           [button {:color "inherit", :onClick (tf [:update :App {:open-project (:project-id project)}])}
            (:project-id project)])
         [box {:sx #js {:flexGrow 1}}]
         [button {:color "inherit", :onClick (tf [:update :App {:open-modal [:_ :create-project]}])} "Create Project"]
         [button {:disabled (:disable-create-issue @app) :color "inherit", :onClick (tf [:update :App {:open-modal [:_ :create-issue]}])} "Create Issue"]]]])))

(defn app []
  (let [app-model (watch App first)]
    (fn []
      (let [open-modal (:open-modal @app-model)]
        [:div
         [nav]
         (when (= :create-project open-modal)
           [create-project])
         (when (= :create-issue open-modal)
           [create-issue])]))))

(rdom/render [app] (js/document.getElementById "app"))