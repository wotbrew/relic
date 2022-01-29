(ns examples.realworld.frontend
  (:require [reagent.dom :as rdom]
            [reagent.core :as r]
            [com.wotbrew.relic :as rel]
            [clojure.string :as str]))

;; poor mans reagent integration

(defn empty-db []
  (-> {}
      (rel/mat [[:from :Watch]
                [:unique :q]])
      (rel/transact {:Global [{:page :home}]})))

(def state
  (atom {:db (empty-db)}))

(defn t [{:keys [db]} & tx] (apply rel/track-transact db tx))

(defn t! [& tx]
  (swap! state #(apply t % tx))
  nil)

(defn- fire-signals! [{:keys [db changes]}]
  (reduce-kv
    (fn [_ q {:keys [added deleted]}]
      (when (or (seq added)
                (seq deleted))
        (doseq [{:keys [ratom]} (rel/q db [[:from :Watch] [:where [= :q [:_ q]]]])]
          (reset! ratom (rel/q db q)))))
    nil
    changes))

(add-watch state ::fire-signals (fn [_ _ _ n] (fire-signals! n)))

(defn q!
  ([q] (q! q identity))
  ([q f]
   (let [new-atom (r/atom nil)
         _ (t! [:insert-ignore :Watch {:q q, :ratom new-atom}])
         {:keys [db]} (swap! state (fn [{:keys [db]}] {:db (rel/watch db q)}))
         {:keys [ratom]} (rel/row db :Watch [= :q [:_ q]])
         _ (if (identical? ratom new-atom)
             (let [new (rel/q db q)]
               (reset! ratom new)))]
     (r/track (fn [] (f @ratom))))))

;; helpers, use routing lib in real life please

(defn route-tx [page]
  (list
    [:delete :Error]
    [:update :Global {:page [:_ page]}]))

(defn route [page]
  (t! (route-tx page))
  false)

(defn api-url [& path]
  ;; encoding bah
  (str "http://localhost:6003/api/" (str/join "/" path)))

(defn api [{:keys [path
                   method
                   params
                   err-origin
                   err-map
                   on-success]
            :or {on-success (constantly nil)
                 err-map {}
                 err-origin :unknown
                 method "GET"}}]
  (let [err (fn [t] {:Error [{:type t, :origin err-origin}]})
        err-map (merge {409 :conflict
                        404 :not-found
                        422 :invalid
                        403 :forbidden
                        401 :unauthorized} err-map)
        token (:token (rel/row (:db @state) :User))]
    (t! [:delete :Error])
    (-> (js/fetch
          (apply api-url path)
          (clj->js
            (cond->
              {:method method
               :headers (cond-> {"Content-Type" "application/json"}
                                token (assoc "Authorization" (str "Token " token)))
               :mode "cors"}
              (not= "GET" method) (assoc :body (js/JSON.stringify (clj->js params))))))
        (.catch (fn [] (t! (err :unknown))))
        (.then (fn [response]
                 (if (.-ok response)
                   (-> (.json response)
                       (.catch (fn [] (t! (err :bad-json))))
                       (.then #(on-success (js->clj % :keywordize-keys true))))
                   (t! (err (err-map (.-status response) :unknown)))))))))

(defn load-tags! []
  (t! [:delete :Tag]
      [:delete :Error [:where [= :origin [:_ :load-tags]]]])
  (api {:path ["tags"]
        :method "GET"
        :err-origin :load-tags
        :on-success #(t! (into [:replace-all :Tag] (for [t (:tags %)] {:tag t})))}))

(defn load-global-feed! []
  (load-tags!)
  (t! [:delete :Article]
      [:delete :Error [:where [= :origin [:_ :load-global-feed]]]])
  (api {:path ["articles"]
        :method "GET"
        :params {}
        :err-origin :load-global-feed
        :on-success #(t! (into [:replace-all :Article] (:articles %)))}))

(defn load-personal-feed! []
  (load-tags!)
  (t! [:delete :Article]
      [:delete :Error [:where [= :origin [:_ :load-personal-feed]]]])
  (api {:path ["articles" "feed"]
        :method "GET"
        :params {}
        :err-origin :load-personal-feed
        :on-success #(t! (into [:replace-all :Article] (:articles %)))}))

(defn load-profile-feed! [username & {:keys [favorited]}]
  (t! [:delete :Article]
      [:delete :Error [:where [= :origin [:_ :load-profile-feed]]]])
  ;; yea i know, no url encoding
  (api {:path [(str "articles?author=" username (when favorited "&favorited=1"))]
        :method "GET"
        :err-origin :load-profile-feed
        :on-success #(t! (into [:replace-all :Article] (:articles %)))}))

(defn load-profile! [username]
  (t! [:delete :Profile]
      [:delete :Error [:where [= :origin [:_ :load-profile]]]])
  (api {:path ["profiles" username]
        :method "GET"
        :err-origin :load-profile
        :on-success #(t! [:replace-all :Profile %])})
  (load-profile-feed! username))

(defn reload-feed! []
  (if (:feed (rel/row (:db @state) :Global))
    (load-personal-feed!)
    (load-global-feed!)))

(defn submit-via [event f]
  (.preventDefault event)
  (let [fd (js/FormData. (.-target event))
        data (js->clj (js/Object.fromEntries (.entries fd)))]
    (f data)
    false))

;; ui

(defn sign-in []
  (let [errors (q! [[:from :Error] [:where [= :origin [:_ :sign-in]]]])
        sign-in!
        (fn [params]
          (api
            {:path ["users" "login"]
             :method "POST"
             :params params
             :on-success #(t! [:replace-all :User %]
                              (route-tx :home))
             :err-origin :sign-in}))]
    (fn []
      [:div.auth-page
       [:div.container.page
        [:div.row
         [:div.col-md-6.offset-md-3.col-xs-12
          [:h1.text-xs-center "Sign in"]
          [:p.text-xs-center
           [:a {:href "#" :on-click #(route :sign-up)} "Don't have an account?"]]

          (when @errors
            [:ul.error-messages
             (for [{:keys [type]} @errors]
               (case type
                 :not-found [:li "Could not find account, check your password and try again."]
                 [:li "Unknown"]))])

          [:form {:on-submit #(submit-via % sign-in!)}
           [:fieldset.form-group
            [:input.form-control.form-control-lg {:type "text" :name "email" :placeholder "Email"}]]
           [:fieldset.form-group
            [:input.form-control.form-control-lg {:type "password" :name "password" :placeholder "Password"}]]
           [:button.btn.btn-lg.btn-primary.pull-xs-right "Sign in"]]]]]])))


(defn sign-up []
  (let [errors (q! [[:from :Error] [:where [= :origin [:_ :sign-up]]]])
        sign-up! (fn [params]
                   (api {:path ["users"]
                         :method "POST"
                         :params params
                         :on-success #(t! [:replace-all :User %]
                                          (route-tx :home))
                         :err-origin :sign-up}))]
    (fn []
      [:div.auth-page
       [:div.container.page
        [:div.row
         [:div.col-md-6.offset-md-3.col-xs-12
          [:h1.text-xs-center "Sign up"]
          [:p.text-xs-center
           [:a {:href "#" :on-click #(route :sign-in)} "Have an account?"]]

          (when @errors
            [:ul.error-messages
             (for [{:keys [type]} @errors]
               (case type
                 :conflict [:li "That email (or username) is already taken"]
                 :invalid [:li "Validation error"]
                 [:li "Unknown"]))])

          [:form
           {:on-submit (fn [e] (submit-via e sign-up!))}
           [:fieldset.form-group
            [:input.form-control.form-control-lg {:name "username" :type "text" :placeholder "Your Name"}]]
           [:fieldset.form-group
            [:input.form-control.form-control-lg {:name "email" :type "text" :placeholder "Email"}]]
           [:fieldset.form-group
            [:input.form-control.form-control-lg {:name "password" :type "password" :placeholder "Password"}]]
           [:button.btn.btn-lg.btn-primary.pull-xs-right
            "Sign up"]]]]]])))

(defn goto-profile [username]
  (load-profile! username)
  (route :profile))

(defn article-preview [{:keys [title
                               description
                               slug
                               author
                               favoritesCount
                               createdAt
                               tagList]}]
  [:div.article-preview
   [:div.article-meta
    [:a {:href "#", :on-click #(goto-profile (:username author))} [:img {:src (:image author)}]]
    [:div.info
     [:a.author {:href "#" :on-click #(goto-profile (:username author))} (:username author)]
     [:span.date createdAt]]
    [:button.btn.btn-outline-primary.btn-sm.pull-xs-right
     [:i.ion-heart] favoritesCount]]
   [:a.preview-link {:href "#"}
    [:h1 title]
    [:p description]
    [:span "Read more..."]
    [:ul.tag-list
     (for [tag tagList]
       ^{:key (str slug "-tag-" tag)} [:li.tag-default.tag-pill.tag-outline tag])]]])

(defn profile []
  (let [profile (q! :Profile first)
        articles (q! [[:from :Article]
                      [:sort [:createdAt :desc]]])
        global (q! :Global first)]
    (fn []
      [:div.profile-page
       [:div.user-info
        [:div.container
         [:div.row
          [:div.col-xs-12.col-md-10.offset-md-1
           [:img.user-img {:src (:image @profile)}]
           [:h4 (:username @profile)]
           [:p (:bio @profile)]
           [:button.btn.btn-sm.btn-outline-secondary.action-btn
            [:i.ion-plus-round] "Follow " (:username @profile)]]]]]
       [:div.container
        [:div.row
         [:div.col-xs-12.col-md-10.offset-md-1
          [:div.articles-toggle
           [:ul.nav.nav-pills.outline-active
            [:li.nav-item
             [:a.nav-link {:href "#",
                           :on-click #(do (t! [:update :Global {:favorited false}])
                                          (load-profile-feed! (:username @profile) :favorited false))
                           :class (if (:favorited @global) "" "active")} "My Articles"]]
            [:li.nav-item
             [:a.nav-link {:href "#"
                           :on-click #(do (t! [:update :Global {:favorited true}])
                                          (load-profile-feed! (:username @profile) :favorited true))
                           :class (if (:favorited @global) "active" "")} "Favorited Articles"]]]]
          (for [article @articles] ^{:key (:slug article)} [article-preview article])]]]])))

(defn settings []
  (let [save-settings! (fn [params]
                         (api {:path ["user"]
                               :method "PUT"
                               :params params
                               :on-success #(t! [:replace-all :User %]
                                                (reload-feed!)
                                                (route-tx :home))
                               :err-origin :settings}))
        errors (q! [[:from :Error] [:where [= :origin [:_ :settings]]]])
        user (q! :User first)]
    [:div.settings-page
     [:div.container.page
      [:div.row
       [:div.col-md-6.offset-md-3.col-xs-12
        [:h1.text-xs-center "Your Settings"]

        (when @errors
          [:ul.error-messages
           (for [{:keys [type]} @errors]
             (case type
               :conflict [:li "That email (or username) is already taken"]
               :invalid [:li "Validation error"]
               [:li "Unknown"]))])

        [:form
         {:on-submit #(submit-via % save-settings!)}
         [:fieldset
          [:fieldset.form-group
           [:input.form-control {:name "image" :type "text" :placeholder "URL of profile picture", :defaultValue (:image @user)}]]
          [:fieldset.form-group
           [:input.form-control.form-control-lg {:name "username" :type "text" :placeholder "Your Name", :defaultValue (:username @user)}]]
          [:fieldset.form-group
           [:textarea.form-control.form-control-lg {:name "bio" :rows "8" :placeholder "Short bio about you" :defaultValue (:bio @user)}]]
          [:fieldset.form-group
           [:input.form-control.form-control-lg {:name "email" :type "text" :placeholder "Email" :defaultValue (:email @user)}]]
          [:fieldset.form-group
           [:input.form-control.form-control-lg {:name "password" :type "password" :placeholder "Password" :defaultValue (:password @user)}]]
          [:button.btn.btn-lg.btn-primary.pull-xs-right "Update Settings"]]]]]]]))

(defn article []
  [:div.article-page
   [:div.banner
    [:div.container
     [:h1 "How to build webapps that scale"]
     [:div.article-meta
      [:a {:href ""} [:img {:src "http://i.imgur.com/Qr71crq.jpg"}]]
      [:div.info
       [:a.author {:href ""} "Eric Simons"]
       [:span.date "January 20th"]]
      [:button.btn.btn-sm.btn-outline-secondary
       [:i.ion-plus-round] "&nbsp;
                    Follow Eric Simons" [:span.counter "(10)"]] "&nbsp;&nbsp;"
      [:button.btn.btn-sm.btn-outline-primary
       [:i.ion-heart] "&nbsp;
                    Favorite Post" [:span.counter "(29)"]]]]]
   [:div.container.page
    [:div.row.article-content
     [:div.col-md-12
      [:p "Web development technologies have evolved at an incredible clip over the past few years."]
      [:h2#introducing-ionic "Introducing RealWorld."]
      [:p "It's a great solution for learning how other frameworks work."]]]
    [:hr]
    [:div.article-actions
     [:div.article-meta
      [:a {:href "profile.html"} [:img {:src "http://i.imgur.com/Qr71crq.jpg"}]]
      [:div.info
       [:a.author {:href ""} "Eric Simons"]
       [:span.date "January 20th"]]
      [:button.btn.btn-sm.btn-outline-secondary
       [:i.ion-plus-round] "&nbsp;
                    Follow Eric Simons"] "&nbsp;"
      [:button.btn.btn-sm.btn-outline-primary
       [:i.ion-heart] "&nbsp;
                    Favorite Post" [:span.counter "(29)"]]]]
    [:div.row
     [:div.col-xs-12.col-md-8.offset-md-2
      [:form.card.comment-form
       [:div.card-block
        [:textarea.form-control {:placeholder "Write a comment..." :rows "3"}]]
       [:div.card-footer
        [:img.comment-author-img {:src "http://i.imgur.com/Qr71crq.jpg"}]
        [:button.btn.btn-sm.btn-primary "Post Comment"]]]
      [:div.card
       [:div.card-block
        [:p.card-text "With supporting text below as a natural lead-in to additional content."]]
       [:div.card-footer
        [:a.comment-author {:href ""}
         [:img.comment-author-img {:src "http://i.imgur.com/Qr71crq.jpg"}]] "&nbsp;"
        [:a.comment-author {:href ""} "Jacob Schmidt"]
        [:span.date-posted "Dec 29th"]]]
      [:div.card
       [:div.card-block
        [:p.card-text "With supporting text below as a natural lead-in to additional content."]]
       [:div.card-footer
        [:a.comment-author {:href ""}
         [:img.comment-author-img {:src "http://i.imgur.com/Qr71crq.jpg"}]] "&nbsp;"
        [:a.comment-author {:href ""} "Jacob Schmidt"]
        [:span.date-posted "Dec 29th"]
        [:span.mod-options
         [:i.ion-edit]
         [:i.ion-trash-a]]]]]]]])

(defn new-article []
  (let [new-article! (fn [params]
                       (api {:path ["articles"]
                             :method "POST"
                             :params params
                             :on-success #(do
                                            (route-tx :home)
                                            (reload-feed!))
                             :err-origin :new-article}))
        errors (q! [[:from :Error] [:where [= :origin [:_ :new-article]]]])]
    (fn []
      [:div.editor-page
       [:div.container.page

        (when @errors
          [:ul.error-messages
           (for [{:keys [type]} @errors]
             (case type
               :conflict [:li "That title is already taken"]
               :invalid [:li "Validation error"]
               [:li "Unknown"]))])

        [:div.row
         [:div.col-md-10.offset-md-1.col-xs-12
          [:form
           {:on-submit #(submit-via % new-article!)}
           [:fieldset
            [:fieldset.form-group
             [:input.form-control.form-control-lg {:name "title" :type "text" :placeholder "Article Title"}]]
            [:fieldset.form-group
             [:input.form-control {:name "description" :type "text" :placeholder "What's this article about?"}]]
            [:fieldset.form-group
             [:textarea.form-control {:name "body" :rows "8" :placeholder "Write your article (in markdown)"}]]
            [:fieldset.form-group
             [:input.form-control {:name "tagList" :type "text" :placeholder "Enter tags"}]
             [:div.tag-list]]
            [:button.btn.btn-lg.pull-xs-right.btn-primary "Publish Article"]]]]]]])))

(defn nav []
  (let [user (q! :User first)]
    (fn []
      [:nav.navbar.navbar-light
       [:div.container
        [:a.navbar-brand {:href "#" :on-click #(route :home)} "conduit"]
        [:ul.nav.navbar-nav.pull-xs-right
         [:li.nav-item
          [:a.nav-link.active {:href "#" :on-click #(route :home)} "Home"]]
         (when @user
           [:li.nav-item
            [:a.nav-link {:href "#" :on-click #(route :new-article)}
             [:i.ion-compose] "New Article"]])
         (when @user
           [:li.nav-item
            [:a.nav-link {:href "#" :on-click #(route :settings)}
             [:i.ion-gear-a] "Settings"]])
         (when-not @user
           [:li.nav-item
            [:a.nav-link {:href "#" :on-click #(route :sign-in)} "Sign in"]])
         (when-not @user
           [:li.nav-item
            [:a.nav-link {:href "#" :on-click #(route :sign-up)} "Sign up"]])
         (when @user
           [:li.nav-item
            [:a.nav-link {:href "#" :on-click #(do (t! [:delete :User]) (route :home))} "Log out (" (:username @user) ")"]])]]])))

(defn footer []
  [:footer
   [:div.container
    [:a.logo-font {:href "/"} "conduit"]
    [:span.attribution "An interactive learning project from" [:a {:href "https://thinkster.io"} "Thinkster"] ". Code & design licensed under MIT."]]])

(defn home []
  (let [global (q! :Global first)
        articles (q! [[:from :Article]
                      [:sort [:createdAt :desc]]])
        tags (q! [[:from :Tag]
                  [:sort [:tag :asc]]])]
    (fn []

      [:div.home-page
       [:div.banner
        [:div.container
         [:h1.logo-font "conduit"]
         [:p "A place to share your knowledge."]]]
       [:div.container.page
        [:div.row
         [:div.col-md-9
          [:div.feed-toggle
           [:ul.nav.nav-pills.outline-active
            [:li.nav-item
             [:a.nav-link {:href "#"
                           :on-click #(do (load-personal-feed!) (t! [:update :Global {:feed true}]))
                           :class (if (:feed @global) "active" "")} "Your Feed"]]
            [:li.nav-item
             [:a.nav-link {:href "#"
                           :on-click #(do (load-global-feed!) (t! [:update :Global {:feed false}]))
                           :class (if (:feed @global) "" "active")} "Global Feed"]]]]

          (for [article @articles] ^{:key (:slug article)} [article-preview article])]

         [:div.col-md-3
          [:div.sidebar
           [:p "Popular Tags"]
           [:div.tag-list
            (for [{:keys [tag]} @tags]
              ^{:key tag} [:a.tag-pill.tag-default {:href "#"} tag])]]]]]])))

(defn app []
  (let [global (q! :Global first)]
    (fn []
      [:div
       [nav]
       (case (:page @global)
         :home [home (:feed @global)]
         :new-article [new-article]
         :settings [settings]
         :sign-in [sign-in]
         :sign-up [sign-up]
         :profile [profile]
         [home (:feed @global)])
       [footer]])))

(load-global-feed!)

(rdom/render
  [app]
  (js/document.getElementById "app"))