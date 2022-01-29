(ns examples.realworld.backend
  (:require [clojure.string :as str]
            [com.wotbrew.relic :as rel]
            [compojure.core :refer [GET POST PUT DELETE OPTIONS routes]]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.format :as format]
            [ring.middleware.cors :as cors]
            [ring.middleware.params :as params])
  (:import (java.time Instant)
           (org.eclipse.jetty.server Server)))

;; model

(defn- slugify [s]
  (-> s str/lower-case (str/replace #"\s+" "-")))

(def FavoriteCount
  [[:from :Favorite]
   [:agg [:favorite/article] [:article/favorite-count count]]])

(defn- tag-set [s]
  (set (str/split (str s) #"(,|\s+)")))

(def Article
  [[:from :Article]
   [:extend
    [:article/author [rel/sel1 :User {:article/user :user/id}]]
    [:article/slug [slugify :article/title]]
    [:article/tag-set [tag-set :article/tag-list]]
    [[:article/favorite-count] [rel/sel1 FavoriteCount {:article/id :favorite/article}]]]])

(def Tag
  [[:from Article]
   [:expand [:tag/tag :article/tag-set]]
   [:select :tag/tag]])

(def Comment
  [[:from :Comment]
   [:extend
    [:comment/author [rel/sel1 :User {:comment/user :user/id}]]]])

;; queries

(defn list-article-query [{:keys [uid tag author favorited]}]
  (let [filtered
        (cond->
          [[:from Article]]

          author
          (conj [:where [= [:user/username :article/author] author]])

          ;; this is where I would like to
          ;; demo an optimisation if we are running lots of articles
          ;; e.g create index on tag article author
          tag
          (conj [:where [contains? :article/tag-set tag]])

          favorited
          (conj [:where [rel/sel1 :Favorite {:article/id :favorite/article
                                             uid :favorite/user}]]))

        counted
        (conj filtered [:agg [] [:article-count count]])

        joined
        (conj filtered [:join counted {}])

        sorted
        (conj joined [:sort [[:article/ts :desc]]])]

    sorted))

(defn feed-article-query [{:keys [uid]}]
  (let [filtered [[:from :Follow]
                  [:where [= :follow/follower uid]]
                  [:join Article {:follow/followed :article/user}]]
        counted (conj filtered [:agg [] [:article-count count]])
        joined (conj filtered [:join counted {}])
        sorted (conj joined [:sort [[:article/ts :desc]]])]
    sorted))

(defn article-comments-query [{:keys [slug]}]
  [[:from Article]
   [:where [= :article/slug slug]]
   [:join Comment {:article/id :comment/article}]
   [:sort [:comment/created-at :asc]]])

;; transactions

(defn register-user [{:keys [uid username email password]}]
  {:User [{:user/id uid
           :user/username username
           :user/email email
           :user/password password}]})

(defn update-user [{:keys [uid email username password image bio]
                    :or {email :user/email
                         username :user/username
                         password :user/password
                         image :user/image
                         bio :user/bio}}]
  [:update :User
   {:user/email email
    :user/username username
    :user/password password
    :user/image image
    :user/bio bio}
   [= :user/id uid]])

(defn follow-user [{:keys [follower-uid followed-uid]}]
  [:insert-ignore :Follow {:follow/follower follower-uid :follow/followed followed-uid}])

(defn unfollow-user [{:keys [follower-uid followed-uid]}]
  [:delete-exact :Follow {:follow/follower follower-uid :follow/followed followed-uid}])

(defn create-article [{:keys [aid uid title description body tag-list created-at]}]
  {:Article [{:article/id aid
              :article/user uid
              :article/title title
              :article/description description
              :article/body body
              :article/tag-list tag-list
              :article/created-at created-at
              :article/updated-at created-at}]})

(defn- slug->article-id [db slug]
  (:article/id (rel/row db :Article [= :article/slug slug])))

(defn update-article [{:keys [slug title description body updated-at]
                       :or {title :article/title
                            description :article/description
                            body :article/body}}]
  (fn [db]
    [:update :Article {:article/title title
                       :article/description description
                       :article/body body
                       :article/updated-at updated-at}
     [= :article/id (slug->article-id db slug)]]))

(defn delete-article [{:keys [slug]}]
  (fn [db]
    [:delete :Article [= :article/id (slug->article-id db slug)]]))

(defn create-comment [{:keys [cid uid slug body created-at]}]
  (fn [db]
    (let [aid (slug->article-id db slug)]
      {:Comment {:comment/id cid
                 :comment/article aid
                 :comment/user uid
                 :comment/body body
                 :comment/created-at created-at
                 :comment/updated-at created-at}})))

(defn delete-comment [{:keys [cid]}]
  [:delete :Comment [= :comment/id cid]])

(defn favorite-article [{:keys [uid slug]}]
  (fn [db]
    [:insert-ignore :Favorite {:favorite/user uid
                               :favorite/article (slug->article-id db slug)}]))

(defn unfavorite-article [{:keys [uid slug]}]
  (fn [db]
    [:delete-exact :Favorite {:favorite/user uid
                              :favorite/article (slug->article-id db slug)}]))

;; jwt

(defn jwt [uid] (str uid))
(defn uid [jwt] (Long/parseLong jwt))

;; responses

(defn user-response [user]
  {:status 200
   :body {"email" (:user/email user)
          "token" (jwt (:user/id user))
          "username" (:user/username user)
          "bio" (:user/bio user)
          "image" (:user/image user)}})

(def FollowedIndex
  [[:from :Follow]
   [:unique :follow/follower :follow/followed]])

(defn- following? [db follower-uid followed-uid]
  (let [idx (rel/index db FollowedIndex)]
    (get-in idx [follower-uid followed-uid])))

(def FavoritedIndex
  [[:from :Favorite]
   [:unique :favorite/article :favorite/user]])

(defn- favorited? [db aid uid]
  (let [idx (rel/index db FavoritedIndex)]
    (get-in idx [uid aid])))

(defn profile-response [user authed-uid db]
  {:status 200
   :body
   {"username" (:user/username user)
    "bio" (:user/bio user)
    "image" (:user/image user)
    "following" (following? db authed-uid (:user/id user))}})

(defn article-response [article authed-uid db]
  {:status 200
   :body
   {"slug" (:article/slug article)
    "title" (:article/title article)
    "description" (:article/description article)
    "body" (:article/body article)
    "tagList" (vec (sort (:article/tag-set article)))
    "createdAt" (str (:article/created-at article))
    "updatedAt" (str (:article/updated-at article))
    "favorited" (favorited? db (:article/id article) authed-uid)
    "favoritesCount" (:article/favorite-count article)
    "author" (:body (profile-response (:article/author article) authed-uid db))}})

(defn article-list-response [articles authed-uid db]
  {:status 200
   :body {"articleCount" (:article-count (first articles) 0)
          "articles" (mapv #(:body (article-response % authed-uid db)) articles)}})

(defn comment-response [comment authed-uid db]
  {:status 200
   :body
   {"id" (:comment/id comment)
    "createdAt" (str (:comment/created-at comment))
    "updatedAt" (str (:comment/updated-at comment))
    "body" (:comment/body comment)
    "author" (:body (profile-response (:comment/author comment) authed-uid db))}})

(defn comment-list-response [comments authed-uid db]
  {:status 200
   :body {"comments" (mapv #(comment-response % authed-uid db) comments)}})

;; initial state

(def hints
  [[[:from :User]
    [:unique :user/email]]

   [[:from :User]
    [:unique :user/id]]

   [[:from :User]
    [:unique :user/username]]

   [[:from :Article]
    [:unique :article/id]]

   [[:from Article]
    [:unique :article/slug]]

   [[:from Article]
    [:unique :article/id]]

   [[:from Article]
    [:hash [:author/username :article/author]]]

   [[:from :Follow]
    [:unique :follow/follower :follow/followed]]

   [[:from :Follow]
    [:fk :User {:follow/follower :user/id} {:cascade :delete}]]

   [[:from :Follow]
    [:fk :User {:follow/followed :user/id} {:cascade :delete}]]

   FollowedIndex

   [[:from :Favorite]
    [:unique :favorite/user :favorite/article]]

   FavoritedIndex

   [[:from :Favorite]
    [:fk :User {:favorite/user :user/id} {:cascade :delete}]]

   [[:from :Comment]
    [:fk :Article {:comment/article :article/id} {:cascade :delete}]]

   [[:from Comment]
    [:unique :comment/id]]

   (list-article-query {})])

(defn- empty-db []
  (-> (apply rel/mat {} hints)
      (rel/transact
        {:User [{:user/username "wotbrew"
                 :user/bio "Clojure Dev"
                 :user/password "password"
                 :user/email "wotbrew@gmail.com"
                 :user/id -1
                 :user/image "https://gravatar.com/avatar/30fe87666734dfa826fd15c3c0741547.jpeg?s=160"}]

         :Article [{:article/id -1
                    :article/title "Relic, all out of tar"
                    :article/user -1
                    :article/created-at (Instant/ofEpochMilli 1643238440091)
                    :article/updated-at (Instant/ofEpochMilli 1643238440091)
                    :article/description "Functional relational programming with relic"
                    :article/body "Tis good..."
                    :article/tag-list "functional relational clojure db"}

                   {:article/id -2
                    :article/title "Enough with the thing already!"
                    :article/user -1
                    :article/created-at (Instant/ofEpochMilli 1643224310091)
                    :article/updated-at (Instant/ofEpochMilli 1643224310091)
                    :article/description "Enough is enough"
                    :article/body "rant"
                    :article/tag-list "rant"}]})))

;; state

(def state (atom (empty-db)))

(defonce id-ctr (atom 0))

;; api

(def forbidden {:status 403, :body {:msg "Forbidden"}})
(def unauthorized {:status 401, :body {:msg "Unauthorized"}})

(defn api-login [{:keys [params]}]
  (let [{:strs [email password]} params
        ;; lol no hash
        user (rel/row @state :User [= :user/email email] [= :user/password password])]
    (when user
      (user-response user))))

(defn api-register [{:keys [params]}]
  (let [{:strs [username email password]} params
        uid (swap! id-ctr inc)
        tx (register-user {:uid uid,
                           :username username
                           :email email
                           :password password})
        db (swap! state rel/transact tx)]
    (user-response (rel/row db :User [= :user/id uid]))))

(defn api-update-current-user [{:keys [authed-uid params]}]
  (let [{:strs [email username password image bio]} params]
    (if-not authed-uid
      unauthorized
      (let [tx (update-user {:uid authed-uid
                             :username username
                             :email email
                             :password password
                             :image image
                             :bio bio})
            db (swap! state rel/transact tx)]
        (user-response (rel/row db :User [= :user/id authed-uid]))))))

(defn- api-get-current-user [{:keys [authed-uid]}]
  (if-not authed-uid
    unauthorized
    (let [db @state]
      (when-some [usr (rel/row db :User [= :user/id authed-uid])]
        (user-response usr)))))

(defn api-get-profile [{:keys [authed-uid params] :as req}]
  (let [db @state
        {:keys [username]} params]
    (when-some [user (rel/row db :User [= :user/username username])]
      (profile-response user authed-uid db))))

(defn api-follow [{:keys [authed-uid params]}]
  (if-not authed-uid
    unauthorized
    (let [{:keys [username]} params
          db @state]
      (when-some [followed (rel/row db :User [= :user/username username])]
        (let [tx (follow-user {:follower-uid authed-uid, :followed-uid (:user/id followed)})
              db (swap! state rel/transact tx)]
          (profile-response followed authed-uid db))))))

(defn api-unfollow [{:keys [authed-uid params]}]
  (if-not authed-uid
    unauthorized
    (let [{:keys [username]} params
          db @state]
      (when-some [followed (rel/row db :User [= :user/username username])]
        (let [tx (unfollow-user {:follower-uid authed-uid, :followed-uid (:user/id followed)})
              db (swap! state rel/transact tx)]
          (profile-response followed authed-uid db))))))

(defn api-get-articles [{:keys [authed-uid params] :as req}]
  (let [{:strs [limit offset author favorited tag]
         :or {limit 20
              offset 0}} params
        db @state
        q (list-article-query {:uid authed-uid
                               :author author
                               :favorited favorited
                               :tag tag})
        xf (comp (drop offset) (take limit))
        rs (rel/q db q {:xf xf})]
    (article-list-response rs authed-uid db)))

(defn api-get-articles-feed [{:keys [authed-uid params]}]
  (let [db @state
        q (feed-article-query {:uid authed-uid})
        {:strs [limit offset]
         :or {limit 20
              offset 0}} params
        xf (comp (drop offset) (take limit))
        rs (rel/q db q {:xf xf})]
    (article-list-response rs authed-uid db)))

(defn api-get-article [{:keys [authed-uid params]}]
  (let [{:strs [slug]} params
        db @state]
    (some-> (rel/row db Article [= :article/slug slug])
            (article-response authed-uid db))))

(defn api-create-article [{:keys [authed-uid params] :as req}]
  (let [{:strs [title description body tagList]} params]
    (if-not authed-uid
      unauthorized
      (let [ts (Instant/now)
            aid (swap! id-ctr inc)
            tx (create-article {:aid aid
                                :uid authed-uid
                                :created-at ts
                                :title title
                                :description description
                                :body body
                                :tag-list tagList})
            db (swap! state rel/transact tx)]
        (-> (rel/row db Article [= :article/id aid])
            (article-response authed-uid db))))))

(defn api-update-article [{:keys [authed-uid params]}]
  (let [{:keys [slug]
         :strs [title description body tagList]} params]
    (cond
      (nil? authed-uid) unauthorized

      :else
      (let [ts (Instant/now)
            tx (update-article {:slug slug
                                :updated-at ts
                                :title title
                                :description description
                                :body body
                                :tag-list tagList})
            db (swap! state rel/transact tx)]
        (-> (rel/row db Article [= :article/slug (:slug params)])
            (article-response authed-uid db))))))

(defn api-delete-article [{:keys [authed-uid params]}]
  (let [{:keys [slug]} params]
    (cond
      (nil? authed-uid) unauthorized

      :else
      (let [tx (delete-article {:slug slug})
            _ (swap! state rel/transact tx)]
        {}))))

(defn api-create-comment [{:keys [authed-uid params]}]
  (let [{:keys [slug]
         :strs [body]} params]
    (if-not authed-uid
      unauthorized
      (let [ts (Instant/now)
            cid (swap! id-ctr inc)
            tx (create-comment {:cid cid, :uid authed-uid, :slug slug, :body body, :created-at ts})
            db (swap! state rel/transact tx)]
        (-> (rel/row db Comment [= :comment/id cid])
            (comment-response authed-uid db))))))

(defn api-delete-comment [{:keys [authed-uid params]}]
  (let [{:keys [id]} params]
    (cond
      (nil? authed-uid) unauthorized

      :else
      (let [tx (delete-comment {:cid id})
            _ (swap! state rel/transact tx)]
        {}))))

(defn api-get-comments [{:keys [authed-uid params]}]
  (let [{:keys [slug]} params
        q (article-comments-query {:slug slug})
        db @state
        rs (rel/q db q {:sort [:comment/created-at]})]
    (comment-list-response rs authed-uid db)))

(defn api-favorite [{:keys [authed-uid params]}]
  (if-not authed-uid
    unauthorized
    (let [tx (favorite-article {:uid authed-uid, :slug (:slug params)})
          db (swap! state rel/transact tx)]
      (some-> (rel/row db Article [= :article/slug (:slug params)])
              (article-response authed-uid db)))))

(defn api-unfavorite [{:keys [authed-uid params]}]
  (if-not authed-uid
    unauthorized
    (let [tx (unfavorite-article {:uid authed-uid, :slug (:slug params)})
          db (swap! state rel/transact tx)]
      (some-> (rel/row db Article [= :article/slug (:slug params)])
              (article-response authed-uid db)))))

(defn api-get-tags [_req]
  (let [rs (rel/q @state Tag)]
    {:status 200
     :body {"tags" (mapv :tag/tag rs)}}))

(defn wrap-auth [handler]
  (fn [req]
    (let [authorization (get-in req [:headers "authorization"])
          [_ token] (when authorization (re-find #"Token (.+)" authorization))]
      (if (some? token)
        (handler (assoc req :authed-uid (uid token)))
        (handler req)))))

(defn wrap-content-type [handler]
  (fn [req] (assoc-in (handler req) [:headers "Content-Type"] "application/json; charset=utf-8")))

(defn wrap-error [handler]
  (fn [req]
    (try
      (handler req)
      (catch Throwable e
        (let [rel-error (::rel/error (ex-data e))
              other-error (::error (ex-data e))]
          (case (or rel-error other-error)
            :foreign-key-violation {:status 422, :body {:msg "Not allowed"}}
            :unique-key-violation {:status 409, :body {:msg "Already exists"}}
            :check-violation {:status 422, :body {:msg "Invalid form fields"}}
            :unauthorized unauthorized
            :forbidden forbidden
            (do
              (println e)
              {:status 500
               :body {:msg "Internal server error"}})))))))

(defn middleware [handler]
  (-> handler
      wrap-auth
      wrap-error
      (cors/wrap-cors :access-control-allow-origin [#".+"]
                      :access-control-allow-methods [:get :put :post :delete])
      format/wrap-restful-format
      params/wrap-params))

(defn router []
  (routes
    (POST "/api/users/login" [] #'api-login)
    (POST "/api/users" [] #'api-register)
    (GET "/api/user" [] #'api-get-current-user)
    (PUT "/api/user" [] #'api-update-current-user)
    (GET "/api/profiles/:username" [] #'api-get-profile)
    (POST "/api/profiles/:username/follow" [] #'api-follow)
    (DELETE "/api/profiles/:username/follow" [] #'api-unfollow)
    (GET "/api/articles" [] #'api-get-articles)
    (GET "/api/articles/feed" [] #'api-get-articles-feed)
    (GET "/api/articles/:slug" [] #'api-get-article)
    (POST "/api/articles" [] #'api-create-article)
    (PUT "/api/articles/:slug" [] #'api-update-article)
    (DELETE "/api/articles/:slug" [] #'api-delete-article)
    (POST "/api/articles/:slug/comments" [] #'api-create-comment)
    (GET "/api/articles/:slug/comments" [] #'api-get-comments)
    (DELETE "/api/articles/:slug/comments/:id" [] #'api-delete-comment)
    (POST "/api/articles/:slug/favorite" [] #'api-favorite)
    (DELETE "/api/articles/:slug/favorite" [] #'api-unfavorite)
    (GET "/api/tags" [] #'api-get-tags)
    (OPTIONS "*" [] {:status 200})))

(def handler
  (-> (some-fn (router) (constantly {:status 404, :body {:msg "Not found"}}))
      middleware))

(defn start-server []
  (jetty/run-jetty #'handler {:port 6003, :join? false}))

(defonce default-server
  (delay (start-server)))

(defn stop []
  (alter-var-root #'default-server
                  (fn [d] (when (realized? d)
                            (.stop ^Server @d))
                    (delay (start-server)))))

(defn restart []
  (stop)
  @default-server)