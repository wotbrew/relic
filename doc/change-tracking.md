# Change Tracking

One of the key features of relic is discrete change tracking, this enables integration with other reactive or signal graph
systems such as react or reagent.

Any relvar can be watched so that you can ask what rows were added or deleted by a transaction with `track-transact`.

```clojure 
;; given a relvar we are interested in watching
(def ViewModel 
  [[:from :User]
   [:where :selected]])
   
;; lets assume the state looks like this:
{:User [{:user "alice", :selected false}
        {:user "bob", :selected false}]}

;; enable change tracking for our relvar with watch
;; this returns a new relic database, don't worry its totally pure!
(def db (rel/watch db relvar))   
  
;; now we have a watched relvar, we can track-transact and receive changes to it.
(rel/track-transact db [:update :User {:selected true} [= :user "bob"]])
;; =>
{
 ;; the first key is just the :db as with the transactions applied
 :db {:User #{{:user "alice", :selected false}, {:user "bob", :selected true}}},
 ;; you also get a :changes key, which is the map of {relvar changes} for all watched relvars
 :changes
  {
   ;; our watched ViewModel relvar from earlier
   [[:from :User}]
    [:where :selected]] 
    
   ;; added rows and deleted rows are returned.
   {:added [{:user "bob", :selected true}],
    :deleted []}
    
    
;; you can unwatch
(def db (rel/unwatch db relvar))
```

You can `watch` any [relvar](relvar.md), table or derived view. 
To enable change tracking `watch` [materializes](materialization.md) relvars so the same memory/performance trade-offs apply.