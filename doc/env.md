# The environment

In relic, you are encouraged to keep all of your parameters (e.g things that might affect the result of a query that are not part of the query definition itself) in tables.

However, in real programs we treat many parameters as globals in practice, such as the current time, or the value of an environment variable.

In relic the 'environment' is a special table designed to hold global parameters, which comes with some sugar to
make working with things like time a bit easier.

```clojure 
;; replaces the environment with the given map, returning a new db
(rel/with-env db {:now (System/currentTimeMillis)})

;; you can reference the environment in queries with the Frel/env special expression form
[[:select [:seconds [/ [rel/env :now] 1000]]]

;; get the env map
(rel/get-env db)
;; =>
{:now 1640787329215}

;; apply a fn to the env map
(rel/update-env db assoc :now (System/currentTimeMillis))
```

As with other tables, you want to only store essential state.

So in the case of time, your env may hold the raw ms time, and you could then derive views for various different
local times, projections, datatypes etc that your queries can use.

```clojure 
(defn instant [ms] (Instant/ofEpohMilli ms))
(defn local-date-time [instant zone] (LocalDateTime/ofInstant instant (ZoneId/of zone)))

(def Time 
 [[:select 
   [:time/epoch-ms [rel/env :now]]
   [:time/instant [instant :time/epoch-ms]]
   [:time/london [local-date-time :time/instant "Europe/London"]]])
```