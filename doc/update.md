# :update

The `:update` [transact](transact.md) operation allows you to modify existing rows by applying a function to them.

In lieu of a function, a SQL style set-map can be used instead for column updates/overwrites.

e.g 

```clojure 
(def db 
  (rel/transact {} {:Counter [{:n 0}]}))
  
;; functions can be used
(rel/transact db [:update :Counter #(update % :n inc)])
;; =>
{:Counter #{{:n 1}}}

;; a set map can be terser, it takes functions/relic-expressions in value position.
(rel/transact db [:update :Counter {:n [inc :n]}])
;; =>
{:Counter #{{:n 1}}}

;; as set maps take relic expressions, you can overwrite columns.
(rel/transact db [:update :Counter {:n 42}])
;; =>
{:Counter #{{:n 42}}}

;; additional expresssions can be passed as args to filter updated rows
(rel/transact db [:update :Counter {:n inc} [even? :n]])
;; => {:Counter #{{:n 1}}}
(rel/transact db [:update :Counter {:n inc} [odd? :n]])
;; => {:Counter #{{:n 0}}}  
```