# :check

The check [constraint](constraints.md) can be used for predicative assertions on rows.

You could use it as a hook for spec or malli, or some inline validation.

You can either use relic [expressions](expr.md) as a check, or a map to override error messages.

```clojure

[[:from :Person]
 [:check [string? :name]]]

;; or a map if you want a cleaner error
[[:from :Person]
 [:check {:pred [string? :name]
          :error "All people must have a string :name"}]]

;; you can mix and match multiple check constraints
;; (they run left to right)
[[:from :Person]
 [:check [string? :name] [nat-int? :age]]]

;; the :error key actually takes a relic expression (or function like always!)
;; so you can get really fancy, (think malli / expound errors!)
[[:from :Person]
 [:check {:pred [string? :name]
          :error [str "Expected a string :name, got " [type :name]]}]]
```