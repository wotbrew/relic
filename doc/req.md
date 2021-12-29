# :req

The `:req` [constraint](constraints.md) ensures columns exist. It is short hand for a [`:check`](check.md) constraint.

e.g 

```clojure 
[[:from :Customer]
 [:req :customer/id :customer/firstname]]
```