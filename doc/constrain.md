# :constrain

The `:constrain` operator allows you to compose multiple constraints in one operation for concision.

This is unique among queries only because constraints run for their side effects, so fan-out and check results is a valid approach.

```clojure 
[[:from :Person]
 [:constrain
  [:req :name] 
  [:check [string? :name] [nat-int? :age]]
  [:fk Address {:address-id :id}]
  [:unique :email]]]
```

Pure sugar, otherwise unremarkable, see [constraints](constraints.md) for more detail on individual constraint operators.`