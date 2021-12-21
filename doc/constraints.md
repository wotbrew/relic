# Constraints

Constraints are just relvars ending in one of the constraint statements [`:unique`](unique.md),
[`:fk`](fk.md) and [`:check`](check.md). These operators throw typically exceptions when the constraints are not met.

To constrain a database, you [`materialize`](materialization.md) constraint relvars (and they can be removed with `dematerialize`).

Constraints can apply to _any_ relvar, so you can apply constraints to derived relvars and joins, here
is the obligatory `order can have at most 10 items if its associated customer is called bob and its tuesday` constraint.

```clojure 
[[:from Order] 
 [:join Customer {:customer-id :customer-id}]
 [:where [= "bob" :firstname] [= "tuesday" [uk-day-of-week [::rel/env :now]]]]
 [:check {:pred [<= [count :items] 10],
          :error [str "order can have at most 10 items if its associated customer is called bob and its tuesday, found: " [count :items]]}]]
```

As it is convenient to specify multiple constraints on a relvar in one form, a special
`:constrain` statement is provided.

e.g

```clojure 
[[:from Customer]
 [:constrain
   [:check [string? :firstname] [string? :lastname] [nat-int? :age]]
   [:unique :customer-id]
   [:fk Address {:address-id :address-id}]]]
```

## Constraint operators

- [`:check`](check.md) ensure certain predicates hold
- [`:req`](req.md) ensure cols exist
- [`:fk`](fk.md) ensure a referenced row exists in some other relvar
- [`:constrain`](constrain.md) combine multiple constraints on a relvar
- [`:unique`](unique.md) unsure only one row exists for some set of [expressions](expr.md)