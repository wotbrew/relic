# `:join`

Joins two relations together, returning the product of matching rows. The columns in the relation on the right will be preferred in a conflict.

Like `set/join`, clause is a map of expressions on the left to expressions on the right.

Accepts both tables (keywords) and other relvars.

Similar to an `INNER JOIN` in SQL.

## Form

```clojure 
join = [:join right clause & more]
right = relvar | table
clause = {left-expr right-expr, ...}
```

## Examples

```clojure
[[:from :Customer]
 [:join :Order {:id :customer-id}]]
```