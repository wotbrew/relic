# :left-join

Like [join](join.md) but returns rows even if there are no matches in the right relation.

Like SQL `LEFT JOIN`.

## Form

```clojure 
left-join = [:left-join right clause & more]
right = relvar | table
clause = {left-expr right-expr, ...}
```

## Examples

```clojure
[[:from :Customer]
 ;; if orders are missing, you will get customer rows on their own
 [:left-join :Order {:id :customer-id}]]
```