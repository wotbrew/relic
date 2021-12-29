# :extend

Computes new columns by binding the results of [expressions](expr.md).

If the binding collides with an existing column, the existing column is overwritten.

Accepts multiple extension forms, each a pair `[binding expr]`.

The binding determines how the result should be added to the row:

- a column (keyword), just overwrite or add the column to the row
- a collection, assume the result is a map, and `select-keys` the collection and merge into the row.
- the special keyword `:*`, merge the entire result into the row.

## Form

```clojure 
extend = [:extend & extension]
extension = [binding expr]
binding = col | [& col] | :*
```

See [expression reference](expr.md) for more information on expressions.

## Examples 

```clojure
[[:from :Customer]
 [:extend [:fullname [str :firstname " " :lastname]]]]

(def TotalSpend
  [[:from :Order]
   [:agg [:customer-id] [:total-spend [rel/sum :total]]]])
```

Sub queries with `:$1` and `:$`

```clojure 

;; $1 to sub select another row, i.e an implicit left-join.
(def CustomerStats
  [[:from :Customer]
   [:extend [[:total-spend] [:$1 TotalSpend {:customer-id :customer-id}]]]])

;; would result in a relation something like:
#{{:customer 42, :total-spend 340.0M}}

;; $1 to bind rows from another query to a column
(def Order
  [[:from :Order]
   [:extend [:items [:$ OrderItem {:order-id :order-id}]]]])

;; would result in a relation something like:
#{{:customer-id 42,
   :total 340.0M
   :items #{{:product-id 43, :quantity 1} ...}}

```


## See also

- [`:select`](select.md)
- [`:agg`](agg.md)