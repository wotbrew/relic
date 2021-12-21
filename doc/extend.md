# `:extend`

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

## See also

- [`:select`](select.md)
- [`:agg`](agg.md)
- [Unsafe transformation](unsafe-transform.md)