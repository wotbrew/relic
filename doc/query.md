# Query

Relic query language, or RelicQL. In relic queries are your primitive for describing the data in a domain, and views on it.

- A query is always a vector
- queries are made up of operations, themselves vectors of the form `[operator & args]`.

```clojure 
;; example query from the tcp-h benchmark suite
[[:from :lineitem]
 [:where [<= [compare :l_shipdate #inst "1998-09-02"] 0]]
 [:agg
   [:l_returnflag
    :l_linestatus]
 [:sum_qty [rel/sum :l_quantity]]
 [:sum_base_price [rel/sum :l_extendedprice]]
 [:sum_disc_price  [rel/sum [* :l_extendedprice [- 1 :l_discount]]]]
 [:sum_charge [rel/sum [* :l_extendedprice [- 1 :l_discount] [+ 1 :l_tax]]]]
 [:avg_qty [rel/avg :l_quantity]]
 [:avg_price [rel/avg :l_extendedprice]]
 [:avg_disc [rel/avg :l_discount]]
 [:count_order count]]]
```

queries compose by adding to the vector, this is in contrast to other approaches where query optimisers take care of the overall ordering.

In relic, data always flows top-to-bottom.

The super-power is that relic allows you to materialize the query. This will convert the query into a
DAG to support incremental re-computation as data in tables changes.

With few exceptions (direct index lookup) you can materialize any query with `rel/materialize`.

## Rationale of form

In the tar pit paper, the language used to express its 'relvars' was a traditional expression tree (e.g `union(a, b)`).

I wanted a data-first clojure dsl that met two goals, like any good data dsl I wanted to compose using regular clojure functions,
and I wanted them to be easy to write and read as literals without ide support.

The vector form I think is close to SQL, with a nice top-to-bottom reading flow. Each operation is self-contained in its own
delimited form, and so you can create new queries with `conj`, split them with `split-at` and so on.

## Operators

- [`:from`](from.md) a.k.a start here
- [`:where`](where.md) to select only certain rows
- [`:join`](join.md) sql style relation inner join
- [`:left-join`](left-join.md) sql style relational left join
- [`:extend`](extend.md) compute new columns
- [`:select`](select.md) project a subset of columns, computations
- [`:without`](without.md) drop columns
- [`:expand`](expand.md) flatten nested sequences
- [`:agg`](agg.md) apply aggregates over rows in groups
- [`:const`](const.md) provide a constant relation
- [`:difference`](difference.md) set diff
- [`:intersection`](intersection.md) set intersection
- [`:union`](union.md) set union
- [`:rename`](rename.md) rename columns
- [`:qualify`](qualify.md) qualify (namespace) columns

## Constraints

- [`:check`](check.md) ensure certain predicates hold 
- [`:req`](req.md) ensure cols exist
- [`:fk`](fk.md) ensure a referenced row exists in some other query/table
- [`:unique`](unique.md) unsure only one row exists for some set of [expressions](expr.md)
- [`:constrain`](constrain.md) combine multiple constraints on a query/table

## Indexes and direct index access

- [`:hash`](hash.md) standard one-to-many hash index
- [`:btree`](btree.md) standard one-to-many sorted index
- [`:lookup`](lookup.md) explicit index lookup