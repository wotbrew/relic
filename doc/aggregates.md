# Aggregates 

The [`:agg`](agg.md) statement permits the use of aggregate functions, such as count, sum and so on. See below for a listing of what you can do.

Stats

- [`max`](max.md) the max val by some [expr](expr.md).
- [`min`](min.md) the min val by some [expr](expr.md).
- [`rel/max-by`](max-by.md) the max row by some [expr](expr.md).
- [`rel/min-by`](min-by.md) the min row by some [expr](expr.md).

Top or bottom N

- [`rel/bottom`](bottom.md) find the lowest n vals for some [expr](expr.md).
- [`rel/bottom-by`](bottom-by.md) find the lowest n rows for some [expr](expr.md).
- [`rel/top`](top.md) find the biggest n vals for some [expr](expr.md).
- [`rel/top-by`](top-by.md) find the biggest n rows for some [expr](expr.md).

Counts

- [`count`](count.md) row count, or row count for all rows meeting some [predicate](expr.md).
- [`rel/count-distinct`](count-distinct.md) val count for all distinct vals of some [expr](expr.md).

Other

- [`rel/any`](any.md) / [`rel/not-any`](not-any.md) check if any row meets a [predicate](expr.md) (or not)