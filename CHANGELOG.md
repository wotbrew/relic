# Change Log

## 0.1.7

- Fix #60 (:update dropping rows that did not get updated with no :where)
- Fix using uniques for joins containing more than one column


## 0.1.5

- `:agg` behaviour changed for empty relations when grouping all rows (see below)
- relic databases are wrapped with a custom type (RelicDB) to avoid surprises with metadata and changes to the databases
  state 'as a map'. Continues to meet all the map interfaces so should not cause any breakage. [#52](https://github.com/wotbrew/relic/issues/52)
- existing covering indexes are now scored and used by joins (`:join` & `:left-join`). Previously you had to mat the join or create a specific `:hash` index with exactly the same indexed expressions.

### `:agg` behaviour change (minor breaking)

`:agg` with over all rows now always returns a row with default values instead of nil.

e.g `[[:const] [:agg [] [:n count]]]` will return `[{:n 0}]` instead of `nil`.

This is closer to SQL and will mean a constraint like `[[:from :a] [:agg [] [:n count]] [:check [= 1 :n]]]` will throw if there are no rows, instead of just if there are more than 1.

## 0.1.4

- fixed no results when using `[:_ kw]` with indexed `:where` queries
- added `rel/row`
- added `rel/exists?`
- `rel/transact` can now take a function of db to tx op, i.e a transaction function.
- `:constrain` form internals change, fixing issues with upserts not seeing `:unique` indexes, and therefore throwing.
- `:unique` exception messages contain the index keys (and table if possible) to aid in debugging

## 0.1.3

- [#47](https://github.com/wotbrew/relic/issues/47) db without meta is acceptable to queries
- fixed nil behaviour on multi-expr count-distinct
- set-concat returns empty-set if no non-nil values

## 0.1.2

### Fixed

- fixed glitches with materialized sorted-group deletes
- `(index)` now returns nil for non-index operators (keep it secret, keep it safe.)

## 0.1.1 

### Fixed

- [#45](https://github.com/wotbrew/relic/issues/45) join expr that return nil are not used in index lookups

## 0.1.0 

Initial release
