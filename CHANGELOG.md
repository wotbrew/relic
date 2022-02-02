# Change Log

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