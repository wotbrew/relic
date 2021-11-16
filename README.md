# relic

`STATUS: PRIMORDIAL TAR, not ready quite yet...`

A Clojure(Script) library for doing functional relational programming in clojure.

- in memory relational programming for your clojure data  
- declarative data dsl
- incremental materialized views
- constraints 

## Pitch 

Do you ever feel like this when programming with business data and pipelines of maps with maps and more maps, maps for 
breakfast, dinner and supper?

![tarpit](doc/tar.jpeg)

`relic` might help.

## Tutorial

This is a _relvar_, a _relvar_ is just a vector containing a series of relational statements.
``` clojure
[[:table :Customer]]
```

You derive relvars from relvars using operators, like `:where`, `:extend`, `:join`, `:agg` and so on.

```clojure 
 [[:table :Customer]
  [:where [= :id 42]]]
 ```

Relvars on their own don't do anything, to get a _relation_ we have to pair them with some state.

State is stored in a plain old clojure map which we'll call the `db` in this example, you manipulate the state with the `transact` function.

```clojure 
(require '[com.wotbrew.relic :as rel])

(def db (rel/transact {} [:insert [[:table :Customer]] {:id 42, :name "bob"} {:id 43, :name "alice"}])

db 
;; =>
{[[:table :Customer]] #{{:id 42, :name "bob"}, {:id 43, :name "alice"}}}

```

Now we have our state, we can ask questions of relic to find _relations_, as you would a SQL database.

```clojure 
(rel/q db [[:table :Customer] [:where [= :id 42]]]) 
;; => 
#{{:id 42, :name "bob"}}

(rel/q db [[:table :Customer] [:where [= :name "alice"]]])
;; => 
#{{:id 43, :name "alice"}}
```

Ok ok, not very cool. _What if I told you_ that you can materialize any relvar such that 
it will be maintained for you as you modify the database. In other words `relic` has incremental materialized views.

```clojure 
(rel/materialize db [[:table :Customer] [:where [= :id 42]]])
```

`materialize` will return a new _database_ that looks and smells the same, but will maintain the results
of the passed relvars as you `transact` against the database __incrementally__.

If you read the tarpit paper, you might find this [real estate example](https://github.com/wotbrew/relic/blob/masterel/dev/examples/real_estate.clj) informative.

## Relvar reference

Relvars are always vectors, a each element being some kind of relational statement. 

e.g 

```clojure 
[[:from Customer]
 [:where [= :firstname "fred"]]
 [:extend [:fullname [str :firstname :lastname]]
 [:join Order {:customer-id :customer-id}]]
```

As they are just data, you can def them, just type them as literals or construct/manipulate them programmatically. Each statement depends on the above statements, this forms a dataflow graph. 
Unlike SQL, order matters e.g you cannot use a column that has not been defined in some preceding statement.
### Naming your state `[[:table ?name]]` 

Your tables contain the input to relic, normally data from the outside world or that is inputted by the user. You update/insert/delete against tables.

The idea is you only need tables for _essential_ state, you derive information by joining/slicing/dicing your state tables via relational operators.

### Nesting `[[:from ?relvar]]`

Provides some convenience for deriving relvars without conj, e.g `[[:from A] [:where [= :foo 42]]]` instead of `(conj A [:where [= :foo 42]])`  

### Filtering `[... [:where & ?exprs]]` 

Restricts results to those matching some set of expressions, expressions are vectors where keywords are templated in as cols
e.g `[= :age 42]` the first value needs to be a function, rows will be tested using a form like `(= (:age row) 42)`

#### Using indexes

expressions can be maps, rows are matched by testing `[= k v]`. `v` must be a constant value. if the first expression is a map then an index will be consulted.
e.g `[:where {:a 42, :b 42} ...]` will consult an index to discover rows where :a is 42 and :b is 42.

This is mostly useful in updates/deletes and ad-hoc queries as joins always use indexes.
  
### Adding new keys `[... [:extend & ?extensions]]`

Extension adds new values to rows, an extension looks like this: 

`[:foo [str :a :b]]`

In this case it says provide the column `:foo` by concating `:a` and `:b` with `str`. 

#### Extension forms:

- `[k expr]` bind result of expr to k
- `[[& k] expr]` merge keys from the result of expr

You can use the functions `rel/join-first` and `rel/join-coll` in expression position for sub-selects.

e.g 

```clojure
(def TotalSpend
  [[:from Order] 
   [:agg [:customer-id] [:total-spend [rel/sum :total]]]])

;; join-first to splice columns from another row, i.e an implicit left-join.
(def CustomerStats 
  [[:from Customer] 
   [:extend [[:total-spend] (rel/join-first TotalSpend {:customer-id :customer-id})]]])
;; would result in a relation something like:
#{{:customer 42, :total-spend 340.0M}}

;; join coll to get a set of rows as a column
(def Order 
  [[:from Order]
   [:extend [:item (rel/join-coll OrderItem {:order-id :order-id})]]])
;; would result in a relation something like:
#{{:customer-id 42,
   :total 340.0M
   :items #{{:product-id 43, :quantity 1} ...}}

```


### Keeping a subset of columns `[... [:project & cols]]`

Like `select-keys` or `set/project` the resulting relation will contain only the projected keys.
  
### Removing columns `[... [:project-away & cols]]`  

Inverse of `:project` will instead omit the keys in `cols` from the resulting relation.

### SQL style select `[... [:select & ?binding|col]]`

A mix and match expressions suitable for `:project` and `:extend` resulting in a SQL-style `:select`.
  
### SQL style join `[... [:join ?relvar ?clause]]` 

Like `set/join`, clause is a map of expressions on the left to expressions on the right.
  
### SQL style left join `[... [:left-join ?relvar ?clause]]`

?clause is a map of {left-expr, right-expr}, e.g` [[:from Customer] [:join Order {:customer-id :customer-id}]]`
  
### Aggregation & grouping `[... [:agg [& ?keys] & aggregations]]`

Groups rows under the keys and performs aggregation on them to
construct new values. e.g `[:agg [:a] [:summed-b [rel/sum :b]]]` WIP.

### Flattening trees `[... [:expand & ?expansion]]`
  
### Set union `[... [:union ?relvar]]`
  
### Set difference `[... [:difference ?relvar]]`
  
### Set intersection `[... [:intersection ?relvar]]`
  
### Rename columns `[... [:rename ?renames]]`
  
### Qualify columns `[... [:qualify ?namespace]]`

### Constant relations `[[:const ?coll]]`

## Query reference 

### Get rows with `q`
### Apply a sort with `:sort` and `:rsort` 
### Use a transducer with `:xf`

## Transact reference 

### Modify state with `transact`
### Insert one or more rows `[:insert ?relvar & ?row]`
### Terse insert `{?relvar1 ?rows, relvar2 ?rows}`
### Update rows `[:update ?relvar ?f-or-set-map & where-expr]`
### Delete rows by where filter `[:delete ?relvar & where-expr]`
### Delete rows`[:delete-exact ?relvar & ?row]`


### `TODO upsert`

## Materialization reference 

### materialize relations with `materialize`
### save memory with `dematerialize`

## Constraint reference 

### Apply constraints with `constrain`
### Ensure only one row exists for a set of columns `:unique` 
### Ensure a referenced row exists `:fk`
### Check columns or rows always meet some predicate `:check`

## Tracking changes

### Tag relvars you want to track with `watch`
### Transact with change tracking `tracked-transact`

## Environment (time, variables and such)

### Set an environment with `with-env`
### Update an environment with `update-env`
### Use the environment in relvars with `::rel/env`

## Other cool stuff

### Speculate with `what-if`
### Explore your data `ed`
### Get raw indexes with `index`

## TODO 

- Recursive (fixed point?) relvars to enable logic type queries
- Tests
- Docs  
- Update / Upsert
- Compile time relvars, the great unboxing, custom record types
- Better docs 
- Tooling, spec  
- Uhhhh... Bigger datasets than memory

## Related work

- [core.logic](https://github.com/clojure/core.logic) 
- [datascript](https://github.com/tonsky/datascript)
- [clara-rules](https://github.com/cernerel/clara-rules)
- [odoyle-rules](https://github.com/oakes/odoyle-rules)
- [fyra](https://github.com/yanatan16/fyra)

## License

Copyright © 2021 Dan Stone (wotbrew)

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.