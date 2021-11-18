# relic

`STATUS: PRIMORDIAL TAR, not ready quite yet...`

A Clojure(Script) functional __relational__ programming library. 

``` 
Show me your flowchart and conceal your tables, and I shall continue to be mystified.
Show me your tables, and I won't usually need your flowchart; it'll be obvious." 
-- Fred Brooks, The Mythical Man Month
```

## Pitch 

Do you ever feel _map fatigue_ programming with business data with all its silly associations and rules and dependencies?

![despair](doc/tar2.png)

Are you spending too much time writing mechanical wiring and glue that is not directly expressing the actual business logic?

![in the tar pit](doc/tar.jpeg)

Did you try [meander](https://github.com/noprompt/meander), [core.logic](https://github.com/clojure/core.logic), [datascript](https://github.com/tonsky/datascript) and every graph-map-database under the sun but still do not feel [out of the tar pit](http://curtclifton.net/papers/MoseleyMarks06a.pdf)?

`relic` might help, but its not a medical professional. Its a functional relational programming library.

- seperates essential (minimal state, relationship expression) from incidental (accidental data, mechanisms, and ad-hoc structure).
- like SQL for clojure data. _but awesome_.
- munge with joy via the glorious relational open access information model.
- laugh at cache invalidation problems with __incremental materialized views__ and dataflow sorcery.
- relational expressions as __data__, open to introspection and analysis. Gives static tools a fighting chance..
- __constraints__ a-la-carte, gain confidence. I'm not talking just shape data, say `order can have at most 10 items if its associated customer is called bob and its tuesday` via a constraint across a join.

Definitely not at all like the other graph databases in clojure. this time its different, really.

## Brief tour

I like to use `rel` as an alias.

```clojure
(require '[com.wotbrew.relic :as rel])
```

This is a _relvar_, Think SQL view/tables/queries all as one idea, a relational expression.

```clojure 
 [[:table :Customer]
  [:where [= :id 42]]]
 ```

All `relic` systems that are worth anything will need at least one`:table`. Your `:table`s form your givens, information from the outside world, from users, from an external datasource.


``` clojure
[[:table :Customer]]
```

You can see the relvar itself doesn't contain the rows. A relvar is a description of data, not the data itself - _good design is just breaking things apart_.

You can derive relvars from relvars. All your favorites are here, filtering (`:where`), computing columns (`:extend`), joins (`:join` & `:left-join`), grouping and aggregation (`:agg`) and more.

Because relvars are just vectors, they just sort of lounge around being values. To put them to work to get a _relation_ we have to feed some data into our tables.

`relic` databases are boring clojure maps, you manipulate data in tables with the `transact` function. These databases are immutable, they are literally just maps. We are still programming in clojure after all, no nasty surprises.

```clojure 
(def db (rel/transact {} [:insert [[:table :Customer]] {:id 42, :name "bob"} {:id 43, :name "alice"}])

db 
;; =>
{:Customer #{{:id 42, :name "bob"}, {:id 43, :name "alice"}}}

```

Now we have our state, we can ask questions of relic to find _relations_, as you would a SQL database. You see how relvars and queries _are the same_.

```clojure 
(rel/q db [[:table :Customer] [:where [= :id 42]]]) 
;; => 
#{{:id 42, :name "bob"}}

(rel/q db [[:table :Customer] [:where [= :name "alice"]]])
;; => 
#{{:id 43, :name "alice"}}
```

Ok ok, neat but not _cool_.

Ahhhh... but you don't understand, `relic` doesn't just evaluate queries like cave man technology - it is powered by a data flow graph.
`relic` knows when your data changes, and it knows how to modify dependent relations in a smart way.

```clojure 
(rel/materialize db [[:table :Customer] [:where [= :id 42]]])
;; => returns the database, its value will be the same (hint: metadata).
{:Customer #{{:id 42, :name "bob"}, {:id 43, :name "alice"}}}
```
You can materialize any relvar such that it will be maintained for you as you modify the database. In other words `relic` has __incremental materialized views__.

`materialize` will return a new _database_ that looks and smells the same, but queries against materialized relvars will be instant, and as you change data in tables, those changes will flow to materialized relvars automatically.

relic attempts to deliver on the promise of seperating out essential data and essential computation from accidental. One way it does this is the materialization, the data flow graph is not part of the value domain. It sits in metadata where it belongs. Your databases value is just your state - no machinery.

If you read the tarpit paper, you might find this [real estate example](https://github.com/wotbrew/relic/blob/masterel/dev/examples/real_estate.clj) informative.

## Relvar reference

Relvars are always vectors, they are your relational expressions.

The are vectors of the form `[& statement]` where each statement is itself a vector representing `[operator & args]`.

e.g 

```clojure 
[[:from Customer]
 [:where [= :firstname "fred"]]
 [:extend [:fullname [str :firstname :lastname]]
 [:join Order {:customer-id :customer-id}]]
```

Each statement depends on the above statements, this forms a dataflow graph. Unlike SQL, order matters e.g you cannot use a column that has not been defined in some preceding statement.


### Name your state relations with `:table`

```clojure 
[[:table :Customer]]
```

Tables are your primary variable, their data is provided to relic rather than derived. 

Unlike in SQL tables are sets of rows, not bags / multi-sets.

### Filtering with `:where`

```clojure 
[[:table :Customer]
 [:where [= :age 42]]]
```

Restricts a relation to those rows matching some set of expressions, expressions are vectors where keywords are templated in as cols
e.g `[= :age 42]` the first value needs to be a function, rows will be tested using a form like `(= (:age row) 42)`
 
relic expressions are as powerful as function expressions, see [here](#expression-reference) 
 
spec: `[:where & expr]`

### Adding new columns with `:extend`

```clojure 
[[:table :Customer]
 [:extend [:fullname [str :firstname :lastname]]]]
```

Adds columns by computing [expressions](#expression-reference)

spec: `[:extend & extensions]`

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

### Embed a relvar as a statement with `:from` 

```clojure 
(def Customer [[:table :Customer]])

[[:from Customer]
[:where [= :age 42]]
```

Allows you to splice relvars in to the leading statement position. Allows relvar re-use in literals without conj.

spec: `[:from relvar]`

### SQL style join with `:join`

```clojure 
(def Customer [[:table :Customer]])
(def Order [[:table :Order]])

[[:from Customer]
 [:join Order {:id :customer-id}]]
```

Joins two relations together, returning the product of matching rows. The columns in the relation on the right will be preferred in a conflict.

Like `set/join`, clause is a map of expressions on the left to expressions on the right.

spec: `[:join ?relvar ?clause]`

### SQL style `:left-join`

```clojure 
(def Customer [[:table :Customer]])
(def Order [[:table :Order]])

[[:from Order]
 [:left-join Customer {:customer-id :id}]]
```

spec: `[:left-join ?relvar ?clause]`

### Selecting columns with `:project`

```clojure 
[[:table :Customer]
 [:project :firstname :lastname]]
```

Like `select-keys` or `set/project` the resulting relation will contain only the projected keys. See also [`:select`](#sql-style-select)

As `relic` is set based, `:project` results will always be deduplicated when queried.

spec: `[:project & col]`
  
### Removing columns `:project-away` 

```clojure 
[[:table :Customer]
 [:project-away :age :firstname :lastname]]
```

Inverse of `:project` will instead omit the keys in `cols` from the resulting relation.

spec: `[:project-away & col]`

### SQL style `:select`

```clojure 
[[:table :Customer]
 [:select :firstnam, :lastname [:fullname [str :firstname :lastname]]]]
```

A mix and match expressions suitable for `:project` and `:extend` resulting in a SQL-style `:select`.

spec: `[:select & binding|col]`
  
### Aggregation & grouping with `:agg`

```clojure 
[[:table :Customer]
 [:agg [:age] [:count count]]]
```

Groups rows under the keys and performs aggregate expressions on them.

See [aggregate expressions](#aggregate-reference).

spec: `[:agg [& col] & agg-extension]`

### Flatten trees with `:expand` 

Lets you deal with nested data and flatten it into a form better suited to relational programming.

```clojure 
(rel/q db Order)
;; =>
#{{:customer-id 42, 
  :items [{:product 1, :quantity 2}, {:product 2, :quantity 1}]}}

;; =>
(def OrderItem
  [[:table :Order]
   [:expand [[:product :quantity] :items]])
   
(rel/q db OrderItem)
;; =>
#{{:customer-id 42, :product 1, :quantity 2}, {:customer-id 42, :product 2, :quantity 1}}
```

spec: `[:expand & expansion]`
  
### Set `:union`

spec: `[:union relvar]`
  
### Set `:difference `

spec: `[:difference ?relvar]`
  
### Set `:intersection`

spec: `[:intersection relvar]`
  
### Rename columns with `:rename`

spec: ` [:rename {col new-col, ...}]`
  
### Qualify columns with `:qualify``

spec: `[:qualify namespace]`

### Constant relations with `:const`

spec: `[:const coll]`

### Using indexes

#### `:where`

expressions can be maps, rows are matched by testing `[= k v]`. `v` must be a constant value. if the first expression is a map then an index will be consulted.
e.g `[:where {:a 42, :b 42} ...]` will consult an index to discover rows where :a is 42 and :b is 42.

This is mostly useful in updates/deletes and ad-hoc queries as joins always use indexes.

## Expression reference

## Aggregate reference

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

## Environment (e.g for `$now`)

### Set the environment with `with-env`
### Update the environment with `update-env`
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
- *gulp*... Bigger datasets than memory

## Related work

- [core.logic](https://github.com/clojure/core.logic) 
- [datascript](https://github.com/tonsky/datascript)
- [clara-rules](https://github.com/cernerel/clara-rules)
- [odoyle-rules](https://github.com/oakes/odoyle-rules)
- [fyra](https://github.com/yanatan16/fyra)
- [meander](https://github.com/noprompt/meander)
- [doxa](https://github.com/ribelo/doxa)

Any many, many more, sorry if I forgot you, give me a PR.

## Thanks 

- [@bradb](https://github.com/bradb) for early sketch sessions
- [@tom-riverford](https://github.com/tom-riverford) for indulging me
- [@riverford](https://github.com/riverford) for being the greatest green grocers in devon that uses clojure

## LETS GO

![lets go](doc/tar3.png)

`#relic` on clojurians

Email and raise issues. PR welcome, ideas and discussion encouraged.

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