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

- seperates essential (minimal state, relationship expression) from incidental (accidental data, mechanisms, and ad-hoc structure)
- like SQL for clojure data. _but awesome_.
- munge with joy via the glorious relational open access information model, great for sloppy domains with lots of change.
- laugh at cache invalidation problems with __incremental materialized views__ via relics dataflow black magic
- relational expressions as __data__, and open to introspection and analysis. Gives static tools a fighting chance.
- __constraints__ a-la-carte, gain confidence. I'm not talking just shape data, say things like each 'order can have at most 10 items if its associated customer is called bob' via a constraint across a join.

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

A relvar on their own don't do anything, they are just vectors that just sort of lounge around. To put them to work to get a _relation_ we have to feed some data into our tables.

`relic` databases are just plain old clojure maps, you manipulate data in tables with the `transact` function. These databases are immutable, we are still programming in clojure after all, no nasty surprises.

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