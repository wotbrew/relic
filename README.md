# relic

`STATUS: PRIMORDIAL TAR, this readme, and library are still a work in progress..`

A Clojure(Script) functional __relational__ programming library. 

``` 
"Show me your flowchart and conceal your tables, and I shall continue to be mystified.
 
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
- __constraints__ a-la-carte, gain confidence. I'm not talking just shape data, say things like [order can have at most 10 items if its associated customer is called bob and its tuesday](#constraint-reference).

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

All `relic` systems that are worth anything will need at least one `:table`. Your `:table`s form your givens, information from the outside world, from users, from an external datasource.


``` clojure
[[:table :Customer]]
```

You can see the relvar itself doesn't contain the rows. A relvar is a description of data, not the data itself - _good design is just breaking things apart_.

You can derive relvars from relvars by just adding elements to the vector.  All your favorites are here, filtering (`:where`), computing columns (`:extend`), joins (`:join` & `:left-join`), grouping and aggregation (`:agg`) and more.

```clojure 
[[:table :Customer]
 [:where [= :id 42]]
 [:extend [:fullname [str :firstname " " :lastname]]]
```

Because relvars are just vectors, they just sort of lounge around being values. To put them to work to get a _relation_ we have to feed some data into our tables.

`relic` databases are boring clojure maps, you manipulate data in tables with the `transact` function. These databases are immutable, they are literally just maps. We are still programming in clojure after all, no nasty surprises.

```clojure 
(def db (rel/transact {} [:insert :Customer {:id 42, :name "bob"} {:id 43, :name "alice"}])

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

relic attempts to deliver on the promise of separating out essential data and essential computation from accidental. One way it does this is the materialization, the data flow graph is not part of the value domain. It sits in metadata where it belongs. Your databases value is just your state - no machinery.

You can do more than query and materialize with relic, you can [react to changes](#tracking-changes), use [constraints](#constraint-reference) and [more](#other-cool-stuff).

If you read the tarpit paper, you might find this [real estate example](https://github.com/wotbrew/relic/blob/master/dev/examples/real_estate.clj) informative.

## Relvar reference

Relvars are always vectors, they are your relational expressions.

The are vectors of the form `[& statement]` where each statement is itself a vector representing `[operator & args]`.

e.g 

```clojure 
[[:from Customer]
 [:where [= :firstname "fred"]]
 [:extend [:fullname [str :firstname " " :lastname]]
 [:join Order {:customer-id :customer-id}]]
```

Each statement depends on the above statements, this forms a dataflow graph. Order matters e.g you cannot use a column that has not been defined in some preceding statement.


### Name your state relations with `:table`

```clojure 
[[:table :Customer]]
```

Tables are your primary variable, their data is provided to relic rather than derived. The name provided to the table
is used to determine where data will be stored in the database map, e.g for `[[:table :Customer]]` the data will be stored under `:Customer`.

note: Unlike in SQL tables are sets of rows, not bags / multi-sets.

spec: `[:table table-name]`

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
 [:extend [:fullname [str :firstname " " :lastname]]]]
```

Adds columns by computing [expressions](#expression-reference)

spec: `[:extend & extensions]`

#### Extension forms:

- `[k expr]` bind result of expr to k
- `[[& k] expr]` merge keys from the result of expr
- `[::rel/* expr]` merge _all_ keys from the result of expr

You can use the special expressions `::rel/join-first` and `::rel/join-coll` in expression position for sub-selects.

e.g 

```clojure
(def TotalSpend
  [[:from Order] 
   [:agg [:customer-id] [:total-spend [rel/sum :total]]]])

;; join-first to splice columns from another row, i.e an implicit left-join.
(def CustomerStats 
  [[:from Customer] 
   [:extend [[:total-spend] [::rel/join-first TotalSpend {:customer-id :customer-id}]]]])
;; would result in a relation something like:
#{{:customer 42, :total-spend 340.0M}}

;; join coll to get a set of rows as a column
(def Order 
  [[:from Order]
   [:extend [:items [::rel/join-coll OrderItem {:order-id :order-id}]]]])
;; would result in a relation something like:
#{{:customer-id 42,
   :total 340.0M
   :items #{{:product-id 43, :quantity 1} ...}}
```

### Embed a relvar as a statement with `:from` 

```clojure 
(def Customer [[:table :Customer]])

[[:from Customer]
 [:where [= :age 42]]]
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

spec: `[:join relvar clause]`

### SQL style `:left-join`

```clojure 
(def Customer [[:table :Customer]])
(def Order [[:table :Order]])

[[:from Order]
 [:left-join Customer {:customer-id :id}]]
```

spec: `[:left-join relvar clause]`

### Selecting new columns with `:select`

```clojure 
[[:table :Customer]
 [:select :firstname :lastname [:fullname [str :firstname " " :lastname]]]
```

Like `select-keys` or `set/project` the resulting relation will contain only the projected & computed keys.

As `relic` is set based, `:select` results will always be deduplicated when queried.

spec: `[:select & col|binding]`
  
### Removing columns `:without` 

```clojure 
[[:table :Customer]
 [:without :age :firstname :lastname]]
```

Inverse of `:select` will instead omit the keys in `cols` from the resulting relation.

spec: `[:without & col]`

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
   [:expand [[:product :quantity] :items]]
   [:without :items]])
   
(rel/q db OrderItem)
;; =>
#{{:customer-id 42, :product 1, :quantity 2}, {:customer-id 42, :product 2, :quantity 1}}
```

spec: `[:expand & expansion]`
  
### Set `:union`

spec: `[:union relvar]`
  
### Set `:difference `

spec: `[:difference relvar]`
  
### Set `:intersection`

spec: `[:intersection relvar]`
  
### Rename columns with `:rename`

spec: ` [:rename {col new-col, ...}]`
  
### Qualify columns with `:qualify`

spec: `[:qualify namespace]`

### Constant relations with `:const`

spec: `[:const coll]`

### Using indexes

#### `:where`

expressions can be maps, rows are matched by testing `[= k v]`. `v` must be a constant value. if the first expression is a map then an index will be consulted.
e.g `[:where {:a 42, :b 42} ...]` will consult an index to discover rows where :a is 42 and :b is 42.

This is mostly useful in updates/deletes and ad-hoc queries as joins always use indexes.

## Expression reference

Relic expressions are the means by which computation on columns is performed. They ultimately compile to functions of rows, and in fact a function is a valid
relic expression.

They are kind of like clojure s-expressions, but have some implicit evaluation rules that makes programming relic more ergonomic in the common case.

For example, consider the relic expression

`[+ :a 42]`

If you squint a bit you can see the same as a clojure call:

`(+ (:a row) 42)`

Differences from clojure s-expressions

- vectors are instead of lists to delay evaluation, without needing quote/unquote.
- keywords are substituted with a lookup against the row, e.g `:a` become `(:a row)`.
- ultimately all relic expressions are tested against a row, but they can contain constants, and constant expressions that 
  do not depend on the row, such `[+ 1 2]`.
  
Conditional special forms are available due to inability to use clojure macros in the call position.

 - `[:and & expr]`
 - `[:or & expr]`
 - `[:if then else]`

### Escaping keywords `::rel/esc`

Because normally keywords are substituted with a lookup, an expression like `[= :a :b]` will be evaluated as `(= (:a row) (:b row))`

If you do not want this behaviour you can escape with the `::rel/esc` special form. e.g `[= :a [::rel/esc :b]]` would result in `(= (:a row) :b)`

### Using non-keyword keys with `::rel/get`

Ergonomic relic programming kind of wants your keys to be keywords but if you want to start with say, json with string keys - You can do so.

Use `::rel/get` as a shorthand for a `get` call. e.g `[::rel/get "firstname"]` would result in `(get row "firstname")`

### Getting a reference to the row with `::rel/%`

If you want the whole map you can reference it with `::rel/%` so you could for example call `[pos? [count ::rel/%]]` would be a valid relic expression.

### Sampling the environment with `::rel/env`

A special form `::rel/env` is provided for referencing the 'environment', the 'environment' is a relvar with special support in relic, its a good place to deal
with things like time, parameters, and configuration for your relic database.

e.g `[::rel/env :now-utc]`

See [env](#use-the-environment-in-relvars-with-relenv)

## Aggregate reference

The [`:agg`](#aggregation--grouping-with-agg) relational operator takes [extension](#adding-new-columns-with-extend) like bindings to compute aggregate expressions over grouped rows.

They look like relic [expressions](#expression-reference) but do in fact follow different rules. 

The simplest thing to do is to use the built-in aggregations in relic.

### Count grouped rows with `clojure.core/count`

Count is easy, just use normal `count`! The library function from clojure itself is a valid aggregate.

e.g `[:agg [] [:n count]]` 

### Find rows with the smallest/highest of something with `greatest-by` / `least-by` 

e.g `[:agg [] [:min-row-by-a [rel/least-by :a]]]` would bind the row with the lowest :a.

Good for find-first, find-last type use cases where you want to preserve the row.

spec: `[greatest-by|least-by expr]`

### Find the smallest or highest result of an expression with `greatest` / `least` 

e.g `[:agg [] [:min-a [rel/least :a]]]` would bind the lowest `:a`.

### Sum numbers with `sum`

e.g `[:agg [] [:n [rel/sum :a]` would bind the sum of all `:a` over the grouped rows.

spec: `[sum expr]`

### Get the set of all values for an expression with `set-concat`

e.g `[:agg [] [:a-set [rel/set-concat :a]]]` would bind the set of all distinct values of `:a`

### Count the number of distinct values for an expression with `count-distinct`

e.g `[:agg [] [:a-set [rel/set-concat :a]]]` would bind the count of all distinct values of `:a`

### Test if any row meets some predicate (or not) with `any` / `not-any`

These are your like for `some` and `not-any?` in clojure.

e.g `[:agg [] [rel/any :b]]` would bind true if any `:b` is true

spec: `[any/not-any expr]`

### Find lowest/greatest rows by some expression with `top-by` / `bottom-by`

e.g `[:agg [] [rel/top-by 5 :a]]` would bind a vector of 5 rows with the highest values for `:a`

spec: `[top-by/bottom-by n expr]`

### Find lowest/greatest values of some expression with `top` / `bottom`

e.g `[:agg [] [rel/top-by 5 :a]]` would bind a vector of the 5 highest values of` :a`

spec: `[top/bottom n expr]`

## Query reference

### Get rows with `q`
### Apply a sort with `:sort` and `:rsort` 
### Use a transducer with `:xf`

## Transact reference 

### Modify state with `transact`

You modify relic databases with transact, in relic we try to only modify the essential state of the program, so you 
can only modify tables.

The function `transact` takes various instructions for tables to change as args.

```clojure 
(rel/transact db 
  [:insert :Customer {:firstname "Fred", ...}, {:firstname "Alice", ...}]
  [:delete Order [not :shipping]])
```

See below for what you can do.

### Insert one or more rows `:insert`

To add rows use `:insert`. Duplicate rows are just discarded.

spec: `[:insert table & row]`

### Terse insert `{table1 rows, table2 rows}`

If you want to add to multiple tables with little ceremony, you can use a map of table to coll of rows instead.

### Update rows with `:update`

spec: `[:update table set-map-or-f & where-expr]`

### Delete rows by filter with `:delete`

spec: `[:delete table & where-expr]`

### Delete rows with `:delete-exact`

spec: `[:delete-exact table & row]`

### Insert-or-update with `:upsert`

spec: `[:upsert table & row]`

## Materialization reference 

### materialize relations with `materialize`
### save memory with `dematerialize`

## Constraint reference 

Constraints are just relvars ending in one of the constraint statements [`:unique`](#ensure-only-one-row-exists-for-a-set-of-columns-unique),
[`:fk`](#ensure-a-referenced-row-exists-fk) and [`:check`](#check-columns-or-rows-always-meet-some-predicate-check). 

To constrain a database, you `materialize` constraint relvars (and they can be removed with `dematerialize`). 

Constraints can apply to _any_ relvar, so you can apply constraints to derived relvars and joins, here
is the `order can have at most 10 items if its associated customer is called bob and its tuesday` constraint we talked about before:

```clojure 
[[:from Order] 
 [:join Customer {:customer-id :customer-id}]
 [:where [= "bob" :firstname] [= "tuesday" [uk-day-of-week [::rel/env :now]]]]
 [:check {:pred [<= [count :items] 10],
          :error [str "order can have at most 10 items if its associated customer is called bob and its tuesday, found: " [count :items]]}]]
```

As it is convenient to specify multiple constraints on a relvar in one form, a special
`:constrain` statement is provided.

e.g

```clojure 
[[:from Customer]
 [:constrain
   [:check [string? :firstname] [string? :lastname] [nat-int? :age]]
   [:unique :customer-id]
   [:fk Address {:address-id :address-id}]]]
```

### Ensure only one row exists for a combination of columns `:unique`

spec: `[:unique & expr]`

### Ensure a referenced row exists with `:fk`

spec: `[:fk relvar clause opts]`

Options:

- `:cascade` if true will cause rows referencing deleted/modified rows such as the constraint is violated to themselves be deleted.

### Test predicates against columns and rows using `:check`

Tests predicate expressions against rows and ensures they all return true. A map can be provided
to specialise error messages. e.g

```clojure
[string? :firstname]
```

or

```clojure 
{:pred [string? :firstname], :error "firstname must be a string"}
```

`:error` is also a relic expression that returns a string e.g

```clojure 
{:pred [string? :firstname], :error [str "firstname must be a string, found: " :firstname]}
``` 

spec: `[:check & pred-expr|pred-map]`

Yes, relic will integrate with spec1/2 & malli. Give me time.

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