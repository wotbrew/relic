# relic

`STATUS: EXPERIMENTAL`

A Clojure(Script) library for doing functional relational programming in clojure.

- SQL-style relational programming
- declarative 
- data dsl
- incremental materialized views
- decent performance

## Why 

Do you ever feel like this when programming with business data and pipelines of maps with maps and more maps, maps for 
breakfast, dinner and supper?

![tarpit](doc/tar.jpeg)

`relic` might help.

## Tutorial

This is a _relvar_, a _relvar_ is just a vector containing a series of relational statements.
``` clojure
[[:table :Customer]]
```

You derive new relvars by appending statements to the vector, statements are relational operators, like SQL.

```clojure 
 [[:table :Customer]
  [:where [= :id 42]]]
 ```

Relvars on their own don't do anything, to get a _relation_ we have to pair them with some state.

State is stored in a plain old clojure map which we'll call the `db` in this example, you manipulate the state with the `transact` function.

```clojure 
(require '[com.wotbrew.relic :as r])


(def db (r/transact {} [:insert [[:table :Customer]] {:id 42, :name "bob"} {:id 43, :name "alice"}])

db 
;; =>
{[[:table :Customer]] #{{:id 42, :name "bob"}, {:id 43, :name "alice"}}}

```

Now we have our state, we can ask questions of relic to find _relations_, as you would a SQL database.

```clojure 
(r/q db [[:table :Customer] [:where [= :id 42]]]) 
;; => 
#{{:id 42, :name "bob"}}

(r/q db [[:table :Customer] [:where [= :name "alice"]]])
;; => 
#{{:id 43, :name "alice"}}
```

Ok ok, not very cool. _What if I told you_ that you can materialize any relvar such that 
it will be maintained for you as you modify the database. In other words `relic` has incremental materialized views.

```clojure 
(r/materialize db [[:table :Customer] [:where [= :id 42]]])
```

`materialize` will return a new _database_ that looks and smells the same, but will maintain the results
of the passed relvars as you `transact` against the database __incrementally__.

If you read the tarpit paper, you might find this [real estate example](https://github.com/wotbrew/relic/blob/master/dev/examples/real_estate.clj) informative.

## Relational operators

### `[:table ?name]` 

The base state relvar, at the moment just a name e.g [:table :Customer]

### `[:from ?relvar]`

Provides some convenience for deriving relvars without conj, e.g `[[:from A] ...]`  

### `[:where & ?exprs]` 

Restricts results to those matching some set of expressions, expressions are vectors where keywords are templated in as cols
e.g `[= :age 42]` the first value needs to be a function, rows will be tested using a form like `(= (:age row) 42)`

#### Using indexes

expressions can be maps, rows are matched by testing `[= k v]`. `v` must be a constant value. if the first expression is a map then an index will be consulted.
e.g `[:where {:a 42, :b 42} ...]` will consult an index to discover rows where :a is 42 and :b is 42.

This is mostly useful in updates/deletes and ad-hoc queries as joins always use indexes.
  
### `[:extend & ?extensions]`

Extension adds new values to rows, an extension looks like this: 

`[:foo [str :a :b]]`

In this case it says provide the column `:foo` by concating `:a` and `:b` with `str`. 

#### Extension forms:

- `[k expr]` bind result of expr to k
- `[[& k] expr]` merge keys from the result of expr

You can use the functions r/join-first and r/join-coll in expression position for sub-selects.

e.g 

```clojure
(def FooSum [[:from Foo] [:agg [:foo] [:n-sum [r/sum :n]]]])
[:extend [[:n-sum] (r/join-first FooSum {:foo :foo})]]
```


### `[:project & keys]`

Like `select-keys` or `set/project` the resulting relation will contain only the projected keys.
  
### `[:project-away & keys]`  

Inverse of `:project` will instead omit the keys in `keys` from the resulting relation.

### `[:select & ?binding|key]`

A mix and match expressions suitable for `:project` and `:extend` resulting in a SQL-style `:select`.
  
### `[:join ?relvar ?clause]` 

Like `set/join`, clause is a map of keys on the left to keys on the right.
  
### `[:left-join ?relvar ?clause]`

SQL-style left join.
  
### `[:agg [& ?keys] & aggregations]`

The aggregation / grouping operator.  Groups rows under the keys and performs aggregation on them to
construct new values. e.g `[:agg [:a] [:summed-b [r/sum :b]]]` WIP.
  
### `[:expand & ?expansion]`
  
### `[:union ?relvar]`
  
### `[:difference ?relvar]`
  
### `[:intersection ?relvar]`
  
### `[:rename ?renames]`
  
### `[:qualify ?namespace]`

## Transact forms 

### `[:insert ?relvar & ?row]`
### `[:delete ?relvar & ?row]`
### `{?relvar ?rows}`
### `TODO update`
### `TODO upsert`

## TODO 

- Recursive (fixed point?) relvars to enable logic type queries
- Tests
- Docs  
- Constraints
- Update / Upsert
- Compile time relvars, the great unboxing, custom record types
- Better docs 
- Tooling, spec  
- Uhhhh... Bigger datasets than memory

## Related work

- [core.logic](https://github.com/clojure/core.logic) 
- [datascript](https://github.com/tonsky/datascript)
- [clara-rules](https://github.com/cerner/clara-rules)
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