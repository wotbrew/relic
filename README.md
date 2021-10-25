# relic

`STATUS: EXPERIMENTAL`

A Clojure(Script) library for doing information programming using 'codd style' relational algebra with materialized views for fun.

## WTF is this?

This is a _relvar_, a _relvar_ is just a vector containing a series of relational statements.
``` clojure
[[:state :Customer]]
```

You derive new relvars by appending statements to the vector, there is a whole algebra of statements to choose from, like SQL.

```clojure 
 [[:state :Customer]
  [:where [= :id 42]]]
 ```

Relvars on their own don't do anything, to get a _relation_ we have to pair them with some state.

State is stored in a plain old clojure map which we'll call the `db` in this example, you manipulate the state with the `transact` function.

```clojure 
(require '[com.wotbrew.relic :as r])


(def db (r/transact {} [:insert [[:state :Customer]] {:id 42, :name "bob"} {:id 43, :name "alice"}])

db 
;; =>
{[[:state :Customer]] #{{:id 42, :name "bob}, {:id 43, :name "alice"}}}

```

Now we have our state, we can ask questions of relic to find _relations_, as you would a SQL database.

```clojure 
(r/relation db [[:state :Customer] [:where [= :id 42]]]) 
;; => 
#{{:id 42, :name "bob}}

(r/relation db [[:state :Customer] [:where [= :name "alice"]]])
;; => 
#{{:id 43, :name "alice"}}
```

Ok ok, not very cool. _What if I told you_ that you can materialize any relvar such that 
it will be maintained for you as you modify the database. In other words `relic` has materialized views.

```clojure 
(r/materialize st [[:state :Customer] [:where [= :id 42]]])
```

`materialize` will return a new _database_ that looks and smells the same, but will maintain the results
of the passed relvars as you `transact` against the database __incrementally__.

## Relational operators

### `[:state ?name]` 

The base state relvar, at the moment just a name e.g [:state :Customer]

### `[:from ?relvar]`

Provides some convenience for deriving relvars without conj, e.g `[[:from A] ...]`  

### `[:where & ?exprs]` 

Restricts results to those matching some set of expressions, expressions are vectors where keywords are templated in as cols
e.g `[= :age 42]` the first value needs to be a function, rows will be tested using a form like `(= (:age row) 42)`
  
### `[:extend & ?extensions]`

Extension adds new values to rows, an extension looks like this: 

`[:foo :<- [str :a :b]]`

In this case it says provide the column `:foo` by concating `:a` and `:b` with `str`. 

You can do more with extend but thats all I'm willing to document right now :)

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
construct new values. e.g `[:agg [:a] [:summed-b :<- [r/sum :b]]]` WIP.
  
### `[:expand & ?expansion]`
  
### `[:union ?relvar]`
  
### `[:difference ?relvar]`
  
### `[:intersection ?relvar]`
  
### `[:rename ?renames]`
  
### `[:qualify ?namespace]`

## Transact forms 

### `[:insert ?relvar & ?row]`
### `[:delete ?relvar & ?row`
### `{?relvar ?rows}`

## TODO 

- Tests please
- Docs  
- Constraints
- Update / Upsert
- Compile time relvars  
- Improve perf
- Better docs 
- Tooling, spec

## Related work

- [core.logic](https://github.com/clojure/core.logic) 
- [datascript](https://github.com/tonsky/datascript)
- [clara rules](https://github.com/cerner/clara-rules)
- [o-doyle rules](https://github.com/oakes/odoyle-rules)

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