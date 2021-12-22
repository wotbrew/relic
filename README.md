# relic

`STATUS: PRIMORDIAL TAR, ready soon...`

`relic` is a Clojure/Script in-memory database and data processing library, inspired by Codd's relational algebra. It is immutable, functional and declarative.

As well as answering ad-hoc queries with a range of relational operators, 
it supports incremental materialization, and constraints on arbitrary views.

`relic` aims to compete with or exceed the performance of other traditional clojure in-memory databases, and materialization extends its reach to areas
where any kind of query at all is too slow.

```clojure 
(rel/q db [[:from :Library]
           [:where [contains? :lib/tags "relational"] [str/starts-with? :lib/name "rel"]]
           [:join :Author {:lib/author :author/id}]
           [:select
             :lib/name
             :author/github
             [:url [str "https://github.com/" :author/github "/" :lib/name]]]])
;; =>
#{{:lib/name "relic", :author/github "wotbrew", :url "https://github.com/wotbrew/relic"}}
```

## Documentation

See [documentation](https://wotbrew.github.io/relic) for a detailed reference.

## Who is it for?

- you want declarative query of your data, but are more familiar with SQL than datalog
- you want spreadsheet style incremental computation over large (or small!) amounts of data
- you work with result sets from SQL databases, and want to compute further in memory without leaving the relational information model
- you work with other tabular data, sequences of map records, csv files etc
- your data fits in memory (for now!)

## How does it work?

- relvars are transformed into a directed-acyclic-graph of data flow nodes, sharing structure with other relvars (e.g indexes), to avoid redundant computation.
- data flows from realised nodes to unrealised nodes, initialising them.
- as you change data in tables, nodes in the DAG receive new inserted/deleted signals, and data flows along the graph.
- the difference between a query and materialized view is just whether the graph nodes are saved or thrown away.
- unlike some other reactive signal graphs, the relic dataflow graph uses indexes and set-based flow, so only the rows that have changed are flowed, not the entire result

## Pitch 

Do you suffer from _map fatigue_? [[1]](http://timothypratley.blogspot.com/2019/01/meander-answer-to-map-fatigue.html)

![despair](doc/tar2.png)

Did you try [meander](https://github.com/noprompt/meander), [core.logic](https://github.com/clojure/core.logic), [datascript](https://github.com/tonsky/datascript) and every graph-map-database under the sun but still do not feel [out of the tar pit](http://curtclifton.net/papers/MoseleyMarks06a.pdf)?

![in the tar pit](doc/tar.jpeg)

Are you spending too much time writing mechanical wiring and glue? That has *nothing* to do with your actual business logic?

`relic` might help, but it's not a medical professional. It's a functional relational programming library.

- allows you to focus on the essential (minimal state, relationship expression) rather than incidental (accidental data, mechanisms, and ad-hoc structure).
- like SQL for clojure data, but _actually composes_.
- munge with joy via the glorious relational open access information model.
- laugh at cache invalidation problems with __incremental materialized views__ and dataflow sorcery.
- relational expressions as __data__, open to introspection and analysis. Gives static tools a fighting chance.
- __constraints__ a-la-carte, gain confidence. I'm not talking just types, say things like [order can have at most 10 items if its associated customer is called bob and its tuesday](#constraint-reference).

Definitely not at all like the other in-memory databases in clojure. this time its different, really.

## Brief tour

See [docs](docs)

I like to use `rel` as an alias.

```clojure
(require '[com.wotbrew.relic :as rel])
```

This is a _relvar_, Think SQL view/tables/queries all as one idea, a relational expression.

```clojure 
 [[:from :Customer]
  [:where [= :id 42]]]
 ```

You can derive relvars from relvars by just adding elements to the vector.  All your favorites are here, filtering (`:where`), computing columns (`:extend`), joins (`:join` & `:left-join`), grouping and aggregation (`:agg`) and more.

```clojure 
[[:from :Customer]
 [:where [= :id 42]]
 [:extend [:fullname [str :firstname " " :lastname]]]
```

Because relvars are just vectors, they just sort of lounge around being values. To put them to work to get a _relation_ we have to feed some data into our tables.

`relic` databases are boring clojure maps. Prepare yourself:

```clojure 
(def db {})
```

See, boring.

You manipulate data in tables with the [`transact`](#transact-reference) function. This just returns a new database. Plain old functional programming, no surprises.
```clojure 
(def db (rel/transact {} [:insert :Customer {:id 42, :name "bob"} {:id 43, :name "alice"}])

db 
;; =>
{:Customer #{{:id 42, :name "bob"}, {:id 43, :name "alice"}}}

```

Now we have our state, we can ask questions of relic to find _relations_, as you would a SQL database. You see how relvars and queries _are the same_.

```clojure 
(rel/q db [[:from :Customer] [:where [= :id 42]]]) 
;; => 
#{{:id 42, :name "bob"}}

(rel/q db [[:from :Customer] [:where [= :name "alice"]]])
;; => 
#{{:id 43, :name "alice"}}
```

Ok ok, neat but not _cool_.

Ahhhh... but you don't understand, `relic` doesn't just evaluate queries like some kind of cave man technology - it is powered by a __data flow graph__.
`relic` knows when your data changes, and it knows how to modify dependent relations in a smart way.

```clojure 
(rel/materialize db [[:from :Customer] [:where [= :id 42]]])
;; => returns the database, its value will be the same (hint: metadata).
{:Customer #{{:id 42, :name "bob"}, {:id 43, :name "alice"}}}
```
You can materialize any relvar such that it will be maintained for you as you modify the database. In other words `relic` has __incremental materialized views__.

`materialize` will return a new _database_ that looks and smells the same, but queries against materialized relvars will be instant, and __as you change data in tables, those changes will flow to materialized relvars automatically.__

relic attempts to deliver on the promise of separating out essential data and essential computation from accidental. One way it does this is the materialization, the data flow graph is not part of the value domain. It sits in metadata where it belongs. Your databases value is just your state - no machinery.

You can do more than query and materialize with relic, you can [react to changes](https://wotbrew.github.io/relic/change-tracking), use [constraints](https://wotbrew.github.io/relic/constraints) and [more](https://wotbrew.github.io/relic).

If you read the tarpit paper, you might find this [real estate example](https://github.com/wotbrew/relic/blob/master/dev/examples/real_estate.clj) informative.

For further reading, see the [docs](https://wotbrew.github.io/relic)

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