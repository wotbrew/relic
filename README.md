# relic

![tests](https://github.com/wotbrew/relic/actions/workflows/tests.yml/badge.svg)

_status: alpha, breaking changes unlikely but possible_

```clojure 
[com.wotbrew/relic "0.1.4"]
```

`relic` is an experimental Clojure/Script in-memory database and data processing library.

Built in a lockdown induced _strange mood_, the goal is a clojure flavoured version of the functional relational programming model described by the [tar pit](http://curtclifton.net/papers/MoseleyMarks06a.pdf) paper.

## Why

When working on boring $business_problems, too much energy is spent on the machine like bits of programs,
data structures, threading the data to where it is needed, layout, ordering, access path etc.

I would prefer to focus more on the data itself, and the relationships between the data.

Industrial SQL databases seem to solve machine like problems by providing planners and optimisers, and ultimately making decisions on behalf of the programmer such that the programmer can get their work done and go home at 5pm.

`relic` can take care of many of the machine like bits of data query & processing, just as databases do.

- Fully featured in-memory database with indexed SQL style [query](https://wotbrew.github.io/relic/query).
- Integrated and embedded in clojure, use clojure [functions](https://wotbrew.github.io/relic/expr) in queries, build queries with clojure.
- [Materialized views](https://wotbrew.github.io/relic/materialization) with incremental maintenance.
- Make invalid states illegal with [constraints](https://wotbrew.github.io/relic/constraints).
- [Reactive](https://wotbrew.github.io/relic/change-tracking), allowing efficient integration with react, bind components to materialized queries and remain responsive at 60fps.

relic is about in memory computation, for this kind of thing at scale consider: [materialize](https://github.com/MaterializeInc/materialize).

## Documentation

See [documentation](https://wotbrew.github.io/relic) for a detailed reference.

## Pitch

Do you suffer from _map fatigue_? [[1]](http://timothypratley.blogspot.com/2019/01/meander-answer-to-map-fatigue.html)

![despair](doc/tar2.png)

Did you try [meander](https://github.com/noprompt/meander), [core.logic](https://github.com/clojure/core.logic), [datascript](https://github.com/tonsky/datascript) and every graph-map-database under the sun but still do not feel [out of the tar pit](http://curtclifton.net/papers/MoseleyMarks06a.pdf)?

![in the tar pit](doc/tar.jpeg)

Are you tired of writing mechanical wiring and glue? That has *nothing* to do with your actual business logic?

`relic` might help, but it's not a medical professional. It's a functional relational programming library.

Definitely not at all like the other in-memory databases in clojure. This time it is different, really.

## Brief tour

I like to use `rel` as an alias.

```clojure
(require '[com.wotbrew.relic :as rel])
```

This is a query, lets find some bob's.

```clojure 
 [[:from :Customer]
  [:where [= :name "bob"]]]
 ```

You can refine queries by just adding elements to the vector.  All your favorites are here, filtering (`:where`), computing columns (`:extend`), joins (`:join` & `:left-join`), grouping and aggregation (`:agg`) and more.

```clojure 
[[:from :Customer]
 [:where [= :name "bob"]]
 [:extend [:greeting [str "Hello, " :name "!"]]]
```

Because queries are just vectors, they just sort of lounge around being values. To put them to work we have to feed some data into relic.

`relic` databases are boring clojure maps. Prepare yourself:

```clojure 
(def db {})
```

See, boring.

You manipulate your database with the [`transact`](https://wotbrew.github.io/relic/transact) function. This returns a new database with the transaction applied. Plain old functional programming, no surprises.
```clojure 
(def db (rel/transact {} [:insert :Customer {:name "bob"} {:name "alice"}])

db 
;; =>
{:Customer #{{:name "bob"}, {:name "alice"}}}
```

Now we have some state, we can ask questions of relic, as you would a SQL database.

```clojure 
(rel/q db :Customer)
;; => 
({:name "bob"}, {:name "alice"})

(rel/q db [[:from :Customer] [:where [= :name "bob"]]]) 
;; => 
({:name "bob"})

(rel/q db [[:from :Customer] [:agg [] [:avg-name-len [rel/avg [count :name]]]]])
;; => 
({:avg-name-len 4})
```

Ok ok, neat but not _cool_.

Ahhhh... but you don't understand, `relic` doesn't just evaluate queries like some kind of cave man technology - it is powered by a __data flow graph__.
`relic` knows when your data changes, and it knows how to modify dependent relations in a smart way.

You can materialize any query such that it will be maintained for you as you modify the database. In other words `relic` has __incremental materialized views__.

```clojure 
(rel/mat db [[:from :Customer] [:where [= :name "bob"]]])
;; => returns the database, its value will be the same (hint: metadata).
{:Customer #{{:name "bob"}, {:name "alice"}}}
```

`mat` will return a new _database_, against which materialized queries will be instant, and __as you change data in tables, those changes will flow to materialized queries automatically.__

You can do more than query and materialize with relic, you can [react to changes](https://wotbrew.github.io/relic/change-tracking), use [constraints](https://wotbrew.github.io/relic/constraints) and [more](https://wotbrew.github.io/relic).

If you read the tarpit paper, you might find this [real estate example](https://github.com/wotbrew/relic/blob/master/dev/examples/real_estate.clj) informative.

Another example demonstrating usage in the browser can be found in [cljidle](https://github.com/wotbrew/relic/blob/master/dev/examples/cljidle).

For further reading, see the [docs](https://wotbrew.github.io/relic)

## Related work

- [core.logic](https://github.com/clojure/core.logic) 
- [datascript](https://github.com/tonsky/datascript)
- [clara-rules](https://github.com/cernerel/clara-rules)
- [odoyle-rules](https://github.com/oakes/odoyle-rules)
- [fyra](https://github.com/yanatan16/fyra)
- [meander](https://github.com/noprompt/meander)
- [doxa](https://github.com/ribelo/doxa)
- [clj-3df](https://github.com/sixthnormal/clj-3df)

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

Copyright 2022 Dan Stone (wotbrew)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.