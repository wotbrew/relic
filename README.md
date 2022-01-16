# relic

![tests](https://github.com/wotbrew/relic/actions/workflows/tests.yml/badge.svg)

`STATUS: Early alpha, will eat your homework, set laptop on fire, use at own risk`

```clojure 
[com.wotbrew/relic "0.1.0"]
```

`relic` is a Clojure/Script in-memory database and data processing library, inspired by Codd's relational algebra.

It has materialized views (with incremental maintenance), change tracking, constraints and SQL-like query DSL.

It is an attempt to deliver in memory the programming model described by the [tar pit](http://curtclifton.net/papers/MoseleyMarks06a.pdf) paper.


```clojure 
(rel/q db [[:from :Library]
           [:where [contains? :lib/tags "relational"] [str/starts-with? :lib/name "rel"]]
           [:join :Author {:lib/author :author/id}]
           [:select
             :lib/name
             :author/github
             [:url [str "https://github.com/" :author/github "/" :lib/name]]]])
;; =>
({:lib/name "relic", :author/github "wotbrew", :url "https://github.com/wotbrew/relic"})
```

## Documentation

See [documentation](https://wotbrew.github.io/relic) for a detailed reference.

## Pitch 

Do you suffer from _map fatigue_? [[1]](http://timothypratley.blogspot.com/2019/01/meander-answer-to-map-fatigue.html)

![despair](doc/tar2.png)

Did you try [meander](https://github.com/noprompt/meander), [core.logic](https://github.com/clojure/core.logic), [datascript](https://github.com/tonsky/datascript) and every graph-map-database under the sun but still do not feel [out of the tar pit](http://curtclifton.net/papers/MoseleyMarks06a.pdf)?

![in the tar pit](doc/tar.jpeg)

Are you tired of writing mechanical wiring and glue? That has *nothing* to do with your actual business logic?

`relic` might help, but it's not a medical professional. It's a functional relational programming library.

- like SQL for clojure data.
- join with joy with the glorious relational open access information model.
- laugh at cache invalidation problems with __incremental materialized views__ and dataflow sorcery.
- __constraints__ a-la-carte, gain confidence. I'm not talking just types, say things like [order can have at most 10 items if its associated customer is called bob and its tuesday](https://wotbrew.github.io/relic/constraints).

Definitely not at all like the other in-memory databases in clojure. this time its different, really.

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
(rel/q db [[:from :Customer] [:where [= :name "bob"]]]) 
;; => 
#{{:name "bob"}}

(rel/q db [[:from :Customer] [:where [= :name "alice"]]])
;; => 
#{{:name "alice"}}
```

Ok ok, neat but not _cool_.

Ahhhh... but you don't understand, `relic` doesn't just evaluate queries like some kind of cave man technology - it is powered by a __data flow graph__.
`relic` knows when your data changes, and it knows how to modify dependent relations in a smart way.

You can materialize any query such that it will be maintained for you as you modify the database. In other words `relic` has __incremental materialized views__.

```clojure 
(rel/materialize db [[:from :Customer] [:where [= :name "bob"]]])
;; => returns the database, its value will be the same (hint: metadata).
{:Customer #{{:name "bob"}, {:name "alice"}}}
```

`materialize` will return a new _database_, against which materialized queries will be instant, and __as you change data in tables, those changes will flow to materialized queries automatically.__

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

Copyright 2022 Dan Stone (wotbrew)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.