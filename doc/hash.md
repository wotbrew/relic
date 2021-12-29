# :hash

The `:hash` operation creates a hash index.

Like other [index](indexes.md) operations this is considered a tuning parameter and isn't necessary for relic to work, typically
you would only materialize these to enable faster query in advanced situations, or index access.

Hash indexes can enable faster equality checks.

They are used by default for `:join` and `:left-join` implicit indexes, you should not need to create them yourself.

Here is an example of indexing customers by name.

```clojure 
;; QUERY
;; you index expressions, here I am going to use a single expression,
;; but I could use multiple and get a nested map, e.g [:hash :a, :b, :c]
[[:from :Customer] 
 [:hash :name]]
 
;; STATE
{:Customer [{:name "bob", :age 42}
            {:name "alice", :age 23}]}
         
;; INDEX
;; a clojure map
;; get with rel/index
{"bob" #{{:name "bob", :age 42}}
 "alice" #{{:name "alice", :age 23}}}
```