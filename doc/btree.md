# :btree

The `:btree` statement creates a sorted index, it is similar to [`:hash`](hash.md) apart from the resulting nested-map is sorted instead of hash-ordered.

Like other [index](indexes.md) statements, this is considered a tuning parameter and isn't necessary for relic to work, typically
you would only materialize these to enable faster query, or index access to sorted indexes.

`:btree` indexes can enable range query optimisations, such as seqing from a key in a particular order.

Here is an example of indexing events by timestamp.

```clojure 

;; RELVAR
;; you index expressions, here I am going to use a single expression,
;; but I could use multiple and get a nested map, e.g [:btree :a, :b, :c]
[[:from :Event] 
 [:btree :ts]]
 
;; STATE
{:Event [{:ts 0, :msg "hello"}
         {:ts 1, :msg ", world"}
         {:ts 2, :msg "!"}]}
         
;; INDEX
;; a sorted map, that supports subseq and rsubseq
;; get with rel/index
{0 #{{:ts 0, :msg "hello"}}
 1 #{{:ts 1, :msg ", world"}}
 2 #{{:ts 2, :msg "!"}
```