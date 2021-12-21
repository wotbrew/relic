# `:select`

The projection statement, takes columns & [extension](extend.md) style bindings and returns only them.

## Form 

```clojure 
select = [:select & col-or-extension]
col-or-binding = col | [binding expr]
```

## Examples

```clojure 
[[:from :Customer]
 [:select :firstname :lastname [:fullname [str :firstname " " :lastname]]]]
 ```