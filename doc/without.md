# :without

Removes columns, e.g like `dissoc`.

## Form 

```clojure 
[:without & col]
```

## Examples

```clojure 
[[:from :Customer]
 [:without :age :firstname :lastname]]
```