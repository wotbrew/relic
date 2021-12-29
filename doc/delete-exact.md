# :delete-exact

Delete exact is useful when you know the exact values of the rows you want to delete, so conditions are kind of pointless.

```clojure 
[:delete-exact :Customer customer-row] 
;; is the same as below, but faster, and saves some typing.
[:delete :Customer [= :% [:_ customer-row]]] 
```

See also [:delete](delete.md).