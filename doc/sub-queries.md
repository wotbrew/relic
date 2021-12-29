# Sub queries

You can issue sub queries in relic by using the special forms `:$` (select all) and `:$1` (select first) in relic
[expressions](expr.md).

Both take a table (or query) and a join clause as arguments.

In the below example we use sub queries to select all OrderItems for a given Order.

```clojure 
;; QUERY 
[[:from :Order]
 [:select [:items [:$ :OrderItem {:order-id :order-id}]]]]
 
;; STATE
{:Order #{{:order-id 0}}
 :OrderItem #{{:order-id 0, :product "eggs"}, {:order-id 1, :product "bread"}}}
 
;; RESULTS
({:order-id 0, :items #{{:order-id 0, :product "eggs"}, {:order-id 1, :product "bread"}}})
```

You can use sub queries in queries anywhere expressions are expected, for example - here we will use `:$1` for a sql-style exists check.

```clojure 
(def CustomersWithOrders
  [[:from :Customer] 
   [:where [:$1 :Order {:customer-id :customer-id}]]])
```

Sub queries can nest and multiple sub queries can be issued per operation, but the columns necessary to satisfy the joins must exist at the point the sub query is defined.

## Under the hood

Sub queries are converted into implicit join dependencies, you do not actually issue a query for every single evaluation of an expression. They use indexes like joins do.