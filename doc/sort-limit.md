# :sort-limit

Like [`:sort`](sort.md) but takes a limit param, for asking a query like the last 1000 events by timestamp.

```clojure 
[[:from :Event]
 [:sort-limit 1000 [:ts :desc] [:event-type :asc]]]
```