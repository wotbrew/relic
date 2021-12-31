# Security

Relic goes out of its way to be resistant to injection attacks with its design.

As relic's support of clojure functions allows pretty arbitrary evaluation as part of a query, e.g 
`[sh/sh "rm" "-rf" "/"]` we need to mindful of security when processing untrusted user input.

## Protection offered by relic by default

relic is careful about what is allowed in the prefix function position of relic expressions (a potential vector for injection)

Relic allows:

- function objects (not symbols!), 
- whitelisted sentinel values (that could not normally be constructed with `edn/read`)
- safe keywords sentinels like `:_` that cannot be used for exfiltration / eval, 
- column keywords

Out of these column keywords are the one to be worried about, although they do not provide
opportunities for RCE, you should be wary of expressions like this `[:extend [:my-col untrusted]]` as they could be used to exfiltrate any data on the row.
Now I do not think in practice users will do this

I would advise in general to not allow user input to create arbitrary keywords as regardless of vectors against relic, it could be used
to pollute the keyword cache and cause a DOS by increasing memory pressure / weak reference collection time.

## Escaping 

If you want to be absolutely sure, the `:_` form can be used in expressions to ensure
that the form is never interpreted and is treated as a normal value, even if a vector with a function in prefix position.

e.g `[:_ [sh/sh "echo" "foo"]]` would just bind the value of `[sh/sh "echo" "foo"]` without evaluating `sh/sh`.