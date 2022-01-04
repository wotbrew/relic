(ns com.wotbrew.relic.impl.relvar)

(defn to-relvar [relvar-or-table]
  (if (keyword? relvar-or-table)
    [[:table relvar-or-table]]
    relvar-or-table))

(defn operator [stmt] (nth stmt 0))

(defn left [relvar] (when (peek relvar) (subvec relvar 0 (dec (count relvar)))))

(defn head [relvar] (peek relvar))

(defn table-relvar?
  "True if the relvar is a table."
  [relvar]
  (case (count relvar)
    1 (= :table (operator (head relvar)))
    false))

(defn unwrap-from [relvar]
  (if (= :from (operator (head relvar)))
    (let [[_ relvar] (head relvar)]
      (unwrap-from (to-relvar relvar)))
    relvar))

(defn unwrap-table
  "If the relvar is a table (or :from chain containing a table) then unwrap the underlying table relvar and return it."
  [relvar]
  (if (table-relvar? relvar)
    relvar
    (let [relvar (unwrap-from relvar)]
      (when (table-relvar? relvar)
        relvar))))

(defn unwrap-table-key [relvar]
  (let [[_ table-key] (some-> (unwrap-table relvar) head)]
    table-key))

(defn head-operator [relvar]
  (when (vector? relvar)
    (some-> relvar head operator)))