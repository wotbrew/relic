(ns com.wotbrew.tarpit.protocols)

(defprotocol RelId
  (rel-id [rel]))

(defprotocol State
  (get-schema [st])
  (rows [st rel])
  (q [st rel])
  (modify [st mods])
  (check [st constraint]))