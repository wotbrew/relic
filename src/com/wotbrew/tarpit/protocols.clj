(ns com.wotbrew.tarpit.protocols)

(defprotocol RelId
  (rel-id [rel]))

(defprotocol State
  (get-schema [st])
  (rows [st rel]))