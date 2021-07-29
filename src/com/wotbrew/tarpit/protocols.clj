(ns com.wotbrew.tarpit.protocols)

(defprotocol State
  (get-schema [st])
  (materialized-row-set [st rel])
  (add-rows [st rel rows])
  (del-rows [st rel rows]))

(defprotocol Table
  (table-add [table rows])
  (table-delete [table rows]))

(defprotocol Rel
  (rel-id [rel])
  (row-set [rel st]))

(defprotocol Restriction
  (restriction->fn [restriction get-variable])
  (restriction-deps [restriction]))

(defprotocol Extension
  (extension->fn [extension get-variable])
  (extension-deps [extension])
  (extension-keys [extension]))