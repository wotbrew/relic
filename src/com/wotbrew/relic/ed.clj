(ns com.wotbrew.relic.ed
  (:require [com.wotbrew.relic :as r])
  (:import (javax.swing JTable JFrame JScrollPane SwingUtilities)
           (java.awt BorderLayout)
           (javax.swing.table AbstractTableModel)))

(def db
  (ref {}))

(def models
  (ref {}))

(def ui-agent
  (agent nil))

(defn- init [relvar]
  (dosync
    (if (get (ensure db) relvar)
      (:table (get @models relvar))
      (let [st (ensure db)
            st (r/materialize st relvar)
            st (r/watch st relvar)
            rows (r/query st relvar)
            _ (ref-set db st)

            cols (r/col-data relvar)
            col-keys (mapv :k cols)

            mdl-state
            (ref (vec rows))

            mdl
            (proxy [AbstractTableModel] []
              (getColumnCount [] (count cols))
              (getRowCount [] (count @mdl-state))
              (getColumnName [col] (str (nth col-keys col)))
              (getValueAt [row col]
                (let [k (nth col-keys col)
                      r (nth @mdl-state row nil)]
                  (when r
                    (binding [*print-length* 100] (pr-str (get r k))))))
              (getColumnClass [col] String)
              (isCellEditable [row col] false)
              (setValueAt [val row col] nil))

            _ (alter models assoc relvar {:table mdl :state mdl-state})]
        mdl))))

(defn set-state [st]
  (dosync
    (ref-set db st)
    (doseq [[relvar {:keys [^AbstractTableModel table state]}] @models]
      (ref-set state (vec (r/query st relvar)))
      (send ui-agent (fn [_]
                       (SwingUtilities/invokeLater
                         (fn []
                           (.fireTableDataChanged table))))))))

(defn transact [& tx]
  (dosync
    (let [st (ensure db)
          {:keys [result changes]} (apply r/track-transact st tx)]
      (doseq [[relvar {:keys [added deleted]}] changes
              :let [{:keys [state ^AbstractTableModel table]} (get @models relvar)]]
        (when (and table (or (seq added) (seq deleted)))
          (ref-set state (vec (r/query result relvar)))
          (send ui-agent (fn [_]
                           (SwingUtilities/invokeLater
                             (fn []
                               (.fireTableDataChanged table)))))))
      (ref-set db result))))

(defn- ed-derived [relvar]
  (let [p (JFrame. "VIEW")
        mdl (init relvar)
        table (JTable. mdl)
        _ (.setFillsViewportHeight table true)
        scroll-pane (JScrollPane. table)]
    (.add p scroll-pane BorderLayout/CENTER)
    (.setSize p 640 480)
    (.setVisible p true)))

(defn- ed-base [relvar]
  (ed-derived relvar))

(defn ed [relvar]
  (if-some [base (r/unwrap-base relvar)]
    (ed-base base)
    (ed-derived relvar)))