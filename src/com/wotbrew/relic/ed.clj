(ns com.wotbrew.relic.ed
  (:require [com.wotbrew.relic :as r]
            [clojure.edn :as edn])
  (:import (javax.swing JTable JFrame JScrollPane SwingUtilities)
           (java.awt BorderLayout)
           (javax.swing.table AbstractTableModel)))

(def db
  (ref {}))

(def models
  (ref {}))

(def ui-agent
  (agent nil))

(defn- redraw [result changes]
  (doseq [[relvar {:keys [added deleted]}] changes
          :let [{:keys [state ^AbstractTableModel table]} (get @models relvar)]]
    (when (and table (or (seq added) (seq deleted)))
      (ref-set state (vec (r/q result relvar)))
      (send ui-agent (fn [_]
                       (SwingUtilities/invokeLater
                         (fn []
                           (.fireTableDataChanged table))))))))

(defn transact [& tx]
  (dosync
    (let [st (ensure db)
          {:keys [result changes]} (apply r/track-transact st tx)]
      (redraw result changes)
      (ref-set db result))))

(defn- init [relvar]
  (dosync
    (or (:table (get (ensure models) relvar))
      (let [st (ensure db)
            st (r/materialize st relvar)
            st (r/watch st relvar)
            rows (r/q st relvar)
            _ (ref-set db st)

            cols (r/col-data relvar)
            col-keys (mapv :k cols)

            mdl-state
            (ref (vec rows))

            mdl
            (if-some [base (r/unwrap-base relvar)]
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
                (isCellEditable [row col] true)
                (setValueAt [val row col]
                  (let [k (nth col-keys col)
                        r (nth @mdl-state row nil)
                        parsed-v (try (edn/read-string val) (catch Throwable e ::parse-error))]
                    (when (and r (not= ::parse-error parsed-v))
                      (let [nr (assoc r k parsed-v)]
                        (dosync
                          (alter mdl-state assoc row nr)
                          (let [st (ensure db)
                                {:keys [result changes]} (r/track-transact st [:delete base r] [:insert base nr])]
                            (redraw result (dissoc changes base relvar))
                            (ref-set db result))))))))
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
                (setValueAt [val row col] nil)))

            _ (alter models assoc relvar {:table mdl :state mdl-state})]
        mdl))))

(defn set-state [st]
  (dosync
    (ref-set db st)
    (doseq [[relvar {:keys [^AbstractTableModel table state]}] @models]
      (ref-set state (vec (r/q st relvar)))
      (send ui-agent (fn [_]
                       (SwingUtilities/invokeLater
                         (fn []
                           (.fireTableDataChanged table))))))))

(defn ed [relvar]
  (let [p (JFrame. (if-some [[[_ n]] (r/unwrap-base relvar)] (str "Base " n) "Derived View"))
        mdl (init relvar)
        table (JTable. mdl)
        _ (.setFillsViewportHeight table true)
        scroll-pane (JScrollPane. table)]
    (.add p scroll-pane BorderLayout/CENTER)
    (.setSize p 640 480)
    (.setVisible p true)))