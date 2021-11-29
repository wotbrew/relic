(ns com.wotbrew.relic.ed
  (:require [com.wotbrew.relic :as rel]
            [clojure.edn :as edn])
  (:import (javax.swing JTable JFrame JScrollPane SwingUtilities)
           (java.awt BorderLayout)
           (javax.swing.table AbstractTableModel)
           (java.awt.event WindowAdapter)))

(defn- get-state [ui-state relvar]
  (get @ui-state relvar))

(defn- transact! [ui-state & tx]
  (let [runnables (volatile! [])]
    (swap!
      ui-state
      (fn [{:keys [db] :as st}]
        (vreset! runnables [])
        (let [{:keys [result, changes]} (apply rel/track-transact db tx)
              st (assoc st :db result)
              st (reduce-kv (fn [st relvar {:keys [added, deleted]}]
                              (if (and (empty? added) (empty? deleted))
                                st
                                (let [aliases (get-in st [:aliases relvar])]
                                  (reduce
                                    (fn [st a]
                                      (let [{:keys [model, col-vec]} (st a)
                                            now-editable (some? (rel/unwrap-table relvar))
                                            row-vec (vec (rel/q result relvar))
                                            col-seq (rel/columns relvar)
                                            real-cols (into #{} (mapcat keys) row-vec)
                                            old-col-vec col-vec
                                            col-vec (->> (concat col-seq real-cols) (distinct) vec)]
                                        (vswap! runnables conj
                                                (if (not= old-col-vec col-vec)
                                                  #(do (.fireTableStructureChanged @model)
                                                       (.fireTableDataChanged @model))
                                                  #(.fireTableDataChanged @model)))
                                        (assoc st a {:model model
                                                     :editable now-editable
                                                     :col-vec col-vec
                                                     :row-vec row-vec
                                                     :relvar-value relvar})))
                                    st
                                    aliases))))
                            st
                            changes)]
          st)))
    (doseq [r @runnables]
      (SwingUtilities/invokeLater r)))
  nil)

(defn- create-model [ui-state relvar]
  (proxy [AbstractTableModel] []
    (getColumnCount []
      (let [{:keys [col-vec]} (get-state ui-state relvar)]
        (count col-vec)))

    (getRowCount []
      (let [{:keys [row-vec]} (get-state ui-state relvar)]
        (count row-vec)))

    (getColumnName [col]
      (let [{:keys [col-vec]} (get-state ui-state relvar)]
        (str (nth col-vec col))))

    (getColumnClass [col] String)

    (isCellEditable [row col]
      (let [{:keys [editable]} (get-state ui-state relvar)]
        (boolean editable)))

    (setValueAt [val row col]
      (let [{:keys [relvar-value, col-vec, row-vec]} (get-state ui-state relvar)
            k (nth col-vec col)
            r (nth row-vec row)
            parsed-v (try (edn/read-string val) (catch Throwable e ::parse-error))
            table (rel/unwrap-table relvar-value)]
        (when (and r (not= ::parse-error parsed-v))
          (let [nr (assoc r k parsed-v)]
            (transact! ui-state [:delete-exact table r] [:insert table nr])))))

    (getValueAt [row col]
      (let [{:keys [col-vec, row-vec]} (get-state ui-state relvar)
            k (nth col-vec col)
            r (nth row-vec row)]
        (when r
          (binding [*print-length* 100] (pr-str (get r k))))))))

(defn- clear-old [st relvar ev]
  (let [db (:db st {})
        ev-old-aliases (get (:aliases st) ev #{})
        ev-new-aliases (disj ev-old-aliases relvar)
        can-remove (and ev (empty? ev-new-aliases))
        db (if can-remove (rel/unwatch db ev) db)
        st (if can-remove (update st :aliases dissoc ev) (assoc-in st [:aliases ev] ev-new-aliases))]
    (assoc st :db db)))

(defn ed* [ui-state relvar]
  (let [runnables (ThreadLocal.)]
    (letfn [(setup-relvar! [relvar relvar-value]
              (swap! ui-state setup-relvar relvar relvar-value)
              (doseq [r (.get runnables)]
                (SwingUtilities/invokeLater r))
              (.set runnables nil)
              nil)
            (setup-relvar [st relvar relvar-value]
              (.set runnables [])
              (let [{ev :relvar-value, :keys [col-vec, model]} (st relvar)]
                (if (= ev relvar-value)
                  st
                  (let [st (if ev (clear-old st relvar ev) st)
                        db (:db st {})
                        st (update-in st [:aliases relvar-value] (fnil conj #{}) relvar)
                        db (rel/watch db relvar-value)
                        st (assoc st :db db)
                        row-vec (vec (rel/q db relvar-value))
                        col-seq (rel/columns relvar-value)
                        real-cols (into #{} (mapcat keys) row-vec)
                        old-col-vec col-vec
                        col-vec (->> (concat col-seq real-cols) (distinct) vec)]

                    (when model
                      (if (not= old-col-vec col-vec)
                        #(do (.fireTableStructureChanged @model)
                             (.fireTableDataChanged @model))
                        #(.fireTableDataChanged @model)))

                    (assoc st relvar {:relvar-value relvar-value
                                      :col-vec col-vec
                                      :row-vec row-vec
                                      :model (or model (delay (create-model ui-state relvar)))
                                      :editable (boolean (rel/unwrap-table relvar-value))})))))]
      (when (var? relvar)
        (add-watch relvar ::watch (fn [k v o n] (setup-relvar! v n))))
      (setup-relvar! relvar (if (var? relvar) @relvar relvar))
      nil)))

(def ^:private ui-state (atom {}))

(defn ed [relvar]
  (let [nm (if (var? relvar) (str relvar) (if-some [[[_ n]] (rel/unwrap-table relvar)] (str "Base " n) "Derived View"))
        p (JFrame. nm)
        _ (assert (not (false? (ed* ui-state relvar))))
        table (JTable. @(:model (@ui-state relvar)))
        _ (.setFillsViewportHeight table true)
        _ (.addWindowListener p (proxy [WindowAdapter] []
                                   (windowClosing [evt]
                                     (when (var? relvar)
                                       (remove-watch relvar ::watch))
                                     (swap! ui-state (fn [st]
                                                      (let [{ev :relvar-value} (st relvar)
                                                            st (clear-old st relvar ev)]
                                                        st)))
                                     nil)))
        _ (.setDefaultCloseOperation p JFrame/DISPOSE_ON_CLOSE)
        scroll-pane (JScrollPane. table)]
    (.add p scroll-pane BorderLayout/CENTER)
    (.setSize p 640 480)
    (.setVisible p true)))

(defn global-transact! [& tx]
  (apply transact! ui-state tx))

(defn global-set-env! [env]
  (global-transact! (rel/set-env-tx env)))