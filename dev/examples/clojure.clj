(ns examples.clojure
  "Computes various statistics about the loaded clojure environment"
  (:require [com.wotbrew.tarpit :as rel]))

(def BaseLoadedLib
  (-> [:dynamic (fn [] [:const (for [sym (loaded-libs)] {:symbol sym})])]
      (rel/project :symbol)))

(def BaseVar
  (-> [:dynamic (fn [] [:const (for [sym (loaded-libs), var (vals (ns-publics sym))] {:var var})])]
      (rel/project :var)))

(def Namespace
  (-> BaseLoadedLib
      (rel/extend [:ns [find-ns :symbol]]
                  [:object [identity :ns]])))

(def Var
  (-> BaseVar
      (rel/extend [:value [deref :var]]
                  [:symbol [(fn [v] (.toSymbol v)) :var]]
                  [:object [identity :var]])))

(def Obj
  (-> Namespace
      (rel/union Var)
      (rel/project :object)
      (rel/extend [:meta [meta :object]])))

(def ObjDoc
  (-> Obj
      (rel/extend [:doc [:doc :meta]])
      (rel/restrict [not-empty :doc])
      (rel/project :object :doc)))

(def ObjFile
  (-> Obj
      (rel/extend [:file [:file :meta]])
      (rel/restrict [not-empty :file])
      (rel/project :object :file)))

(def Value
  (-> Var
      (rel/extend [:type [type :value]])
      (rel/project :value :type)))

(def Function
  (-> Value
      (rel/restrict [fn? :value])))

(def Str
  (-> Value
      (rel/restrict [string? :value])))

(def Num
  (-> Value
      (rel/restrict [number? :value])))

(defn state [] (rel/empty-state {}))

(comment
  (let [st (state)]
    (time (take 5 (rel/q st ObjFile)))))