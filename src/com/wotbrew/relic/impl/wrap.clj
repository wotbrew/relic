(ns com.wotbrew.relic.impl.wrap
  (:import (clojure.lang IHashEq IPersistentMap Associative ILookup IPersistentCollection Seqable IMeta IObj MapEquivalence IFn ArityException IKVReduce Counted)
           (java.util Map)))

(defprotocol GraphUnsafe
  (set-graph [db graph])
  (get-graph [db]))

(defprotocol Unwrap
  (unwrap [db]))

(deftype RelicDB [m graph]
  GraphUnsafe
  (set-graph [db graph2] (RelicDB. db graph2))
  (get-graph [db] graph)
  Unwrap
  (unwrap [_] m)
  Object
  (toString [_] (.toString m))
  (hashCode [_] (.hashCode m))
  (equals [_ o] (.equals m o))
  MapEquivalence
  IHashEq
  (hasheq [this] (.hasheq ^IHashEq m))
  IPersistentMap
  (assoc [this key val] (.assoc ^IPersistentMap m key val))
  (assocEx [this key val] (.assocEx ^IPersistentMap m key val))
  (without [this key] (.without ^IPersistentMap m key))
  Iterable
  (iterator [this] (.iterator ^Iterable m))
  Seqable
  (seq [this] (.seq ^Seqable m))
  Counted
  IPersistentCollection
  (count [this] (.count ^IPersistentCollection m))
  (cons [this o] (.cons ^IPersistentCollection m o))
  (empty [this] (.empty ^IPersistentCollection m))
  (equiv [this o] (.equiv ^IPersistentCollection m o))
  ILookup
  (valAt [this key] (.valAt ^ILookup m key))
  (valAt [this key notFound] (.valAt ^ILookup m key notFound))
  Associative
  (containsKey [this key] (.containsKey ^Associative m key))
  (entryAt [this key] (.entryAt ^Associative m key))
  Map
  (size [this] (.size ^Map m))
  (isEmpty [this] (.isEmpty ^Map m))
  (containsValue [this value] (.containsValue ^Map m value))
  (get [this key] (.get ^Map m key))
  (put [this key value] (.put ^Map m key value))
  (remove [this key] (.remove ^Map m key))
  (putAll [this m2] (.putAll ^Map m m2))
  (clear [this] (.clear ^Map m))
  (keySet [this] (.keySet ^Map m))
  (values [this] (.values ^Map m))
  (entrySet [this] (.entrySet ^Map m))
  IMeta
  (meta [this] (.meta ^IMeta m))
  IObj
  (withMeta [this meta] (RelicDB. (.withMeta ^IObj m meta) graph))
  IFn
  (invoke [this o] (.invoke ^IFn m o))
  (invoke [this o o1] (.invoke ^IFn m o o1))
  (applyTo [this arglist] (apply m arglist))
  Callable
  (call [this] (throw (ArityException. 0 "RelicDB")))
  Runnable
  (run [this] (throw (ArityException. 0 "RelicDB")))
  IKVReduce
  (kvreduce [this f init] (reduce-kv f init m)))

(defn db? [x] (instance? RelicDB x))