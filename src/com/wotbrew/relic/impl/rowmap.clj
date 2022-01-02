(ns com.wotbrew.relic.impl.rowmap
  (:import (clojure.lang RT IPersistentCollection Seqable IPersistentMap IPersistentVector IKVReduce Associative ILookup Util Murmur3 IFn MapEntry IHashEq)
           (java.util Map$Entry Map)))

(defprotocol Unwrap
  (unwrap [m]))

(extend-protocol Unwrap
  Object
  (unwrap [m] m))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(declare empty-row-map)

;; map with incremental hashing for fast indexing & deduping
;; also has slightly faster merging with a reduce-kv fast path
(deftype RowMap [^long unmixed
                 ^long mixed
                 m]
  Unwrap
  (unwrap [_] m)
  Object
  (toString [this] (.toString m))
  (equals [this o] (.equals m o))
  (hashCode [this] (.hashCode m))
  IPersistentCollection
  (count [this] (.count ^IPersistentCollection m))
  (cons [this o]
    (cond
      (instance? Map$Entry o) (.assoc ^IPersistentMap this (.getKey ^Map$Entry o) (.getValue ^Map$Entry o))
      (instance? IPersistentVector o) (.assoc ^IPersistentMap this (nth o 0) (nth o 1))
      (instance? IKVReduce o) (reduce-kv assoc this o)
      :else (reduce conj this o)))
  (empty [this] empty-row-map)
  (equiv [this o] (.equiv ^IPersistentCollection m o))
  Seqable
  (seq [this] (.seq ^Seqable m))
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

  IHashEq
  (hasheq [_] mixed)

  ;; this is where the magic happens for this impl
  IPersistentMap
  (assoc [this key val]
    (let [e (find m key)
          eh (int (if e (- (Util/hasheq e)) 0))
          ne (MapEntry. key val)
          nh (Util/hasheq ne)
          m2 (.assoc ^IPersistentMap m key val)
          h2 (unchecked-add-int (int unmixed) eh)
          h3 (unchecked-add-int h2 nh)]
      (RowMap. h3 (Murmur3/mixCollHash h3 (.count ^IPersistentCollection m2)) m2)))
  (assocEx [this key val]
    (if (.containsKey ^Associative m key)
      (throw (RuntimeException. "Key already present."))
      (.assoc this key val)))
  (without [this key]
    (if-some [e (find m key)]
      (let [eh (int (if e (- (Util/hasheq e)) 0))
            m2 (.without ^IPersistentMap m key)
            h (int unmixed)
            h2 (unchecked-add-int h eh)
            hmx (Murmur3/mixCollHash h2 (.count ^IPersistentCollection m2))]
        (RowMap. h2 hmx m2))
      this))

  Iterable
  (iterator [this] (.iterator ^Iterable m))
  ILookup
  (valAt [this key] (.valAt ^ILookup m key))
  (valAt [this key notFound] (.valAt ^ILookup m key notFound))
  Associative
  (containsKey [this key] (.containsKey ^Associative m key))
  (entryAt [this key] (.entryAt ^Associative m key))
  IFn
  (invoke [this key] (m key))
  (invoke [this key not-found] (m key not-found)))

(defn from-clj-map [m]
  (if (instance? RowMap m)
    m
    (let [iterator (.iterator ^Iterable m)]
      (loop [unmixed (int 0)]
        (if (.hasNext iterator)
          (let [^MapEntry e (.next iterator)
                eh (Util/hasheq e)
                h2 (unchecked-add-int unmixed eh)]
            (recur h2))
          (RowMap.
            (long unmixed)
            (Murmur3/mixCollHash unmixed (.count ^IPersistentCollection m))
            m))))))

(def empty-row-map (from-clj-map {}))