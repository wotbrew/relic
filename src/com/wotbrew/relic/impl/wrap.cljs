(ns com.wotbrew.relic.impl.wrap)

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
  (toString [coll]
    (str m))
  (equiv [this other]
    (-equiv m other))
  (keys [coll]
    (es6-iterator (keys coll)))
  (entries [coll]
    (es6-entries-iterator (seq coll)))
  (values [coll]
    (es6-iterator (vals coll)))
  (has [coll k]
    (contains? coll k))
  (get [coll k not-found]
    (-lookup coll k not-found))
  (forEach [coll f]
    (doseq [[k v] coll]
      (f v k)))
  IPrintWithWriter
  (-pr-writer [coll writer opts] (-pr-writer m writer opts))
  ICloneable
  (-clone [_] (RelicDB. m graph))
  IWithMeta
  (-with-meta [coll new-meta]
    (RelicDB. (-with-meta m new-meta) graph))
  IMeta
  (-meta [coll] (meta m))
  ICounted
  (-count [coll] (-count m))
  IAssociative
  (-contains-key? [coll k] (-contains-key? m k))
  (-assoc [coll k element] (-assoc m k element))
  IMap
  (-dissoc [coll k] (-dissoc m k))
  IFind
  (-find [coll k] (-find m k))
  ISeqable
  (-seq [o] (-seq m))
  IFn
  (-invoke [coll k] (-lookup m k))
  (-invoke [coll k not-found] (-lookup m k not-found))
  ILookup
  (-lookup [coll k] (-lookup m k))
  (-lookup [coll k not-found] (-lookup m k not-found))
  IEmptyableCollection
  (-empty [coll] (-empty m))
  ICollection
  (-conj [coll entry] (-conj m entry))
  IKVReduce
  (-kv-reduce [coll f init] (-kv-reduce m f init))
  IReduce
  (-reduce [coll f] (-reduce m f))
  (-reduce [coll f start] (-reduce m f start))
  IEquiv
  (-equiv [o other] (-equiv m other))
  IHash
  (-hash [o] (-hash m))
  IIterable
  (-iterator [coll] (-iterator m)))

(es6-iterable RelicDB)

(defn db? [x] (instance? RelicDB x))