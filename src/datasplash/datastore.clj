(ns datasplash.datastore
  (:require [datasplash.core :refer :all])
  (:import
   [com.google.datastore.v1.client DatastoreHelper]
   [com.google.cloud.dataflow.sdk.io.datastore DatastoreIO]
   [com.google.cloud.dataflow.sdk Pipeline]
   [com.google.cloud.dataflow.sdk.values PBegin PCollection]
   [com.google.datastore.v1 Entity Value Entity$Builder Key$Builder Value$Builder Value$ValueTypeCase]
   [com.google.protobuf ByteString])
  (:gen-class))

(defn write-datastore-raw
  "Write a pcoll of already generated datastore entity in datastore"
  [{:keys [project-id] :as options} pcoll]
  (let [opts (assoc options :label :write-datastore-raw)
        ptrans (-> (DatastoreIO/v1)
                   (.write)
                   (.withProjectId project-id))]
    (apply-transform pcoll ptrans named-schema opts)))


(defn read-datastore-raw
  "Read a datastore source return a pcoll of raw datastore entities"
  [{:keys [project-id query] :as options} pcoll]
  (let [opts (assoc options :label :write-datastore)
        ptrans (-> (DatastoreIO/v1)
                   (.read)
                   (.withProjectId project-id)
                   (cond-> query (.withQuery query)))]
    (apply-transform pcoll ptrans named-schema opts)))

(declare value->clj)

(def type-mapping {(Value$ValueTypeCase/valueOf "INTEGER_VALUE") (fn [^Value v] (.getIntegerValue v))
                   (Value$ValueTypeCase/valueOf "DOUBLE_VALUE") (fn [^Value v] (.getDoubleValue v))
                   (Value$ValueTypeCase/valueOf "STRING_VALUE") (fn [^Value v] (.getStringValue v))
                   (Value$ValueTypeCase/valueOf "BOOLEAN_VALUE") (fn [^Value v] (.getBooleanValue v))
                   (Value$ValueTypeCase/valueOf "BLOB_VALUE") (fn [^Value v] (.toByteArray (.getBlobValue v)))
                   (Value$ValueTypeCase/valueOf "ARRAY_VALUE") (fn [^Value v] (mapv value->clj (.getValuesList (.getArrayValue v))))
                   (Value$ValueTypeCase/valueOf "NULL_VALUE") (constantly nil)})

(defn value->clj
  [v]
  (let [t (.getValueTypeCase v)
        tx (type-mapping t)]
    (if tx
      (tx v)
      (throw (ex-info (format "Datastore type not supported: %s" t) {:value v})))))

(defn entity->clj
  [e]
  (reduce (fn [acc kv]
            (let [value (value->clj (.getValue kv))]
              (assoc acc (keyword (.getKey kv)) value)))
          {} (.getProperties e)))

(defprotocol IValDS
  (make-ds-value-builder [v options]))

(declare make-ds-value)

(extend-protocol IValDS
  String
  (make-ds-value-builder [^String v _] (DatastoreHelper/makeValue v))
  clojure.lang.Keyword
  (make-ds-value-builder [v _] (DatastoreHelper/makeValue (name v)))
  java.util.Date
  (make-ds-value-builder [^java.util.Date v _] (DatastoreHelper/makeValue v))
  clojure.lang.PersistentList
  (make-ds-value-builder [v options] (DatastoreHelper/makeValue ^Iterable (mapv #(make-ds-value % options) v)))
  clojure.lang.PersistentHashSet
  (make-ds-value-builder [v options] (DatastoreHelper/makeValue ^Iterable (mapv #(make-ds-value % options) v)))
  clojure.lang.PersistentTreeSet
  (make-ds-value-builder [v options] (DatastoreHelper/makeValue ^Iterable (mapv #(make-ds-value % options) v)))
  clojure.lang.PersistentVector
  (make-ds-value-builder [v options] (DatastoreHelper/makeValue ^Iterable (mapv #(make-ds-value % options) v)))
  Object
  (make-ds-value-builder [v _] (DatastoreHelper/makeValue v)))

(defn- make-ds-entity-builder
  [raw-key raw-values {:keys [ds-namespace ds-kind exclude-from-index] :as options}]
  (let [^Key$Builder key-builder (DatastoreHelper/makeKey (into-array [ds-kind raw-key]))
        excluded-set (into #{} (map name exclude-from-index))
        ^Entity$Builder entity-builder (Entity/newBuilder)]
    (when ds-namespace (.setNamespaceId (.getPartitionIdBuilder key-builder) ds-namespace))
    (.setKey entity-builder (.build key-builder))
    (doseq [[v-key v-val] raw-values
            :when v-val]
      (.put (.getMutableProperties entity-builder)
            (if (keyword? v-key) (name v-key) v-key)
            (let [^Value$Builder val-builder (make-ds-value-builder v-val options)]
              (-> val-builder
                  (cond-> (excluded-set (name v-key)) (.setExcludeFromIndexes true))
                  (.build)))))
    entity-builder))

(defn- make-ds-value
  [v options]
  ;; stop building here when print bug id fixed in gcloud and handle excluded afterwards
  (.build ^Value$Builder (make-ds-value-builder v options)))

(defn make-ds-entity
  "Builds a Datastore Entity with the given key, value is a map corresponding to the desired entity, and options contains namespace, kind and an optional set of keys that shoud not be indexed (irrespective of nesting). For now repeated fields are supported but not nested entity"
  [raw-key raw-values {:keys [ds-namespace ds-kind exclude-from-index] :as options}]
  (-> ^Entity$Builder (make-ds-entity-builder raw-key raw-values options)
      (.build)))
