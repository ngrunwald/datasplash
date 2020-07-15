(ns datasplash.datastore
  (:require [datasplash.core :refer :all])
  (:import
   [com.google.datastore.v1.client DatastoreHelper]
   [org.apache.beam.sdk.io.gcp.datastore DatastoreIO]
   [com.google.datastore.v1 Entity Value Key Entity$Builder Key$Builder Value$Builder Value$ValueTypeCase Key$PathElement]
   [com.google.protobuf ByteString NullValue]
   [java.util Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry Date])
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
  [{:keys [project-id query namespace num-query-split] :as options} pcoll]
  (let [opts (assoc options :label :write-datastore)
        ptrans (-> (DatastoreIO/v1)
                   (.read)
                   (.withProjectId project-id)
                   (cond-> query (.withQuery query))
                   (cond-> namespace (.withNamespace namespace))
                   (cond-> num-query-split (.withNumQuerySplits num-query-split)))]
    (apply-transform pcoll ptrans named-schema opts)))

(defn delete-datastore-raw
  "delete a pcoll of already generated datastore entity from datastore"
  [{:keys [project-id] :as options} pcoll]
  (let [opts (assoc options :label :delete-datastore-raw)
        ptrans (-> (DatastoreIO/v1)
                   (.deleteEntity)
                   (.withProjectId project-id))]
    (apply-transform pcoll ptrans named-schema opts)))

(declare value->clj)
(declare entity->clj)

(def ^:dynamic type-mapping
  {(Value$ValueTypeCase/valueOf "INTEGER_VALUE") (fn [^Value v] (.getIntegerValue v))
   (Value$ValueTypeCase/valueOf "DOUBLE_VALUE") (fn [^Value v] (.getDoubleValue v))
   (Value$ValueTypeCase/valueOf "STRING_VALUE") (fn [^Value v] (.getStringValue v))
   (Value$ValueTypeCase/valueOf "BOOLEAN_VALUE") (fn [^Value v] (.getBooleanValue v))
   (Value$ValueTypeCase/valueOf "BLOB_VALUE") (fn [^Value v]
                                                (.toByteArray (.getBlobValue v)))
   (Value$ValueTypeCase/valueOf "ARRAY_VALUE") (fn [^Value v]
                                                 (mapv value->clj (.getValuesList (.getArrayValue v))))
   (Value$ValueTypeCase/valueOf "ENTITY_VALUE") (fn [^Value v]
                                                  (entity->clj (.getEntityValue v)))
   (Value$ValueTypeCase/valueOf "TIMESTAMP_VALUE") (fn [^Value v] (DatastoreHelper/toDate v))
   (Value$ValueTypeCase/valueOf "GEO_POINT_VALUE") (fn [^Value v] (.getGeoPointValue v))
   (Value$ValueTypeCase/valueOf "NULL_VALUE") (constantly nil)})

(defn value->clj
  "Converts a Datastore Value to its Clojure equivalent"
  [^Value v]
  (let [t (.getValueTypeCase v)
        tx (type-mapping t)]
    (if tx
      (tx v)
      (throw (ex-info (format "Datastore type not supported: %s" t) {:value v :type t})))))

(defn entity->clj
  "Converts a Datastore Entity to a Clojure map with the same properties. Repeated fields are handled as vectors and nested Entities as maps. All keys are turned to keywords. If the entity has a Key, Kind or Namespace, these can be found as :key, :kind, :namespace and :path in the meta of the returned map"
  [^Entity e]
  (let [props (persistent!
               (reduce (fn [acc ^Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry kv]
                         (let [value (value->clj (.getValue kv))]
                           (assoc! acc (keyword (.getKey kv)) value)))
                       (transient {}) (.getProperties e)))
        [^Key k key-name kind path] (when (.hasKey e) (let [k (.getKey e)
                                                            results (map (fn [^Key$PathElement p]
                                                                           {:kind (.getKind p) :key (.getName p)})
                                                                         (.getPathList k))
                                                            {:keys [kind key]} (last results)]
                                                        [k key kind (butlast results)]))
        namespace (when (and k (.hasPartitionId k))
                    (some-> k (.getPartitionId) (.getNamespaceId)))]
    (-> props
        (cond-> k (with-meta {:key key-name :kind kind :namespace namespace :path path})))))

(defprotocol IValDS
  "Protocol governing to conversion to datastore Value types"
  (make-ds-value-builder [v] "Returns a Datastore Value builder for this particular value"))

(declare make-ds-value)
(declare make-ds-entity)

(extend-protocol IValDS
  (Class/forName "[B")
  (make-ds-value-builder [v] (DatastoreHelper/makeValue (ByteString/copyFrom ^bytes v)))
  String
  (make-ds-value-builder [^String v] (DatastoreHelper/makeValue v))
  clojure.lang.Keyword
  (make-ds-value-builder [v] (DatastoreHelper/makeValue (name v)))
  java.util.Date
  (make-ds-value-builder [^java.util.Date v] (DatastoreHelper/makeValue v))
  clojure.lang.PersistentList
  (make-ds-value-builder [v] (DatastoreHelper/makeValue ^Iterable (mapv #(make-ds-value %) v)))
  clojure.lang.PersistentHashSet
  (make-ds-value-builder [v] (DatastoreHelper/makeValue ^Iterable (mapv #(make-ds-value %) v)))
  clojure.lang.PersistentTreeSet
  (make-ds-value-builder [v] (DatastoreHelper/makeValue ^Iterable (mapv #(make-ds-value %) v)))
  clojure.lang.PersistentVector
  (make-ds-value-builder [v] (DatastoreHelper/makeValue ^Iterable (mapv #(make-ds-value %) v)))
  clojure.lang.PersistentHashMap
  (make-ds-value-builder [v] (DatastoreHelper/makeValue ^Entity (make-ds-entity v)))
  clojure.lang.PersistentArrayMap
  (make-ds-value-builder [v] (DatastoreHelper/makeValue ^Entity (make-ds-entity v)))
  clojure.lang.PersistentTreeMap
  (make-ds-value-builder [v] (DatastoreHelper/makeValue ^Entity (make-ds-entity v)))
  nil
  (make-ds-value-builder [v] (-> (Value/newBuilder) (.setNullValue (NullValue/valueOf "NULL_VALUE"))))
  Object
  (make-ds-value-builder [v] (DatastoreHelper/makeValue v)))

(defn make-ds-key
  [{:keys [kind key namespace path]}]
  (let [path (for [ancestor path]
               (if (instance? Key ancestor)
                 ancestor
                 (make-ds-key (merge {:namespace namespace :kind kind}
                                     (if (map? ancestor) ancestor {:key ancestor})))))
        key-builder (DatastoreHelper/makeKey (into-array Object (concat path [kind key])))]
    (when namespace (.setNamespaceId (.getPartitionIdBuilder key-builder) namespace))
    (.build key-builder)))

(defn- add-ds-key-namespace-kind
  [^Entity$Builder builder options]
  (.setKey builder (make-ds-key options))
  builder)

(defn- make-ds-entity-builder
  [raw-values {:keys [exclude-from-index] :as options}]
  (let [excluded-set (into #{} (map name exclude-from-index))
        ^Entity$Builder entity-builder (Entity/newBuilder)]
    (doseq [[v-key v-val] raw-values]
      (.put (.getMutableProperties entity-builder)
            (if (keyword? v-key) (name v-key) v-key)
            (let [^Value$Builder val-builder (make-ds-value-builder v-val)]
              (-> val-builder
                  (cond-> (excluded-set (name v-key)) (.setExcludeFromIndexes true))
                  (.build)))))
    entity-builder))

(defn make-ds-value
  [v]
  (.build ^Value$Builder (make-ds-value-builder v)))

(defn make-ds-entity
  "Builds a Datastore Entity with the given Clojure value which is a map or seq of KVs corresponding to the desired entity, and options contains an optional key, path, namespace, kind and an optional set of field names that shoud not be indexed (only supported for top level fields for now). Supports repeated fields and nested entities (as nested map)"
  ([raw-values {:keys [key namespace kind path exclude-from-index] :as options}]
   (let [^Entity$Builder builder (-> (make-ds-entity-builder raw-values options)
                                     (cond-> key (add-ds-key-namespace-kind options)))]
     (.build builder)))
  ([raw-values] (make-ds-entity raw-values {})))
