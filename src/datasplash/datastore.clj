(ns datasplash.datastore
  (:require [datasplash.core :refer :all])
  (:import
   [com.google.datastore.v1.client DatastoreHelper ]
   [com.google.datastore.v1 Entity]
   ;; [com.google.cloud.datastore Datastore DatastoreOptions Entity Key KeyFactory]
   [com.google.cloud.dataflow.sdk.io.datastore DatastoreIO ]
   [com.google.cloud.dataflow.sdk Pipeline]
   [com.google.cloud.dataflow.sdk.values PBegin PCollection]
   [com.google.datastore.v1 Entity$Builder Key$Builder Value$Builder])
  (:gen-class))

(defn write-datastore-raw
  "Write a pcoll of already generated datastore entity in datastore"
  [{:keys [project-id] :as options} pcoll]
  (let [opts (assoc options :label :write-datastore)
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

(defprotocol IValDS
  (make-ds-value-builder [options v]))

(declare make-ds-value)

(extend-protocol IValDS
  String
  (make-ds-value-builder [_ ^String v] (DatastoreHelper/makeValue v))
  java.util.Date
  (make-ds-value-builder [_ ^java.util.Date v] (DatastoreHelper/makeValue v))
  Long
  (make-ds-value-builder [_ ^Long v] (DatastoreHelper/makeValue v))
  java.util.List
  (make-ds-value-builder [options ^Iterable v] (mapv #(make-ds-value options %) v))
  Object
  (make-ds-value-builder [_ v] (DatastoreHelper/makeValue v)))

(defn make-ds-value
  [options v]
  ;; stop building here when print bug id fixed in gcloud and handle excluded afterwards
  (.build ^Value$Builder (make-ds-value-builder options v)))

(defn make-ds-entity-builder
  "Generate a datastore entity. Take as parameters the string key of the entity, a map containing the different fields and an options map defining the datastore namespace and kind"
  [raw-key raw-values {:keys [ds-namespace ds-kind exclude-from-index] :as options}]
  (let [^Key$Builder key-builder (DatastoreHelper/makeKey (into-array [ds-kind raw-key]))
        excluded-set (into #{} (map name exclude-from-index))
        ^Entity$Builder entity-builder (Entity/newBuilder)]
    (when ds-namespace (.setNamespaceId (.getPartitionIdBuilder key-builder) ds-namespace))
    (.setKey entity-builder (.build key-builder))
    (doseq [[v-key v-val] raw-values]
      (.put (.getMutableProperties entity-builder)
            (if (keyword? v-key) (name v-key) v-key)
            (let [^Value$Builder val-builder (make-ds-value-builder options v-val)]
              (-> val-builder
                  (cond-> (excluded-set (name v-key)) (.setExcludeFromIndexes true))
                  (.build)))))
    entity-builder))

(defn make-ds-entity
  [raw-key raw-values {:keys [ds-namespace ds-kind exclude-from-index] :as options}]
  (-> ^Entity$Builder (make-ds-entity-builder raw-key raw-values options)
      (.build)))
