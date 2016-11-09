(ns datasplash.datastore
  (:require [datasplash.core :refer :all])
  (:import
   [com.google.datastore.v1.client DatastoreHelper ]
   [com.google.datastore.v1 Entity]
   ;; [com.google.cloud.datastore Datastore DatastoreOptions Entity Key KeyFactory]
   [com.google.cloud.dataflow.sdk.io.datastore DatastoreIO ]
   [com.google.cloud.dataflow.sdk Pipeline]
   [com.google.cloud.dataflow.sdk.values PBegin PCollection])
  (:gen-class))

(defn write-datastore-raw
  "Write a pcoll of already generated datastore entity in datastore"
  [{:keys [project-id] :as options} pcoll]
  (let [opts (assoc options :label :write-datastore)
        ptrans (-> (DatastoreIO/v1)
                   (.write)
                   (.withProjectId project-id))]
    (apply-transform pcoll ptrans named-schema opts)))


(defn make-ds-entity
  "Generate a datastore entity. Take as parameters the string key of the entity, a map containing the different fields and an options map defining the datastore namespace and kind"
  [raw-key raw-values {:keys [ds-namespace ds-kind excluded-from-index] :as options}]
  (let [key-builder (DatastoreHelper/makeKey (into-array [ds-kind raw-key]))
        excluded-set (into #{} (map name excluded-from-index))
        _ (when ds-namespace (.setNamespaceId (.getPartitionIdBuilder key-builder) ds-namespace))
        entity-builder (Entity/newBuilder)
        _ (.setKey entity-builder (.build key-builder))
        _ (doseq [[v-key v-val] raw-values]
            (.put (.getMutableProperties entity-builder)
                  (if (keyword? v-key) (name v-key) v-key )
                  (-> (DatastoreHelper/makeValue v-val)
                      (cond-> (excluded-set (name v-key)) (.setExcludeFromIndexes true))
                      (.build ))))]
    (.build entity-builder)))
