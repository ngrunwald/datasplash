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
  [{:keys [project-id] :as options} pcoll]
  (let [opts (assoc options :label :write-datastore)
        ptrans (-> (DatastoreIO/v1)
                   (.write)
                   (.withProjectId project-id))]
    (apply-transform pcoll ptrans named-schema opts)))



;; (defn make-ds-entity
;;   [raw-key raw-body {:keys [project-id ds-kind ds-namespace] :as options}]
;;   (let [dso (.getService (DatastoreOptions/getDefaultInstance))
;;         set-kind (fn [x kind] (.invoke (doto (.getDeclaredMethod com.google.cloud.datastore.BaseKey$Builder "setKind"
;;                                                                  (into-array java.lang.Class [java.lang.String]))
;;                                          (.setAccessible true))
;;                                        x
;;                                        (into-array [kind])))
;;         set-project-id (fn [x project] (.invoke (doto (.getDeclaredMethod com.google.cloud.datastore.BaseKey$Builder "setProjectId"
;;                                                                           (into-array java.lang.Class [java.lang.String]))
;;                                                   (.setAccessible true))
;;                                                 x
;;                                                 (into-array [project])))
;;         set-namespace (fn [x namespac] (.invoke (doto (.getDeclaredMethod com.google.cloud.datastore.BaseKey$Builder "setNamespace"
;;                                                                           (into-array java.lang.Class [java.lang.String]))
;;                                                   (.setAccessible true))
;;                                                 x
;;                                                 (into-array [namespac])))
;;         key (-> dso
;;                 (.newKeyFactory)
;;                 (set-project-id project-id)
;;                 (set-kind ds-kind)
;;                 (set-namespace ds-namespace)
;;                 (.newKey raw-key))
;;         entity (-> (Entity/newBuilder key)
;;                    (.set "body" (pr-str raw-body))
;;                    (.build))]
;;     entity))


(defn make-ds-entity
  [raw-key raw-value {:keys [ds-namespace ds-kind] :as options}]
  (let [key-builder (DatastoreHelper/makeKey (into-array [ds-kind raw-key]))
        _ (when ds-namespace (.setNamespaceId (.getPartitionIdBuilder key-builder) ds-namespace))
        entity-builder (Entity/newBuilder)
        _ (.setKey entity-builder (.build key-builder))
        _ (.put (.getMutableProperties entity-builder) "body" (.build (DatastoreHelper/makeValue (pr-str raw-value))))]
    (.build entity-builder)))
