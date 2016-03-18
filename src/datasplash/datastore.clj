(ns datasplash.datastore
  (:require [datasplash.core :refer :all])
  (:import [com.google.cloud.dataflow.sdk.io DatastoreIO Write]
           [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.values PBegin PCollection]))

(defn write-datastore-raw
  [{:keys [dataset host] :as options} pcoll]
  (let [opts (assoc options :label :write-datastore)
        ptrans (-> (DatastoreIO/sink)
                   (.withDataset (name dataset))
                   (cond-> host (.withHost host))
                   (Write/to))]
    (apply-transform pcoll ptrans named-schema opts)))
