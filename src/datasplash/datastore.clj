(ns datasplash.datastore
  (:require [datasplash.core :refer :all])
  (:import [com.google.cloud.dataflow.sdk.io DatastoreIO Write]
           [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.values PBegin PCollection]))

(defn write-datastore-raw
  [{:keys [dataset host] :as options} p]
  (let [opts (assoc options :label :write-datastore)
        ptrans (-> (DatastoreIO/sink)
                   (.withDataset dataset)
                   (cond-> host (.withHost host))
                   (Write/to))]
    (-> p
        (cond-> (instance? Pipeline p) (PBegin/in))
        (apply-transform ptrans named-schema opts))))
