(ns datasplash.elasticsearch
  (:require [datasplash.core :refer :all])
  (:import (org.apache.beam.sdk.io.elasticsearch ElasticsearchIO ElasticsearchIO$ConnectionConfiguration)
           (org.apache.beam.sdk.values PBegin)
           (org.apache.beam.sdk Pipeline)))

(defn elasticsearch-io
  [method hosts index type {:keys [username password query batch-size batch-size-bytes]}]
  (let [hosts-array (into-array String hosts)
        connection-conf (cond-> (ElasticsearchIO$ConnectionConfiguration hosts-array index type)
                          username (.withUsername username)
                          password (.withPassword password))
        io (cond-> method
             batch-size (.withBatchSize batch-size)
             batch-size-bytes (.withBatchSizeBytes batch-size-bytes)
             query (.withQuery query))]
    (apply-transform pipe (.withConnectionConfiguration io connection-conf))))

(defn read-from-elasticsearch
  ([hosts index type opts]
   (let [pipe (if (instance? Pipeline p) (PBegin/in p) p)]
     (elasticsearch-io (Elasticsearch/read) hosts index type opts)))
  ([hosts index type] (read-from-elasticsearch hosts index type {})))

(defn write-to-elasticsearch
  ([hosts index type opts]
   (elasticsearch-io (Elasticsearch/read) hosts index type opts))
  ([hosts index type] (write-to-elasticsearch hosts index type {})))
