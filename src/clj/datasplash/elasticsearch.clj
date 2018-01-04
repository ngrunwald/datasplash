(ns datasplash.elasticsearch
  (:require [datasplash.core :refer :all])
  (:import (org.apache.beam.sdk.io.elasticsearch ElasticsearchIO ElasticsearchIO$ConnectionConfiguration)
           (org.apache.beam.sdk.values PBegin)
           (org.apache.beam.sdk Pipeline)))

(defn config
  [hosts index type {:keys [username password]}]
  (let [hosts-array (into-array String hosts)]
    (cond-> (ElasticsearchIO$ConnectionConfiguration/create hosts-array index type)
      username (.withUsername username)
      password (.withPassword password))))

(def ^:no-doc elasticsearch-io-schema
  (merge
   named-schema
   {:username {:docstr "username"}
    :password {:docstr "password"}}))

(def ^:no-doc read-elasticsearch-schema
  (merge
   elasticsearch-io-schema
   {:batch-size {:docstr "Specify the scroll size (number of document by page). Default to 100."
                 :action (fn [transform ^Long b] (.withBatchSize transform b))}
    :query {:docstr "Specify a scroll query."
            :action (fn [transform ^String q] (.withQuery transform q))}
    :keep-alive {:docstr "Specify the scroll keepalive. Default to \"5m\"."
                 :action (fn [transform ^String q] (.withQuery transform q))}}))

(defn elasticsearch-read
  {:doc (with-opts-docstr
          "Read from elasticsearch.

See https://beam.apache.org/documentation/sdks/javadoc/2.1.0/org/apache/beam/sdk/io/elasticsearch/ElasticsearchIO.html

Examples:
```
(es/elasticsearch-read [\"http://127.0.0.1:9200\"] \"my-index\" \"my-type\" {:batch-size 100 :keep-alive \"5m\"} pcoll)
```"
          read-elasticsearch-schema)
   :added "0.5.3"}
  ([hosts index type opts p]
   (let [pipe (if (instance? Pipeline p) (PBegin/in p) p)
         transform (.withConnectionConfiguration (ElasticsearchIO/read) (config hosts index type opts))]
     (apply-transform pipe transform read-elasticsearch-schema opts)))
  ([hosts index type p] (elasticsearch-read hosts index type {} p)))

(def ^:no-doc write-elasticsearch-schema
  (merge
   elasticsearch-io-schema
   {:max-batch-size {:docstr "Specify the max number of documents in a bulk. Default to 1000"
                     :action (fn [transform ^Long b] (.withMaxBatchSizeBytes transform b))}
    :max-batch-size-bytes {:docstr "Specify the max number of bytes in a bulk. Default to 5MB"
                           :action (fn [transform ^Long b] (.withMaxBatchSizeBytes transform b))}}))

(defn elasticsearch-write
  {:doc (with-opts-docstr
          "Write from elasticsearch.

See https://beam.apache.org/documentation/sdks/javadoc/2.1.0/org/apache/beam/sdk/io/elasticsearch/ElasticsearchIO.html

Examples:
```
(es/elasticsearch-write [\"http://127.0.0.1:9200\"] \"my-index\" \"my-type\")
```"
          write-elasticsearch-schema)
   :added "0.5.3"}
  ([hosts index type opts p]
   (let [transform (.withConnectionConfiguration (ElasticsearchIO/write) (config hosts index type opts))]
     (apply-transform p transform elasticsearch-io-schema opts)))
  ([hosts index type p] (elasticsearch-write hosts index type {} p)))
