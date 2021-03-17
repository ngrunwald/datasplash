(ns datasplash.es
  (:require [datasplash.core :refer :all]
            [cheshire.core :as json])
  (:import
   [org.apache.beam.sdk.io.elasticsearch
    ElasticsearchIO ElasticsearchIO$Read ElasticsearchIO$Write
    ElasticsearchIO$RetryConfiguration
    ElasticsearchIO$ConnectionConfiguration]
   [org.joda.time Duration]
   [org.apache.beam.sdk Pipeline]
   [datasplash.fns
    ExtractKeyFn]
   [org.apache.beam.sdk.values PBegin PCollection])
  (:gen-class))


(def ^:no-doc es-connection-schema
  (merge
   named-schema
   {:username          {:docstr "username"}
    :password          {:docstr "password"}
    :keystore-password {:docstr "If Elasticsearch uses SSL/TLS with mutual authentication (via shield), provide the password to open the client keystore."}
    :keystore-path     {:docstr "If Elasticsearch uses SSL/TLS with mutual authentication (via shield), provide the password to open the client keystore."}}))

(defn- es-config
  "Creates a new Elasticsearch connection configuration."
  [hosts index type {:keys [username password keystore-password keystore-path]}]
  (let [hosts-array (into-array String hosts)]
    (cond-> (ElasticsearchIO$ConnectionConfiguration/create hosts-array index type)
      username          (.withUsername username)
      password          (.withPassword password)
      keystore-password (.withKeystorePassword keystore-password)
      keystore-path     (.withKeystorePath keystore-path))))

(defn- retry-config
  "Creates RetryConfiguration for ElasticsearchIO with provided max-attempts, max-duration-ms and exponential backoff based retries."
  [max-attempts max-duration-ms]
  (let [duration (Duration. max-duration-ms)]
    (ElasticsearchIO$RetryConfiguration/create max-attempts duration)))

(def ^:no-doc read-es-schema
  (merge
   es-connection-schema
   {:key-fn              {:docstr "Can be either true (to coerce keys to keywords),false to leave them as strings, or a function to provide custom coercion."}
    :batch-size          {:docstr "Specify the scroll size (number of document by page). Default to 100. Maximum is 10 000. If documents are small, increasing batch size might improve read performance. If documents are big, you might need to decrease batch-size"
                        :action (fn [^ElasticsearchIO$Read transform ^Long b] (.withBatchSize transform b))}
    :query               {:docstr "Provide a query used while reading from Elasticsearch."
                        :action (fn [^ElasticsearchIO$Read transform ^String q] (.withQuery transform q))}
    :scroll-keep-alive   {:docstr "Provide a scroll keepalive. See https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-scroll.html . Default is \"5m\". Change this only if you get \"No search context found\" errors."
                        :action (fn [^ElasticsearchIO$Read transform ^String q] (.withQuery transform q))}}))

(defn- read-es-raw
  "Connects and reads form Elasticserach, returns a PColl of strings"
  [hosts index type options p]
  (let [opts (assoc options :label :read-es-raw)
        ptrans (-> (ElasticsearchIO/read)
                   (.withConnectionConfiguration (es-config hosts index type opts)))]
    (-> p
        (cond-> (instance? Pipeline p) (PBegin/in))
        (apply-transform ptrans read-es-schema opts))))

(defn- read-es-clj-transform
  "Connects to ES, reads, and convert serialized json to clojure map"
  [hosts index type options]
  (let [safe-opts (dissoc options :name)
        key-fn    (or (get options :key-fn) false)]
    (ptransform
     :read-es-to-clj
     [^PCollection pcoll]
     (->> pcoll
          (read-es-raw hosts index type safe-opts)
          (dmap (fn [x] (json/parse-string x key-fn)) safe-opts)))))

(defn read-es
  {:doc (with-opts-docstr
          "Read from elasticsearch.

See https://beam.apache.org/releases/javadoc/2.13.0/org/apache/beam/sdk/io/elasticsearch/ElasticsearchIO.html

Examples:
```
(es/read-es [\"http://127.0.0.1:9200\"] \"my-index\" \"my-type\" {:batch-size 100 :keep-alive \"5m\"} pcoll)
```"
          read-es-schema)
   :added "0.6.5"}
  ([hosts index type options p]
   (let [opts (assoc options :label :read-es)]
     (apply-transform p (read-es-clj-transform hosts index type options) base-schema opts)))
  ([hosts index type p]
      (read-es hosts index type {} p)))

(def ^:no-doc write-es-schema
  (merge
   es-connection-schema
   {:max-batch-size       {:docstr "Specify the max number of documents in a bulk. Default to 1000"
                           :action (fn [^ElasticsearchIO$Write transform ^Long b] (.withMaxBatchSizeBytes transform b))}
    :max-batch-size-bytes {:docstr "Specify the max number of bytes in a bulk. Default to 5MB"
                           :action (fn [^ElasticsearchIO$Write transform ^Long b] (.withMaxBatchSizeBytes transform b))}
    :retry-configuration  {:docstr "Creates RetryConfiguration for ElasticsearchIO with provided max-attempts, max-durations and exponential backoff based retries"
                           :action (fn [^ElasticsearchIO$Write transform [^Long max-attempts ^Long max-duration-ms]]
                                     (.withRetryConfiguration transform (retry-config max-attempts max-duration-ms)))}
    :id-fn                {:doctstr "Provide a function to extract the id from the document."
                           :action (fn [^ElasticsearchIO$Write transform key-fn]
                                     (let [serializing-key-fn (comp key-fn json/parse-string)
                                           id-fn (ExtractKeyFn. serializing-key-fn)]
                                       (.withIdFn transform id-fn)))}
    :index-fn             {:doctstr "Provide a function to extract the target index from the document allowing for dynamic document routing."
                           :action (fn [^ElasticsearchIO$Write transform key-fn]
                                     (let [serializing-key-fn (comp key-fn json/parse-string)
                                           index-fn (ExtractKeyFn. serializing-key-fn)]
                                       (.withIndexFn transform index-fn)))}
    :type-fn              {:docstr "Provide a function to extract the target type from the document allowing for dynamic document routing."
                           :action (fn [^ElasticsearchIO$Write transform key-fn]
                                     (let [serializing-key-fn (comp key-fn json/parse-string)
                                           type-fn (ExtractKeyFn. serializing-key-fn)]
                                       (.withTypeFn transform type-fn)))}
    :use-partial-update   {:docstr "Provide an instruction to control whether partial updates or inserts (default) are issued to Elasticsearch."
                           :action (fn [^ElasticsearchIO$Write transform is-partial-update]
                                     (.withUsePartialUpdate transform is-partial-update))}}))

(defn- write-es-raw
  ([hosts index type options ^PCollection pcoll]
   (let [opts (assoc options :label :write-es-raw)]
     (apply-transform pcoll (-> (ElasticsearchIO/write)
                                (.withConnectionConfiguration (es-config hosts index type opts)))
                      write-es-schema
                      opts)))
  ([hosts index type pcoll] (write-es-raw hosts index type {} pcoll)))

(defn- write-es-clj-transform
  [hosts index type options]
  (let [safe-opts (dissoc options :name)]
    (ptransform
     :write-es-from-clj
     [^PCollection pcoll]
     (->> pcoll
          (dmap (fn [x] (json/generate-string x)))
          (write-es-raw hosts index type safe-opts)))))

(defn write-es
  {:doc (with-opts-docstr
          "Write to elasticsearch.

See https://beam.apache.org/releases/javadoc/2.13.0/org/apache/beam/sdk/io/elasticsearch/ElasticsearchIO.html

Examples:
```
(es/write-es [\"http://127.0.0.1:9200\"] \"my-index\" \"my-type\" {:id-fn (fn [x] (get x \"id\")) :max-batch-size-bytes 50000000 :name write-es})
```"
          write-es-schema)
   :added "0.6.5"}
  ([hosts index type options ^PCollection pcoll]
   (let [opts (assoc options :label :write-es)]
     (apply-transform pcoll (write-es-clj-transform hosts index type opts) named-schema opts)))
  ([hosts index type pcoll] (write-es hosts index type {} pcoll)))
