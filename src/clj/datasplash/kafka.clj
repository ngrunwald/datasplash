(ns datasplash.kafka
  (:require
   [datasplash.core :as ds])
  (:import
   (org.apache.beam.sdk Pipeline)
   (org.apache.beam.sdk.io.kafka KafkaIO KafkaIO$Read KafkaIO$Write KafkaRecord)
   (org.apache.beam.sdk.values PBegin PCollection)
   (org.apache.kafka.common.header Header)
   (org.joda.time Duration Instant))
  (:gen-class))

(defn- kafka-record->clj
  "Maps a `KafkaRecord` to a clojure map.
   See: https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/io/kafka/KafkaRecord.html"
  [^KafkaRecord r]
  (let [kv (.getKV r)]
    {:payload   (.getValue kv)
     :key       (.getKey kv)
     :offset    (.getOffset r)
     :partition (.getPartition r)
     :timestamp (.getTimestamp r)
     :topic     (.getTopic r)
     :headers   (->> (.getHeaders r)
                     (.toArray)
                     (reduce (fn [acc ^Header header]
                               (assoc! acc (.key header) (.value header)))
                             (transient {}))
                     (persistent!))}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;; Read ;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:no-doc read-kafka-schema
  (merge
   ds/named-schema
   {:commit-offsets-in-finalize            {:docstr "Finalized offsets are committed to Kafka."
                                            :action (fn [^KafkaIO$Read transform b]
                                                      (if b
                                                        (.commitOffsetsInFinalize transform)
                                                        transform))}
    :with-consumer-config-updates          {:docstr "Update configuration for the backend main consumer."
                                            :action (fn [^KafkaIO$Read transform config-map]
                                                      (.withConsumerConfigUpdates transform config-map))}
    :with-create-time                      {:docstr "Sets the timestamps policy based on `KafkaTimestampType.CREATE_TIME` timestamp of the records."
                                            :action (fn [^KafkaIO$Read transform max-delay] (.withCreateTime transform (Duration. max-delay)))}
    :with-log-append-time                  {:docstr "Sets TimestampPolicy to `TimestampPolicyFactory.LogAppendTimePolicy`."
                                            :action (fn [^KafkaIO$Read transform b]
                                                      (if b
                                                        (.withLogAppendTime transform)
                                                        transform))}
    :with-max-num-records                  {:docstr "Similar to `Read.Unbounded.withMaxNumRecords(long)`."
                                            :action (fn [^KafkaIO$Read transform ^Long max-num-records] (.withMaxNumRecords transform max-num-records))}
    :with-max-read-time                    {:docstr "Similar to `Read.Unbounded.withMaxReadTime(Duration)`."
                                            :action (fn [^KafkaIO$Read transform max-read-time] (.withMaxReadTime transform (Duration. max-read-time)))}
    :with-offset-consumer-config-overrides {:doctstr "Set additional configuration for the backend offset consumer."
                                            :action  (fn [^KafkaIO$Read transform offset-consumer-config]
                                                       (.withOffsetConsumerConfigOverrides transform offset-consumer-config))}
    :with-processing-time                  {:docstr "Sets TimestampPolicy to `TimestampPolicyFactory.ProcessingTimePolicy`."
                                            :action (fn [^KafkaIO$Read transform b]
                                                      (if b
                                                        (.withProcessingTime transform)
                                                        transform))}
    :with-read-committed                   {:docstr "Sets \" isolation_level \" to \" read_committed \" in Kafka consumer configuration."
                                            :action (fn [^KafkaIO$Read transform b]
                                                      (if b
                                                        (.withReadCommitted transform)
                                                        transform))}
    :with-start-read-time                  {:docstr "Provide custom `TimestampPolicyFactory  to set event times and watermark for each partition."
                                            :action (fn [^KafkaIO$Read transform start-read-time] (.withStartReadTime transform (Instant. start-read-time)))}
    :without-metadata                      {:docstr "Returns a PTransform for PCollection of KV, dropping Kafka metatdata."
                                            :action (fn [^KafkaIO$Read transform b]
                                                      (if b
                                                        (.withoutMetadata transform)
                                                        transform))}
    :with-topic-partitions                 {:docstr "Sets a list of partitions to read from. The list of partitions should be a collection of ['str-topic-name', int-partition-number]"
                                            :action (fn [^KafkaIO$Read transform topic-partitions]
                                                      (.withTopicPartitions transform topic-partitions))}}))

(defn- read-kafka-raw
  "Connects and reads form Kafka."
  [bootstrap-servers topic key-deserializer value-deserializer options p]
  (let [opts (assoc options :label :read-kafka-raw)
        ptrans (-> (KafkaIO/read)
                   (.withBootstrapServers bootstrap-servers)
                   (.withKeyDeserializerAndCoder key-deserializer (ds/make-nippy-coder))
                   (.withValueDeserializerAndCoder value-deserializer (ds/make-nippy-coder))
                   (cond->
                       (coll? topic) (.withTopics topic)
                       (string? topic) (.withTopic topic)))]
    (-> p
        (cond-> (instance? Pipeline p) (PBegin/in))
        (ds/apply-transform ptrans read-kafka-schema opts))))

(defn- read-kafka-clj-transform
  [bootstrap-servers topic key-deserializer value-deserializer options]
  (let [safe-opts (dissoc options :name)]
    (ds/ptransform
     :read-kafka-to-clj
     [^PCollection pcoll]
     (->> pcoll
          (read-kafka-raw bootstrap-servers topic key-deserializer value-deserializer safe-opts)
          (ds/dmap kafka-record->clj)))))

(defn read-kafka
  {:doc (ds/with-opts-docstr
          "Reads from a Kafka topic. Returns a `KafkaRecord` (https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/io/kafka/KafkaRecord.html) mapped to a clojure map

```
    {:payload   \"Deserialized with `value-deserializer`\"
     :key       \"Deserialized with `key-deserializer`\"
     :offset    \"...\"     
     :partition \"...\"
     :timestamp \"...\"   
     :topic     \"...\"   
     :headers   \"A map of `{key values}` of `Header` (https://www.javadoc.io/static/org.apache.kafka/kafka-clients/1.0.0/org/apache/kafka/common/header/Header.html)\"}
```

Examples:
```
(kafka/read-kafka \"broker-1:9092,broker-2:9092\" \"my-topic\" key-deserializer  value-deserializer options pcoll)
```

Using `StringDeserializer` from `org.apache.kafka.common.serialization` (https://kafka.apache.org/0102/javadoc/org/apache/kafka/common/serialization/StringDeserializer.html):
```
(kafka/read-kafka \"broker-1:9092,broker-2:9092\" \"my-topic\" StringDeserializer StringDeserializer {:name :read-from-kafka} pcoll)
```"
          read-kafka-schema)
   :added "0.6.7"}
  ([bootstrap-servers topic key-deserializer value-deserializer options p]
   (let [opts (assoc options :label :read-kafka)]
     (ds/apply-transform p (read-kafka-clj-transform bootstrap-servers topic key-deserializer value-deserializer options) ds/base-schema opts)))
  ([bootstrap-servers topic key-deserializer value-deserializer p]
   (read-kafka bootstrap-servers topic key-deserializer value-deserializer {} p)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;; Write ;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:no-doc write-kafka-schema
  (merge
   ds/named-schema
   {:values                       {:docstr "Writes just the values to Kafka."
                                   :action (fn [^KafkaIO$Write transform b]
                                             (if b
                                               (.values transform)
                                               transform))}
    :with-eos                     {:docstr "Wrapper method over `KafkaIO.WriteRecords.withEOS(int, String)`, used to keep the compatibility with old API based on KV type of element."
                                   :action (fn [^KafkaIO$Write transform num-shards sink-group-id]
                                             (.withEOS transform num-shards sink-group-id))}
    :with-input-timestamp         {:docstr "Wrapper method over `KafkaIO.WriteRecords.withInputTimestamp()`, used to keep the compatibility with old API based on KV type of element."
                                   :action (fn [^KafkaIO$Write transform b]
                                             (if b
                                               (.withInputTimestamp transform)
                                               transform))}
    :with-producer-config-updates {:docstr "Update configuration for the producer."
                                   :action (fn [^KafkaIO$Write transform config-map]
                                             (.withProducerConfigUpdates transform config-map))}}))

(defn- write-kafka-raw
  "Connects and writes to Kafka."
  [bootstrap-servers topic key-serializer value-serializer options p]
  (let [opts (assoc options :label :write-kafka-raw)
        ptrans (-> (KafkaIO/write)
                   (.withBootstrapServers bootstrap-servers)
                   (.withKeySerializer key-serializer)
                   (.withValueSerializer value-serializer)
                   (.withTopic topic))]
    (-> p
        (cond-> (instance? Pipeline p) (PBegin/in))
        (ds/apply-transform ptrans write-kafka-schema opts))))

(defn- write-kafka-clj-transform
  [bootstrap-servers topic key-serializer value-serializer options]
  (let [safe-opts (dissoc options :name)]
    (ds/ptransform
     :write-kafka-to-clj
     [^PCollection pcoll]
     (->> pcoll
          (write-kafka-raw bootstrap-servers topic key-serializer value-serializer safe-opts)))))

(defn write-kafka
  {:doc (ds/with-opts-docstr
          "Write to Kafka.

Examples:
```
(kafka/write-kafka \"broker-1:9092,broker-2:9092\" \"my-topic\" key-serializer  value-serializer options pcoll)
```"
          write-kafka-schema)
   :added "0.6.7"}
  ([bootstrap-servers topic key-serializer value-serializer options p]
   (let [opts (assoc options :label :write-kafka)]
     (ds/apply-transform p (write-kafka-clj-transform bootstrap-servers topic key-serializer value-serializer options) ds/base-schema opts)))
  ([bootstrap-servers topic key-serializer value-serializer p]
   (write-kafka bootstrap-servers topic key-serializer value-serializer {} p)))
