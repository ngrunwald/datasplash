(ns datasplash.examples
  (:gen-class)
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [datasplash.api :as ds]
   [datasplash.bq :as bq]
   [datasplash.datastore :as dts]
   [datasplash.options :as options :refer [defoptions]]
   [datasplash.pubsub :as ps])
  (:import
   (com.google.datastore.v1 PropertyFilter$Operator Query)
   (com.google.datastore.v1.client DatastoreHelper)
   (java.util UUID)
   (org.apache.beam.sdk.options PipelineOptionsFactory)))

;;;;;;;;;;;;;;;
;; WordCount ;;
;;;;;;;;;;;;;;;

;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/WordCount.java

(defn tokenize
  [l]
  (remove empty? (.split (str/trim l) "[^a-zA-Z']+")))

(defn count-words
  [p]
  (ds/->> :count-words p
          (ds/mapcat tokenize {:name :tokenize})
          (ds/frequencies)))

(defn format-count
  [[k v]]
  (format "%s: %d" k v))

(defoptions WordCountOptions
  {:input {:default "gs://apache-beam-samples/shakespeare/kinglear.txt"
           :type String}
   :output {:default "kinglear-freqs.txt"
            :type String}
   :numShards {:default 0
               :type Long}})

(defn run-word-count
  [str-args]
  (let [p (ds/make-pipeline WordCountOptions str-args)
        {:keys [input output numShards]} (ds/get-pipeline-options p)]
    (->> p
         (ds/read-text-file input {:name "King-Lear"})
         (count-words)
         (ds/map format-count {:name :format-count})
         (ds/write-text-file output {:num-shards numShards}))))

;;;;;;;;;;;
;; DeDup ;;
;;;;;;;;;;;


;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/DeDupExample.java

(defoptions DeDupOptions
  {:input {:default "gs://apache-beam-samples/shakespeare/*"
           :type String}
   :output {:default "shakespeare-dedup.txt"
            :type String}})

(defn run-dedup
  [str-args]
  (let [p (ds/make-pipeline DeDupOptions str-args)
        {:keys [input output]} (ds/get-pipeline-options p)]
    (->> p
         (ds/read-text-file input {:name "ReadLines"})
         (ds/distinct {:name "dedup"})
         (ds/write-text-file output {:name "DedupedShakespeare"}))))

;;;;;;;;;;;;
;; Filter ;;
;;;;;;;;;;;;

;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/FilterExamples.java

(defoptions FilterOptions
  {:input {:default "clouddataflow-readonly:samples.weather_stations"
           :type String}
   :output {:default "youproject:yourdataset.weather_stations_new"
            :type String}
   :monthFilter {:default 7
                 :type String}})

(defn run-filter
  [str-args]
  (let [p (ds/make-pipeline FilterOptions str-args)
        {:keys [input output monthFilter]} (ds/get-pipeline-options p)
        all-rows (->> p
                      (bq/read-bq {:table input})
                      (ds/map (fn [row]
                                (->>
                                 (select-keys row [:year :month :day :mean_temp])
                                 (map (fn [[k v]] (if (string? v) [k (edn/read-string v)] [k v])))
                                 (into {})))
                              {:name "Projection"}))
        global-mean-temp (->> all-rows
                              (ds/combine (ds/mean-fn :mapper :mean_temp))
                              (ds/view))
        filtered-results (->> all-rows
                              (ds/filter (fn [{:keys [month]}] (= month monthFilter)))
                              (ds/filter (fn [{:keys [mean_temp]}]
                                           (let [gtemp (:global-mean (ds/side-inputs))]
                                             (< mean_temp gtemp)))
                                         {:name "ParseAndFilter" :side-inputs {:global-mean global-mean-temp}}))]
    (if (re-find #":[^/]" output)
      (bq/write-bq-table (bq/custom-output-fn (fn [x]
                                                (str output "_" (:year (.getValue x)))))
                         {:schema [{:name "year" :type "INTEGER"}
                                   {:name "month":type "INTEGER"}
                                   {:name "day"  :type "INTEGER"}
                                   {:name "mean_temp" :type "FLOAT"}]
                          :create-disposition :if-needed
                          :write-disposition :truncate}
                         filtered-results)
      (ds/write-edn-file output filtered-results))))

;;;;;;;;;;;;;;;;;;;
;; CombinePerKey ;;
;;;;;;;;;;;;;;;;;;;

;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/CombinePerKeyExamples.java

(defoptions CombinePerKeyOptions
  {:input {:default "publicdata:samples.shakespeare"
           :type String}
   :output {:default "combinePerKeyRes.edn"
            :type String}
   :minWordLength {:default 8
                   :type Long}})

(defn run-combine-per-key
  [str-args]
  (let [p (ds/make-pipeline CombinePerKeyOptions str-args)
        {:keys [input output minWordLength]} (ds/get-pipeline-options p)
        results (->> p
                     (bq/read-bq {:table input})
                     (ds/filter (fn [{:keys [word]}] (> (count word) minWordLength)))
                     (ds/map-kv (fn [{:keys [word corpus]}] [word corpus]))
                     (ds/combine
                      (ds/sfn (fn [words] (str/join "," words)))
                      {:scope :per-key})
                     (ds/map (fn [[word plays]] {:word word :all_plays plays})))]
    (if (re-find #":[^/]" output)
      (bq/write-bq-table output {:schema [{:name "word" :type "STRING"}
                                          {:name "all_plays" :type "STRING"}]
                                 :create-disposition :if-needed
                                 :write-disposition :truncate}
                         results)
      (ds/write-edn-file output results))))

;;;;;;;;;;;;;;;
;; MaxPerKey ;;
;;;;;;;;;;;;;;;

;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/MaxPerKeyExamples.java

(defoptions MaxPerKeyOptions
  {:input {:default "clouddataflow-readonly:samples.weather_stations"
           :type String}
   :output {:default "maxperKeyRes.edn"
            :type String}})

(defn run-max-per-key
  [str-args]
  (let [p (ds/make-pipeline MaxPerKeyOptions str-args)
        {:keys [input output]} (ds/get-pipeline-options p)
        results (->> p
                     (bq/read-bq {:table input})
                     (ds/map-kv (fn [{:keys [month mean_temp]}]
                                  [(edn/read-string month) (double mean_temp)]))
                     (ds/combine (ds/max-fn) {:scope :per-key})
                     (ds/map (fn [[k v]]
                               {:month k :max_mean_temp v})))]
    (if (re-find #":[^/]" output)
      (bq/write-bq-table output {:schema [{:name "month" :type "INTEGER"}
                                          {:name "max_mean_temp" :type "FLOAT"}]
                                 :create-disposition :if-needed
                                 :write-disposition :truncate}
                         results)
      (ds/write-edn-file output results))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; StandardSQL WordCount > 500;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Example showing how to enable support for StandardSQL in your queries querying for words in the
;; shakespeare dataset that has more than 500 words
;; Test calling lein run standard-sql --stagingLocation=gs://[your-bucket]/jars --output gs://[your-bucket]/

(def StandardSQLOptions
  {:input {:default "bigquery-public-data.samples.shakespeare"
           :type String}
   :output {:default "project:dataset.table"
            :type String}
   :tempLocation {:default "gs://yourbucket"
                  :type String}})

(defn run-standard-sql-query
  [str-args]
  ;; the DirectPipelineRunner doesn't support standardSql yet
  (let [p (ds/make-pipeline StandardSQLOptions str-args {:runner "DataflowRunner"})
        {:keys [_input output]} (ds/get-pipeline-options p)
        query "SELECT * from `bigquery-public-data.samples.shakespeare` LIMIT 100"
        results (->> p
                     (bq/read-bq {:query query
                                  :standard-sql? true}))]
    (ds/write-edn-file output results)))

;;;;;;;;;;;;;;;;;;;;;;;;
;; DatastoreWordCount ;;
;;;;;;;;;;;;;;;;;;;;;;;;

;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/DatastoreWordCount.java

(defoptions DatastoreWordCountOptions
  {:input {:default "gs://apache-beam-samples/shakespeare/kinglear.txt"
           :type String}
   :output {:default "kinglear-freqs.txt"
            :type String}
   :dataset {:default "yourdataset"
             :type String}
   :kind {:default "yourkind"
          :type String}
   :namespace {:default "yournamespace"
               :type String}
   :isReadOnly {:default false
                :type Boolean}
   :numShards {:default 0
               :type Long}})

(defn make-ancestor-key
  [{:keys [kind namespace]}]
  (dts/make-ds-key {:kind kind :namespace namespace :key "root"}))

;; Query is not wrapped yet, use Interop
;; PR welcome :)
(defn make-ancestor-kind-query
  [{:keys [kind namespace] :as opts}]
  (let [qb (Query/newBuilder)]
    (-> qb (.addKindBuilder) (.setName kind))
    (.setFilter qb (DatastoreHelper/makeFilter
                     "__key__"
                     (PropertyFilter$Operator/valueOf "HAS_ANCESTOR")
                     (dts/make-ds-value (make-ancestor-key opts))))
    (.build qb)))

(defn run-datastore-word-count
  [str-args]
  (let [p (ds/make-pipeline DatastoreWordCountOptions str-args)
        {:keys [input output dataset kind
                namespace isReadOnly numShards] :as opts} (ds/get-pipeline-options p)
        root (make-ancestor-key opts)]
    (when-not isReadOnly
      (->> p
           (ds/read-text-file input {:name "King-Lear"})
           (ds/map (fn [content]
                     (dts/make-ds-entity
                      {:content content}
                      {:namespace namespace
                       :key (-> (UUID/randomUUID) (.toString))
                       :kind kind
                       :path [root]}))
                   {:name "create-entities"})
           (dts/write-datastore-raw
            {:project-id dataset :name :write-datastore})))
    (->> p
         (dts/read-datastore-raw {:project-id dataset
                                  :query (make-ancestor-kind-query opts)
                                  :namespace namespace})
         (ds/map dts/entity->clj {:name "convert-clj"})
         (ds/map :content) {:name "get-content"}
         (count-words)
         (ds/map format-count {:name :format-count})
         (ds/write-text-file output {:num-shards numShards}))
    p))


;;;;;;;;;;;;;
;; Pub/Sub ;;
;;;;;;;;;;;;;

;; Run using: lein run pub-sub --pubsubProject=[your google cloud project] --tempLocation=gs:/[your-bucket]/tmp/ --stagingLocation=gs://[your-bucket]/jars
;; You must create the my-subscription and my-transformed-subscription subscriptions, and the my-transformed-topic topics
;; before you run this

(defoptions PubSubOptions
  {:pubsubProject {:default "yourproject"
                   :type String}})

(defn stream-interactions-from-pubsub
 [pipeline read-topic write-transformed-topic]
 (->> pipeline
      (ps/read-from-pubsub read-topic {:name "read-interactions-from-pubsub" :kind :topic})
      (ds/map (fn [message]
                (log/info (str "Got message:\n" message))
                (str/reverse message))
              {:name "log-message"})
      (ps/write-to-pubsub write-transformed-topic {:name "write-forwarded-interactions-to-pubsub"})))

(defn stream-forwarded-interactions-from-pubsub
 [pipeline read-transformed-subscription]
 (->> pipeline
      (ps/read-from-pubsub read-transformed-subscription {:name "read-transformed-interactions-from-pubsub"})
      (ds/map (fn [message]
                (log/info (str "Got transformed message:\n" message))
                message)
              {:name "log-transformed-message"})))


(defn run-pub-sub
  [str-args]
  (let [pipeline (ds/make-pipeline
                  PubSubOptions
                  str-args
                  {:runner "DataflowRunner"
                   :streaming true})
        {:keys [pubsubProject]} (ds/get-pipeline-options pipeline)
        read-topic (format "projects/%s/topics/my-topic" pubsubProject)
        write-transformed-topic (format "projects/%s/topics/my-transformed-topic" pubsubProject)
        read-transformed-subscription (format "projects/%s/subscriptions/my-transformed-subscription" pubsubProject)]
    (stream-interactions-from-pubsub pipeline read-topic write-transformed-topic)
    (stream-forwarded-interactions-from-pubsub pipeline read-transformed-subscription)))

;;;;;;;;;;
;; Main ;;
;;;;;;;;;;

(defn -main
  [job & args]
  (compile 'datasplash.examples)
  (some-> (cond
            (= job "word-count") (run-word-count args)
            (= job "dedup") (run-dedup args)
            (= job "filter") (run-filter args)
            (= job "combine-per-key") (run-combine-per-key args)
            (= job "max-per-key") (run-max-per-key args)
            (= job "standard-sql") (run-standard-sql-query args)
            (= job "datastore-word-count") (run-datastore-word-count args)
            (= job "pub-sub") (run-pub-sub args)
            (re-find #"help" job)
            (do
              (doseq [klass [WordCountOptions]]
                (PipelineOptionsFactory/register (Class/forName (name klass))))
              (-> (PipelineOptionsFactory/fromArgs
                   (into-array String (concat [job] args)))
                  (.create)
                  (.run))))
          (ds/run-pipeline)))
