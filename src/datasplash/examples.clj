(ns datasplash.examples
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [datasplash
             [api :as ds]
             [bq :as bq]
             [datastore :as dts]
             [pubsub :as ps]]
            [clojure.edn :as edn])
  (:import [java.util UUID]
           [com.google.datastore.v1 Query PropertyFilter$Operator]
           [com.google.datastore.v1.client DatastoreHelper])
  (:gen-class))

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

(ds/defoptions WordCountOptions
  {:input {:type String
           :default "gs://dataflow-samples/shakespeare/kinglear.txt"
           :description "Path of the file to read from"}
   :output {:type String
            :default "kinglear-freqs.txt"
            :description "Path of the file to write to"}
   :numShards {:type Long
               :description "Number of output shards (0 if the system should choose automatically)"
               :default 0}})

(defn run-word-count
  [str-args]
  (let [p (ds/make-pipeline 'WordCountOptions str-args)
        {:keys [input output numShards]} (ds/get-pipeline-configuration p)]
    (->> p
         (ds/read-text-file input {:name "King-Lear"})
         (count-words)
         (ds/map format-count {:name :format-count})
         (ds/write-text-file output {:num-shards numShards}))))

;;;;;;;;;;;
;; DeDup ;;
;;;;;;;;;;;


;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/DeDupExample.java

(ds/defoptions DeDupOptions
  {:input {:type String
           :default "gs://dataflow-samples/shakespeare/*"
           :description "Path of the file to read from"}
   :output {:type String
            :default "shakespeare-dedup.txt"
            :description "Path of the file to write to"}})

(defn run-dedup
  [str-args]
  (let [p (ds/make-pipeline 'DeDupOptions str-args)
        {:keys [input output]} (ds/get-pipeline-configuration p)]
    (->> p
         (ds/read-text-file input {:name "ReadLines"})
         (ds/distinct {:name "dedup"})
         (ds/write-text-file output {:name "DedupedShakespeare"}))))

;;;;;;;;;;;;
;; Filter ;;
;;;;;;;;;;;;

;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/FilterExamples.java

(ds/defoptions FilterOptions
  {:input {:type String
           :default "clouddataflow-readonly:samples.weather_stations"
           :description "Table to read from, specified as <project_id>:<dataset_id>.<table_id>"}
   :output {:type String
            :default "filterRes.edn"
            :description "Table to write to, specified as <project_id>:<dataset_id>.<table_id>. The dataset_id must already exist. If given a path, writes to edn."}
   :monthFilter {:type Long
                 :default 7
                 :description "Numeric value of month to filter on"}})

(defn run-filter
  [str-args]
  (let [p (ds/make-pipeline 'FilterOptions str-args)
        {:keys [input output monthFilter]} (ds/get-pipeline-configuration p)
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
      (bq/write-bq-table output {:schema [{:name "year" :type "INTEGER"}
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

(ds/defoptions CombinePerKeyOptions
  {:input {:type String
           :default "publicdata:samples.shakespeare"
           :description "Table to read from, specified as <project_id>:<dataset_id>.<table_id>"}
   :output {:type String
            :default "combinePerKeyRes.edn"
            :description "Table to write to, specified as <project_id>:<dataset_id>.<table_id>. The dataset_id must already exist. If given a path, writes to edn."}
   :minWordLength {:type Long
                   :default 8
                   :description "Minimum word length."}})

(defn run-combine-per-key
  [str-args]
  (let [p (ds/make-pipeline 'CombinePerKeyOptions str-args)
        {:keys [input output minWordLength]} (ds/get-pipeline-configuration p)
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

(ds/defoptions MaxPerKeyOptions
  {:input {:type String
           :default "clouddataflow-readonly:samples.weather_stations"
           :description "Table to read from, specified as <project_id>:<dataset_id>.<table_id>"}
   :output {:type String
            :default "maxperKeyRes.edn"
            :description "Table to write to, specified as <project_id>:<dataset_id>.<table_id>. The dataset_id must already exist. If given a path, writes to edn."}})

(defn run-max-per-key
  [str-args]
  (let [p (ds/make-pipeline 'MaxPerKeyOptions str-args)
        {:keys [input output]} (ds/get-pipeline-configuration p)
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
;; Test calling lein run standard-sql --usingStandardSql=true --stagingLocation=gs://[your-bucket]/jars

(ds/defoptions StandardSQLOptions
  {:input {:type String
           :default "bigquery-public-data.samples.shakespeare"
           :description "Table to read from, specified as <project_id>:<dataset_id>.<table_id>"}
   :output {:type String
            :default "standardSql.edn"
            :description "File to write the result to"}
   :tempLocation {:type String
                     :description "Google Cloud Storage where BigQuery.Read stage local files."}})

(defn run-standard-sql-query
  [str-args]
  (let [p (ds/make-pipeline
           'StandardSQLOptions
           str-args
           {:runner "DataflowPipelineRunner"})  ;; the DirectPipelineRunner doesn't support standardSql yet 
        {:keys [input output usingStandardSql]} (ds/get-pipeline-configuration p)
        query "SELECT * from `bigquery-public-data.samples.shakespeare` LIMIT 100"
        results (->> p
                     (bq/read-bq {:query query
                                  :usingStandardSql true}))] ;; the usingStandardSql is passed to bq/read-bq
      (ds/write-edn-file output results)))

;;;;;;;;;;;;;;;;;;;;;;;;
;; DatastoreWordCount ;;
;;;;;;;;;;;;;;;;;;;;;;;;

;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/cookbook/DatastoreWordCount.java

(ds/defoptions DatastoreWordCountOptions
  {:input {:type String
           :default "gs://dataflow-samples/shakespeare/kinglear.txt"
           :description "Path of the file to read from"}
   :output {:type String
            :default "kinglear-freqs.txt"
            :description "Path of the file to write to"}
   :dataset {:type String
             :description "Dataset ID to read from Cloud Datastore"}
   :kind {:type String
          :description "Cloud Datastore Entity Kind"}
   :namespace {:type String
               :description "Dataset Namespace"}
   :isReadOnly {:type Boolean
                :description "Read an existing dataset, do not write first"}
   :numShards {:type Long
               :description "Number of output shards"
               :default 0}})

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
  (let [p (ds/make-pipeline 'DatastoreWordCountOptions str-args)
        {:keys [input output dataset kind
                namespace isReadOnly numShards] :as opts} (ds/get-pipeline-configuration p)
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

;; Run using: lein run pub-sub --project=[your google cloud project] --stagingLocation=gs://[your-bucket]/jars
;; You must create the my-subscription and my-transformed-subscription subscriptions, and the my-transformed-topic topics
;; before you run this

(ds/defoptions PubSubOptions
   {:project {:type String
              :description "Google Cloud Project where your PubSub runs."}
    :stagingLocation {:type String
                      :description "Google Cloud Storage to stage local files."}})

(defn stream-interactions-from-pubsub
 [pipeline read-subscription write-transformed-topic]
 (->> pipeline
      (ps/read-from-pubsub read-subscription {:name "read-interactions-from-pubsub"})
      (ds/map (fn [message]
                (do
                  (log/info (str "Got message:\n" message))
                  (str/reverse message))) {:name "log-message"})
      (ps/write-to-pubsub write-transformed-topic {:name "write-forwarded-interactions-to-pubsub"})))

(defn stream-forwarded-interactions-from-pubsub
 [pipeline read-transformed-subscription]
 (->> pipeline
      (ps/read-from-pubsub read-transformed-subscription {:name "read-transformed-interactions-from-pubsub"})
      (ds/map (fn [message]
                (do
                  (log/info (str "Got transformed message:\n" message))
                  message)) {:name "log-transformed-message"})))


(defn run-pub-sub
  [str-args]
  (let [pipeline (ds/make-pipeline
                  'PubSubOptions
                  str-args
                  {:runner "DataflowPipelineRunner"
                   :streaming true})
        {:keys [project]} (ds/get-pipeline-configuration pipeline)
        read-subscription (format "projects/%s/subscriptions/my-subscription" project)
        write-transformed-topic (format "projects/%s/topics/my-transformed-topic" project)
        read-transformed-subscription (format "projects/%s/subscriptions/my-transformed-subscription" project)]
    (stream-interactions-from-pubsub pipeline read-subscription write-transformed-topic)
    (stream-forwarded-interactions-from-pubsub pipeline read-transformed-subscription)))

;;;;;;;;;;
;; Main ;;
;;;;;;;;;;

(defn -main
  [job & args]
  (compile 'datasplash.examples)
  (-> (case job
        "word-count" (run-word-count args)
        "dedup" (run-dedup args)
        "filter" (run-filter args)
        "combine-per-key" (run-combine-per-key args)
        "max-per-key" (run-max-per-key args)
        "standard-sql" (run-standard-sql-query args)
        "datastore-word-count" (run-datastore-word-count args)
        "pub-sub" (run-pub-sub args))
      (ds/run-pipeline)))
