(ns datasplash.examples
  (:require [clojure.string :as str]
            [datasplash
             [api :as ds]])
  (:gen-class))

;;;;;;;;;;;;;;;
;; WordCount ;;
;;;;;;;;;;;;;;;

;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/WordCount.java

(defn tokenize
  [l]
  (remove empty? (.split (str/trim l) "[^a-zA-Z']+")))

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
         (ds/mapcat tokenize {:name :tokenize})
         (ds/frequencies)
         (ds/map (fn [[k v]] (format "%s: %d" k v)) {:name :format-count})
         (ds/write-text-file output {:num-shards numShards}))))

;;;;;;;;;;;
;; DeDup ;;
;;;;;;;;;;;

;; Port of https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/src/main/java/com/google/cloud/dataflow/examples/DeDupExample.java

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


;;;;;;;;;;
;; Main ;;
;;;;;;;;;;

(defn -main
  [job & args]
  (compile 'datasplash.examples)
  (-> (case job
        "word-count" (run-word-count args)
        "dedup" (run-dedup args))
      (ds/run-pipeline)))
