(ns datasplash.examples
  (:require [clojure.string :as str]
            [datasplash
             [api :as ds]
             [dv :as dv]])
  (:gen-class))

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
        conf (dissoc (bean (.getOptions p)) :class)]
    (println "CONF" conf)
    (->> p
         (ds/read-text-file (:input conf) {:name "King-Lear"})
         (ds/mapcat tokenize {:name :tokenize})
         (ds/frequencies)
         (ds/map (fn [[k v]] (format "%s: %d" k v)) {:name :format-count})
         (ds/write-text-file (:output conf) {:num-shards (:numShards conf)}))))

(defn -main
  [job & args]
  (compile 'datasplash.examples)
  (-> (case job
        "word-count" (run-word-count args))
      (ds/run-pipeline)))
