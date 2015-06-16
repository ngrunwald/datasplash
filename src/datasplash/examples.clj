(ns datasplash.examples
  (:require [clojure.string :as str]
            [datasplash
             [api :as ds]
             [dv :as dv]])
  (:gen-class))

(defn tokenize
  [l]
  (remove empty? (.split (str/trim l) "[^a-zA-Z']+")))

(defn run-word-count
  [str-args]
  (let [p (ds/make-pipeline str-args)
        proc (->> p
                  (ds/read-text-file "gs://dataflow-samples/shakespeare/kinglear.txt" {:name "King-Lear"})
                  (ds/mapcat tokenize {:name :tokenize})
                  (ds/frequencies)
                  (ds/map (fn [[k v]] (format "%s: %d" k v)) {:name :format-count})
                  (ds/write-text-file "gs://oscaro-dataflow/results/kinglear-freqs"))]
    p))

(defn -main
  [job & args]
  (compile 'datasplash.examples)
  (-> (case job
        "word-count" (run-word-count args))
      (.run)))
