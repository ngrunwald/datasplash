(ns datasplash.examples
  (:require [clojure.string :as str]
            [datasplash
             [api :as ds]
             [dv :as dv]])
  (:gen-class))

(defn tokenize
  [l]
  (remove empty? (.split (str/trim l) "[^a-zA-Z']+")))

(ds/defconfig Options
  {:input {:type :string
           :annotations [[:default-string "gs://dataflow-samples/shakespeare/kinglear.txt"]]}
   :output {:type :string
            :annotations [[:default-string "kinglear-freqs.txt"]]}})

(defn run-word-count
  [str-args]
  (let [p (ds/make-pipeline (Class/forName "Options") str-args)
        conf (bean (.getOptions p))]
    (->> p
         (ds/read-text-file (:input conf) {:name "King-Lear"})
         (ds/mapcat tokenize {:name :tokenize})
         (ds/frequencies)
         (ds/map (fn [[k v]] (format "%s: %d" k v)) {:name :format-count})
         (ds/write-text-file (:output conf)))))

(defn -main
  [job & args]
  (compile 'datasplash.examples)
  (-> (case job
        "word-count" (run-word-count args))
      (ds/run-pipeline)))
