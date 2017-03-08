(ns datasplash.bq-schema-test
  (:require
   [clojure.test :refer :all]
   [datasplash.bq :refer :all])
  (:import
   [com.google.api.services.bigquery.model TableSchema]))


(defn get-names-from-converted-schema
  [schema]
  (map #(.getName %) (.getFields schema)))

(defn get-record-fields-from-converted-schema
  [schema]
  (first (map #(.getFields %) (filter #(= (.getType %) "RECORD") (.getFields schema)))))

(deftest convert-clj-map-to-TableSchema
  (let [schema [{:name "a_float" :type "FLOAT" :mode "NULLABLE"}
                {:name "a_string" :type "STRING" :mode "NULLABLE"}
                {:name "a_record"
                 :type "RECORD"
                 :mode "REPEATED"
                 :fields
                 [{:name "first-name" :type "STRING" :mode "NULLABLE"}
                  {:name "last-name" :type "STRING" :mode "NULLABLE"}]}]
        converted-schema (->schema schema)]
    (println (filter #(= (.getType %) "RECORD") (.getFields converted-schema)))
    (is (instance? TableSchema converted-schema))
    (is (= (map :name schema) (get-names-from-converted-schema converted-schema)))
    (is (= 2 (count (get-record-fields-from-converted-schema converted-schema))))))
