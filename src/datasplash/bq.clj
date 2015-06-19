(ns datasplash.bq
  (:require [cheshire.core :as json]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [datasplash.core :refer :all])
  (:import [com.google.api.services.bigquery.model
            TableRow TableFieldSchema TableSchema]
           [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.io
            BigQueryIO$Read BigQueryIO$Write
            BigQueryIO$Write$WriteDisposition
            BigQueryIO$Write$CreateDisposition]
           [com.google.cloud.dataflow.sdk.transforms
            PTransform]
           [com.google.cloud.dataflow.sdk.values PBegin  PCollection]))

(defn read-bq-table-raw
  ([from options p]
   (let [opts (assoc options :label :read-bq-table-raw)]
     (-> p
         (cond-> (instance? Pipeline p) (PBegin/in))
         (.apply (with-opts base-schema opts
                   (BigQueryIO$Read/from from))))))
  ([from p] (read-bq-table-raw from {} p)))

(defn table-row->clj
  [^TableRow row]
  (let [keyset (.keySet row)]
    (persistent!
     (reduce
      (fn [acc k]
        (assoc! acc (keyword k) (.get row k)))
      (transient {}) keyset))))

(defn clj->table-row
  ^TableRow
  [hmap]
  (let [^TableRow tr (TableRow.)]
    (doseq [[k v] hmap]
      (.set tr (name k) v))
    tr))

(defn- read-bq-table-clj-transform
  [from options]
  (let [safe-opts (dissoc options :name)]
    (proxy [PTransform] []
      (apply [p]
        (->> p
             (read-bq-table-raw from options)
             (dmap table-row->clj options))))))

(defn read-bq-table
  ([from options ^Pipeline p]
   (let [opts (assoc options :label :read-bq-table)]
     (-> p
         (.apply (with-opts base-schema opts
                   (read-bq-table-clj-transform from opts))))))
  ([from p] (read-bq-table from {} p)))

(defn ->schema
  ^TableSchema
  ([defs transform-keys]
   (if (instance? TableSchema defs)
     defs
     (let [fields (for [{:keys [type mode] field-name :name} defs]
                    (-> (TableFieldSchema.)
                        (.setName (transform-keys field-name))
                        (.setType  (str/upper-case (name type)))
                        (cond-> mode (.setMode mode))))]
       (-> (TableSchema.) (.setFields fields)))))
  ([defs] (->schema defs (fn [k] (name k)))))

(defn get-bq-table-schema
  "Beware, uses bq util to get the schema!"
  [table-spec]
  (let [{:keys [exit out] :as return} (sh "bq" "--format=json" "show" (name table-spec))]
    (if (= 0 exit)
      (-> (json/decode out true) (:schema) (:fields))
      (throw (ex-info (str "Could not get bq table schema for table " table-spec)
                      {:table table-spec
                       :bq-return return})))))

(def write-disposition-enum
  {:append BigQueryIO$Write$WriteDisposition/WRITE_APPEND
   :empty BigQueryIO$Write$WriteDisposition/WRITE_EMPTY
   :truncate BigQueryIO$Write$WriteDisposition/WRITE_TRUNCATE})

(def create-disposition-enum
  {:if-needed BigQueryIO$Write$CreateDisposition/CREATE_IF_NEEDED
   :never BigQueryIO$Write$CreateDisposition/CREATE_NEVER})

(def write-bq-table-schema
  (merge
   base-schema
   {:schema {:docstr "Specifies bq schema."
             :action (fn [transform schema] (.withSchema transform (->schema schema)))}
    :write-disposition {:docstr "Choose write disposition."
                        :enum write-disposition-enum
                        :action (select-enum-option-fn
                                 :write-disposition
                                 write-disposition-enum
                                 (fn [transform enum] (.withWriteDisposition transform enum)))}
    :create-disposition {:docstr "Choose create disposition."
                         :enum create-disposition-enum
                         :action (select-enum-option-fn
                                  :create-disposition
                                  create-disposition-enum
                                  (fn [transform enum] (.withCreateDisposition transform enum)))}
    :without-validation {:docstr "Disables validation until runtime."
                         :action (fn [transform] (.withoutValidation transform))}}))

(defn write-bq-table-raw
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-bq-table-raw)]
     (-> pcoll
         (.apply (with-opts write-bq-table-schema opts
                   (BigQueryIO$Write/to to))))))
  ([to pcoll] (write-bq-table-raw to {} pcoll)))

(defn- write-bq-table-clj-transform
  [to options]
  (let [safe-opts (dissoc options :name)]
    (proxy [PTransform] []
      (apply [^PCollection pcoll]
        (->> pcoll
             (dmap clj->table-row options)
             (write-bq-table-raw to options))))))

(defn write-bq-table
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-bq-table)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (write-bq-table-clj-transform to opts))))))
  ([to pcoll] (write-bq-table to {} pcoll)))
