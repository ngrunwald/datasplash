(ns datasplash.bq
  (:require [cheshire.core :as json]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [datasplash.core :refer :all])
  (:import
   [org.codehaus.jackson.map.ObjectMapper]
   [com.google.api.services.bigquery.model
    TableRow TableFieldSchema TableSchema]
   [com.google.cloud.dataflow.sdk Pipeline]
   [com.google.cloud.dataflow.sdk.io
    BigQueryIO$Read BigQueryIO$Write
    BigQueryIO$Write$WriteDisposition
    BigQueryIO$Write$CreateDisposition]
   [com.google.cloud.dataflow.sdk.values PBegin PCollection]
   [com.google.cloud.dataflow.sdk.coders TableRowJsonCoder]))

(defn read-bq-raw
  [{:keys [query table] :as options} p]
  (let [opts (assoc options :label :read-bq-table-raw)
        ptrans (cond
                 query (BigQueryIO$Read/fromQuery query)
                 table (BigQueryIO$Read/from table)
                 :else (throw (ex-info
                               "Error with options of read-bq-table, should specify one of :table or :query"
                               {:options options})))]
    (-> p
        (cond-> (instance? Pipeline p) (PBegin/in))
        (apply-transform ptrans named-schema opts))))

(defn table-row->clj
  [^TableRow row]
  (let [keyset (.keySet row)]
    (persistent!
     (reduce
      (fn [acc k]
        (assoc! acc (keyword k) (.get row k)))
      (transient {}) keyset))))

(defn coerce-by-bq-val
  [v]
  (cond
    (instance? java.util.Date v) (int (/ (.getTime ^java.util.Date v) 1000))
    (set? v) (into '() v)
    (keyword? v) (name v)
    (symbol? v) (name v)
    :else v))

(defn clean-name
  [s]
  (let [test (number? s)]
    (-> s
        (cond-> test (str))
        (name)
        (str/replace #"-" "_")
        (str/replace #"\?" ""))))


(defn bqize-keys
  "Recursively transforms all map keys from strings to keywords."
  {:added "1.1"}
  [m]
  (let [f (fn [[k v]] (if (string? k) [(clean-name k) v] [k v]))]
    ;; only apply to maps
    (clojure.walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

(defn clj->table-row
  ^TableRow
  [hmap]
  (let [^TableRow row (TableRow.)]
    (doseq [[k v] hmap]
      (.set row (clean-name k) (coerce-by-bq-val v)))
    row))

(defn clj-nested->table-row
  ^TableRow
  [hmap]
  (let [clean-map (->> hmap
                       (clojure.walk/prewalk coerce-by-bq-val)
                       (bqize-keys))
        my-mapper (org.codehaus.jackson.map.ObjectMapper.)

        ^TableRow row (.readValue my-mapper (json/encode clean-map) TableRow)]
    row))

(defn- read-bq-clj-transform
  [options]
  (let [safe-opts (dissoc options :name)]
    (ptransform
     :read-bq-to-clj
     [pcoll]
     (->> pcoll
          (read-bq-raw safe-opts)
          (dmap table-row->clj safe-opts)))))

(defn read-bq
  [options ^Pipeline p]
  (let [opts (assoc options :label :read-bq-table)]
    (apply-transform p (read-bq-clj-transform opts) base-schema opts)))

(defn ->schema
  ^TableSchema
  ([defs transform-keys]
   (if (instance? TableSchema defs)
     defs
     (let [fields (for [{:keys [type mode] field-name :name} defs]
                    (-> (TableFieldSchema.)
                        (.setName (transform-keys (clean-name field-name)))
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
     (apply-transform pcoll (BigQueryIO$Write/to to) write-bq-table-schema opts)))
  ([to pcoll] (write-bq-table-raw to {} pcoll)))

(defn- write-bq-table-clj-transform
  [to options]
  (let [safe-opts (dissoc options :name)]
    (ptransform
     :write-bq-table-from-clj
     [^PCollection pcoll]
     (let [schema (:schema options)
           base-coll pcoll]
       (->> base-coll
            (dmap clj-nested->table-row (assoc safe-opts :coder (TableRowJsonCoder/of)))
            (write-bq-table-raw to safe-opts))))))

(defn write-bq-table
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-bq-table)]
     (apply-transform pcoll (write-bq-table-clj-transform to opts) named-schema opts)))
  ([to pcoll] (write-bq-table to {} pcoll)))
