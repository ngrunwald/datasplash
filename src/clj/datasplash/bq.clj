(ns datasplash.bq
  (:require [cheshire.core :as json]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clj-time [coerce :as tc]
             [format :as tf]]
            [clojure.tools.logging :as log]
            [datasplash.core :refer :all])
  (:import
   [org.codehaus.jackson.map.ObjectMapper]
   [com.google.api.services.bigquery.model
    TableRow TableFieldSchema TableSchema]
   [org.apache.beam.sdk.transforms SerializableFunction]
   [org.apache.beam.sdk Pipeline]
   [org.apache.beam.sdk.io.gcp.bigquery
    BigQueryIO BigQueryIO$Read
    BigQueryIO$Write$WriteDisposition
    BigQueryIO$Write$CreateDisposition TableRowJsonCoder TableDestination InsertRetryPolicy]
   [org.apache.beam.sdk.values PBegin PCollection]))

(defn read-bq-raw
  [{:keys [query table standard-sql?] :as options} p]
  (let [opts (assoc options :label :read-bq-table-raw)
        ptrans (cond
                 query (.fromQuery  (BigQueryIO/readTableRows)  query)
                 table (.from  (BigQueryIO/readTableRows) table)
                 :else (throw (ex-info
                               "Error with options of read-bq-table, should specify one of :table or :query"
                               {:options options})))]
    (-> p
        (cond-> (instance? Pipeline p) (PBegin/in))
        (apply-transform
         (if (and standard-sql? query)
           (.usingStandardSql ptrans)
           ptrans)
         named-schema opts))))

(defn auto-parse-val
  [v]
  (cond
    (and (string? v) (re-find #"^\d+$" v)) (Long/parseLong v)
    :else v))

(defn table-row->clj
  ([{:keys [auto-parse]} ^TableRow row]
   (let [keyset (.keySet row)]
     (persistent!
      (reduce
       (fn [acc k]
         (assoc! acc (keyword k)
                 (let [raw-v (get row k)]
                   (cond
                     (instance? java.util.List raw-v) (if (instance? java.util.AbstractMap (first raw-v))
                                                        (map #(table-row->clj {:auto-parse auto-parse} %) raw-v)
                                                        (map #(if auto-parse (auto-parse-val %) %) raw-v))
                     (instance? java.util.AbstractMap raw-v) (table-row->clj {:auto-parse auto-parse} raw-v)
                     :else (if auto-parse (auto-parse-val raw-v) raw-v)))))
       (transient {}) keyset))))
  ([row] (table-row->clj {} row)))

(defn coerce-by-bq-val
  [v]
  (cond
    (instance? java.util.Date v) (try (->> (tc/from-long (.getTime v))
                                           (tf/unparse (tf/formatter "yyyy-MM-dd HH:mm:ss")))
                                      (catch Exception e (log/errorf "error when parsing date %s" v)))
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
  (let [f (fn [[k v]] [(clean-name k) v])]
    ;; only apply to maps
    (clojure.walk/postwalk (fn [x] (if (map? x)
                                     (persistent!
                                      (reduce
                                       (fn [acc [k v]]
                                         (assoc! acc (clean-name k) v))
                                       (transient {}) x))
                                     x))
                           m)))

(defn ^TableRow clj->table-row
  [hmap]
  (let [^TableRow row (TableRow.)]
    (doseq [[k v] hmap]
      (.set row (clean-name k) (coerce-by-bq-val v)))
    row))

(defn ^TableRow clj-nested->table-row
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
          (dmap (partial table-row->clj safe-opts) safe-opts)))))

(defn read-bq
  [options ^Pipeline p]
  (let [opts (assoc options :label :read-bq-table)]
    (apply-transform p (read-bq-clj-transform opts) base-schema opts)))

(defn- clj->TableFieldSchema
  [defs transform-keys]
  (for [{:keys [type mode] field-name :name nested-fields :fields} defs]
       (-> (TableFieldSchema.)
           (.setName (transform-keys (clean-name field-name)))
           (.setType  (str/upper-case (name type)))
           (cond-> mode (.setMode mode))
           (cond-> nested-fields (.setFields (clj->TableFieldSchema nested-fields transform-keys))))))

(defn ^TableSchema ->schema
  ([defs transform-keys]
   (if (instance? TableSchema defs)
     defs
     (let [fields (clj->TableFieldSchema defs transform-keys)]
       (-> (TableSchema.) (.setFields fields)))))
  ([defs] (->schema defs name)))

(defn get-bq-table-schema
  "Beware, uses bq util to get the schema!"
  [table-spec]
  (let [{:keys [exit out] :as return} (sh "bq" "--format=json" "show" (name table-spec))]
    (if (zero? exit)
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
(def retry-policy-enum
  {:never (InsertRetryPolicy/neverRetry)
   :always (InsertRetryPolicy/alwaysRetry)
   :retry-transient (InsertRetryPolicy/retryTransientErrors)})

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
                         :action (fn [transform] (.withoutValidation transform))}
    :retry-policy {:docstr "Specify retry policy for failed insert in streaming"
                   :action (select-enum-option-fn
                            :retry-policy
                            retry-policy-enum
                            (fn [transform retrypolicy] (.withFailedInsertRetryPolicy transform retrypolicy)))}}))

(defn custom-output-fn [cust-fn]
  (sfn (fn [elt]
         (let [^String out (cust-fn elt)]
           (TableDestination. out nil)))))

(def format-fn (sfn clj-nested->table-row))

(defn write-bq-table-raw
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-bq-table-raw)]
     (apply-transform pcoll (-> (BigQueryIO/write)
                                (.to to)
                                (.withFormatFunction format-fn))
                      write-bq-table-schema opts)))
  ([to pcoll] (write-bq-table-raw to {} pcoll)))

(defn- write-bq-table-clj-transform
  [to options]
  (let [safe-opts (dissoc options :name)]
    (ptransform
     :write-bq-table-from-clj
     [^PCollection pcoll]
     (let [schema (:schema options)
           base-coll pcoll]
       (write-bq-table-raw to safe-opts base-coll)))))

(defn write-bq-table
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-bq-table)]
     (apply-transform pcoll (write-bq-table-clj-transform to opts) named-schema opts)))
  ([to pcoll] (write-bq-table to {} pcoll)))
