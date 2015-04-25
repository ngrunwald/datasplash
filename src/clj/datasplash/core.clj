(ns datasplash.core
  (:require [taoensso.nippy :as nippy :refer [thaw freeze]]
            [cognitect.transit :as transit])
  (:import [datasplash.fns ClojureDoFn ClojureSerializableFn]
           [java.io InputStream OutputStream]
           [java.util UUID]
           [com.google.cloud.dataflow.sdk.options PipelineOptionsFactory]
           [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.io TextIO$Read TextIO$Write]
           [com.google.cloud.dataflow.sdk.transforms
            DoFn DoFn$Context ParDo DoFnTester Create PTransform
            SerializableFunction WithKeys GroupByKey]
           [com.google.cloud.dataflow.sdk.values PCollection]
           [com.google.cloud.dataflow.sdk.coders ByteArrayCoder StringUtf8Coder CustomCoder Coder$Context]
           [datasplash.vals ClojureVal]
           [datasplash.coders ClojureCoder])
  (:gen-class))

(def ops-counter (atom {}))

(defn dofn
  [f & {:keys [start-bundle finish-bundle] :as opts}]
  (ClojureDoFn. f))

(defn map-fn
  [f]
  (fn [^DoFn$Context c]
    (let [elt (.element c)
          result (f elt)]
      (.output c result))))

(defn mapcat-fn
  [f]
  (fn [^DoFn$Context c]
    (let [elt (.element c)
          result (f elt)]
      (doseq [atm result]
        (.output c atm)))))

(defn filter-fn
  [f]
  (fn [^DoFn$Context c]
    (let [elt (.element c)
          result (f elt)]
      (when result
        (.output c elt)))))

(defn make-transit-coder
  []
  (proxy [CustomCoder] []
    (encode [obj ^OutputStream out ^Coder$Context context]
      (require '[cognitect.transit :as transit])
      (let [wrt (transit/writer out :msgpack)]
        (transit/write wrt obj)))
    (decode [^InputStream in ^Coder$Context context]
      (require '[cognitect.transit :as transit])
      (let [rdr (transit/reader in :msgpack)]
        (transit/read rdr)))
    (verifyDeterministic [] nil)
    (consistentWithEquals [] true)))

(defmacro with-opts
  [schema opts & body]
  `(let [transform# (do ~@body)
         full-name# (name
                     (or (:name ~opts)
                         (let [label# (name (get ~opts :label "cljfn"))
                               new-idx# (get
                                         (swap! ~'ops-counter update-in [label#]
                                                (fn [i#] (if i# (inc i#) 1)))
                                         label#)]
                           (str (name (get ~opts :label "cljfn")) "_"  new-idx#))))]
     (reduce
      (fn [f# [k# apply#]]
        (if-let [v# (get (assoc ~opts :name full-name#) k#)]
          (apply# f# v#)
          f#))
      transform# ~schema)))

(def base-schema
  {:name (fn [transform n] (.withName transform n))})

(defn map-op
  ([transform label coder]
   (fn make-map-op
     ([f options ^PCollection pcoll]
      (let [opts (assoc options :label label)]
        (-> pcoll
            (.apply (with-opts base-schema opts
                      (ParDo/of (dofn (transform f)))))
            (.setCoder (or (:coder opts) coder)))))
     ([f pcoll] (make-map-op f {} pcoll))))
  ([transform label]
   (map-op transform label (make-transit-coder))))

(def dmap (map-op map-fn :map))
(def dmapcat (map-op mapcat-fn :mapcat))
(def dfilter (map-op filter-fn :filter))

(defn generate-input
  [coll options ^Pipeline p]
  (let [opts (assoc options :label :generate-input)]
    (-> p
        (.apply (with-opts base-schema opts
                  (Create/of (seq coll))))
        (.setCoder (or (:coder opts) (make-transit-coder))))))

(defn- to-edn*
  [^DoFn$Context c]
  (let [elt (.element c)
        result (pr-str elt)]
    (.output c result)))

(def to-edn (partial (map-op identity :to-edn (StringUtf8Coder/of)) to-edn*))

(defn sfn
  ^SerializableFunction [f]
  (ClojureSerializableFn. f))

(defn with-keys
  ([f options ^PCollection pcoll]
   (let [opts (assoc options :label :with-keys)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (WithKeys/of (sfn f)))))))
  ([f pcoll] (with-keys f {} pcoll)))

(defn group-by-key
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :group-by-keys)]
     (-> pcoll
         (.apply (GroupByKey/create)))))
  ([pcoll] (group-by-key {} pcoll)))

(defn- group-by-transform
  [f options]
  (let [safe-opts (dissoc options :name)]
    (proxy [PTransform] []
      (apply [^PCollection pcoll]
        (->> pcoll
             (with-keys f safe-opts)
             (group-by-key safe-opts))))))

(defn dgroup-by
  ([f options ^PCollection pcoll]
   (let [opts (assoc options :label :group-by)]
     (.apply pcoll (with-opts base-schema opts
                     (group-by-transform f options)))))
  ([f pcoll] (dgroup-by f {} pcoll)))

(defn make-pipeline
  [str-args]
  (let [builder (PipelineOptionsFactory/fromArgs
                 (into-array String str-args))
        options (.create builder)
        pipeline (Pipeline/create options)
        coder-registry (.getCoderRegistry pipeline)]
    (doto coder-registry
      (.registerCoder clojure.lang.IPersistentCollection (make-transit-coder))
      (.registerCoder clojure.lang.Keyword (make-transit-coder)))
    pipeline))

(defn load-text-file
  [from options ^Pipeline p]
  (let [opts (assoc options :label :load-text-file)]
    (-> p
        (.apply (with-opts base-schema opts
                  (TextIO$Read/from from)))
        (.setCoder (StringUtf8Coder/of)))))

(defn write-text-file
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-text-file)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (TextIO$Write/to to))))))
  ([to pcoll] (write-text-file to {} pcoll)))

(defn- write-edn-file-transform
  [to options]
  (let [safe-opts (dissoc options :name)]
    (proxy [PTransform] []
      (apply [^PCollection pcoll]
        (->> pcoll
             (to-edn)
             (write-text-file to options))))))

(defn write-edn-file
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-edn-file)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (write-edn-file-transform to opts))))))
  ([to pcoll] (write-edn-file to {} pcoll)))

(comment
  (compile 'datasplash.core))

(defn -main
  [& args]
  (let [p (make-pipeline args)
        final (->> p
                   (generate-input [{:key :a :val 10} {:key :b :val 5} {:key :a :val 42}] {:name "gengen"})
                   (dgroup-by :key {:name :group-by})
                   (dmap (fn [kv] (vector (.getKey kv) (seq (.getValue kv)))) {:name :normalize})
                   (write-edn-file "gs://oscaro-test-dataflow/results/group-by.edn" {:name :output-edn}))]

    (.run p)))
