(ns datasplash.core
  (:require [taoensso.nippy :as nippy :refer [thaw freeze]]
            [cognitect.transit :as transit])
  (:import [datasplash.fns ClojureDoFn]
           [java.io InputStream OutputStream]
           [java.util UUID]
           [com.google.cloud.dataflow.sdk.options PipelineOptionsFactory]
           [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.io TextIO$Read TextIO$Write]
           [com.google.cloud.dataflow.sdk.transforms DoFn DoFn$Context ParDo DoFnTester Create]
           [com.google.cloud.dataflow.sdk.values PCollection]
           [com.google.cloud.dataflow.sdk.coders ByteArrayCoder StringUtf8Coder CustomCoder Coder$Context]
           [datasplash.vals ClojureVal]
           [datasplash.coders ClojureCoder])
  (:gen-class))

(def ops-counter (atom 0))

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
        (transit/read rdr)))))

(defmacro with-opts
  [schema opts & body]
  (println opts)
  (let [full-name (name
                   (or (:name opts)
                       (do
                         (swap! ops-counter inc)
                         (str (name (get opts :label "cljfn")) "_" @ops-counter))))]
    `(let [transform# (do ~@body)]
       (reduce
        (fn [f# [k# apply#]]
          (if-let [v# (get (assoc ~opts :name ~full-name) k#)]
            (apply# f# v#)
            f#))
        transform# ~schema))))

(def base-schema
  {:name (fn [transform n] (.withName transform n))})

(defn map-op
  ([transform label coder]
   (fn
     [f options ^PCollection pcoll]
     (let [opts (assoc options :label label)]
       (-> pcoll
           (.apply (with-opts base-schema opts
                     (ParDo/of (dofn (transform f)))))
           (.setCoder (or (:coder opts) coder))))))
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

(defn make-pipeline
  [str-args]
  (let [builder (PipelineOptionsFactory/fromArgs
                 (into-array String str-args))
        options (.create builder)]
    (Pipeline/create options)))

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

(comment
  (compile 'datasplash.core))

(defn -main
  [& args]
  (let [p (make-pipeline args)
        final (->> p
                   (generate-input (range 10000) {:name "gengen"})
                   (dmap inc {:name "map"})
                   (dfilter even? {:name "filter"})
                   (to-edn {:name "edn"})
                   (write-text-file "tee"))]

    (.run p)))
