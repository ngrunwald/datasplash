(ns datasplash.core
  (:require [taoensso.nippy :as nippy :refer [thaw freeze]]
            [cognitect.transit :as transit])
  (:import [datasplash.fns ClojureDoFn]
           [java.io InputStream OutputStream]
           [com.google.cloud.dataflow.sdk.options PipelineOptionsFactory]
           [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.io TextIO$Read TextIO$Write]
           [com.google.cloud.dataflow.sdk.transforms DoFn DoFn$Context ParDo DoFnTester Create]
           [com.google.cloud.dataflow.sdk.values PCollection]
           [com.google.cloud.dataflow.sdk.coders ByteArrayCoder StringUtf8Coder CustomCoder Coder$Context]
           [datasplash.vals ClojureVal]
           [datasplash.coders ClojureCoder])
  (:gen-class))

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
  `(let [pcoll# (do ~@body)]
     (reduce
      (fn [pc# [k# f#]]
        (if-let [v# (get ~opts k#)]
          (f# pc# v#)
          pc#))
      pcoll# ~schema)))

(def base-schema
  {:named (fn [pcoll n] (.setName pcoll n))
   :coder (fn [pcoll coder] (.setCoder pcoll coder))})

(defn map-op
  ([transform coder]
   (fn
     [f opts ^PCollection pcoll]
     (with-opts base-schema opts
       (-> pcoll
           (.apply (ParDo/of (dofn (transform f))))
           (.setCoder coder)))))
  ([transform]
   (map-op transform (make-transit-coder))))

(def dmap (map-op map-fn))
(def dmapcat (map-op mapcat-fn))
(def dfilter (map-op filter-fn))

(defn generate-input
  [coll opts ^Pipeline p]
  (with-opts base-schema opts
    (-> p
        (.apply (Create/of (seq coll)))
        (.setCoder (make-transit-coder)))))

(defn- to-edn*
  [^DoFn$Context c]
  (let [^ClojureVal elt (.element c)
        result (pr-str elt)]
    (.output c result)))

(def to-edn (partial (map-op identity (StringUtf8Coder/of)) to-edn*))

(defn to-edn
  ([opts ^PCollection pcoll]
   (with-opts base-schema opts
     (-> pcoll
         (.apply (ParDo/of (dofn to-edn*)))
         (.setCoder (StringUtf8Coder/of)))))
  ([pcoll] (to-edn {} pcoll)))

(defn make-pipeline
  [str-args]
  (let [builder (PipelineOptionsFactory/fromArgs (into-array String str-args))
        options (.create builder)]
    (Pipeline/create options)))

(defn load-text-file
  [from opts ^Pipeline p]
  (-> p
      (.apply (TextIO$Read/from from))
      (.setCoder (StringUtf8Coder/of))))

(defn write-text-file
  ([to opts ^PCollection pcoll]
   (-> pcoll
       (.apply (-> (TextIO$Write/to to)))))
  ([to pcoll] (write-text-file to {} pcoll)))

(comment
  (compile 'datasplash.core))

(defn -main
  [& args]
  (let [p (make-pipeline args)
        final (->> p
                   (generate-input (range 10000) {:named "gengen"})
                   (dmap inc {:named "map"})
                   (dfilter even? {:named "filter"})
                   (to-edn {:named "edn"})
                   (write-text-file "tee"))]

    (.run p)))
