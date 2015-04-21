(ns datasplash.core
  (:require [taoensso.nippy :as nippy :refer [thaw freeze]])
  (:import [datasplash.fns ClojureDoFn]
           [com.google.cloud.dataflow.sdk.options PipelineOptionsFactory]
           [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.io TextIO$Read TextIO$Write]
           [com.google.cloud.dataflow.sdk.transforms DoFn DoFn$Context ParDo DoFnTester Create]
           [com.google.cloud.dataflow.sdk.values PCollection]
           [com.google.cloud.dataflow.sdk.coders ByteArrayCoder StringUtf8Coder]
           [datasplash.vals ClojureVal]
           [datasplash.coders ClojureCoder])
  (:gen-class))

(defn dofn
  [f & {:keys [start-bundle finish-bundle] :as opts}]
  (ClojureDoFn. f))

(defn map-fn
  [f]
  (fn [^DoFn$Context c]
    (let [^ClojureVal elt (.element c)
          value (.getValue elt)
          result (f value)]
      (.output c (ClojureVal. result)))))

(defn dmap
  [f ^PCollection pcoll]
  (-> pcoll
      (.apply (ParDo/of (dofn (map-fn f))))
      (.setCoder (ClojureCoder/of))))

(defn generate-input
  [coll p]
  (-> p
      (.apply (Create/of (map #(ClojureVal. %) coll)))
      (.setCoder (ClojureCoder/of))))

(defn- to-edn*
  [^DoFn$Context c]
  (let [^ClojureVal elt (.element c)
        value (.getValue elt)
        result (pr-str value)]
    (.output c result)))

(defn to-edn
  [^PCollection pcoll]
  (-> pcoll
      (.apply (ParDo/of (dofn to-edn*)))
      (.setCoder (StringUtf8Coder/of))))

(defn make-pipeline
  [str-args]
  (let [builder (PipelineOptionsFactory/fromArgs (into-array String str-args))
        options (.create builder)]
    (Pipeline/create options)))

(defn write-file
  ([to opts ^PCollection pcoll]
   (-> pcoll
       (.apply (-> (TextIO$Write/to to)))))
  ([to pcoll] (write-file to {} pcoll)))

(comment
  (compile 'datasplash.core))

(defn -main
  [& args]
  (let [p (make-pipeline args)
        final (->> p
                   (generate-input [1 2 3])
                   (dmap inc)
                   (to-edn)
                   (write-file "ptest"))]

    (.run p)))
