(ns datasplash.core
  (:require [taoensso.nippy :as nippy :refer [thaw freeze]])
  (:import [datasplash.fns ClojureDoFn]
           [com.google.cloud.dataflow.sdk.options PipelineOptionsFactory]
           [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.io TextIO$Read]
           [com.google.cloud.dataflow.sdk.transforms DoFn DoFn$Context ParDo DoFnTester Create]
           [com.google.cloud.dataflow.sdk.values PCollection]
           [com.google.cloud.dataflow.sdk.coders ByteArrayCoder]
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
  [f pcoll]
  (.apply pcoll (ParDo/of (dofn (map-fn f)))))

(defn generate-input
  [p coll]
  (-> p
      (.apply (Create/of (map #(ClojureVal. %) coll)))
      (.setCoder (ClojureCoder/of))))

(comment
  (compile 'datasplash.core))
