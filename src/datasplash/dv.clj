(ns datasplash.dv)

(def ^{:dynamic true :no-doc true} *coerce-to-clj* true)
(def ^{:dynamic true :added "0.1.0"
       :doc "In the context of a ParDo, contains the corresponding Context object.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/DoFn.ProcessContext.html"}
  *context* nil)
(def ^{:dynamic true :added "0.1.0"
       :doc "In the context of a ParDo, contains the corresponding side inputs as a map from names to values.
  Example:
    (let [input (ds/generate-input [1 2 3 4 5] p)
          side-input (ds/view (ds/generate-input [{1 :a 2 :b 3 :c 4 :d 5 :e}] p))
          proc (ds/map (fn [x] (get (:mapping dv/*side-inputs*) x))
                       {:side-inputs {:mapping side-input}} input)])"}
  *side-inputs* {})
