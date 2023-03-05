(ns datasplash.testing
  "Namespace of various utilities to test pipelines."
  (:require
   [datasplash.core :as ds])
  (:import
   (org.apache.beam.sdk.testing TestPipeline)))

(set! *warn-on-reflection* true)

(defn generate
  "Like `datasplash.api/generate-input` but uses a unique name.
   Useful in TestPipeline where ptranforms **cannot** have the same name."
  ([p]
   (generate [] p))
  ([values p]
   (generate values nil p))
  ([values options p]
   (ds/generate-input values
                      (cond-> options
                        (nil? (:name options))
                        (assoc :name (str "generate-" (System/nanoTime))))
                      p)))

(defn test-pipeline
  "Returns a TestPipeline."
  ^TestPipeline []
  (-> (TestPipeline/create)
      (.enableAbandonedNodeEnforcement true)))

(defn run-test-pipeline
  "Runs a Pipeline and waits until it finishes to assert execution state."
  [topology]
  (let [state (ds/wait-pipeline-result (ds/run-pipeline topology))
        ;; dynamically load `clojure.test` to avoid dependency in main profile.
        do-report (requiring-resolve 'clojure.test/do-report)]
    (if (= :done state)
      (do-report {:type :pass})
      (do-report {:type :fail :message "Wrong pipeline state"
                  :expected :done :actual state}))
    state))

(defmacro with-test-pipeline
  "Syntactic sugar to run a test pipeline.
   Use bindings vector to initialize options and create a pipeline.
   The pipeline creation must be the last binding.

   ```
   (with-test-pipeline [p (test-pipeline)]
     (generate [:a :b :c] p))
   ```"
  [bindings & body]
  (assert (vector? bindings) "a vector for its binding")
  (assert (even? (count bindings)) "an even number of forms in binding vector")
  `(let ~bindings
     ~@body
     (run-test-pipeline ~(nth bindings (- (count bindings) 2)))))
