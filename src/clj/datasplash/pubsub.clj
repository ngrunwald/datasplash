(ns datasplash.pubsub
  (:require [datasplash.core :refer :all])
  (:import (org.apache.beam.sdk.io.gcp.pubsub PubsubIO)
           (org.apache.beam.sdk.values PBegin)
           (org.apache.beam.sdk Pipeline)))

(defn read-from-pubsub
  "Create an unbounded PCollection from a pubsub stream. Takes a :kind option that specifies if the input is a :subscription or a :topic"
  [subscription-or-topic {:keys [kind] :or {:kind :subscription} :as options} p]
  (let [pipe (if (instance? Pipeline p) (PBegin/in p) p)]
    (cond
      (= :subscription kind) (apply-transform pipe (.fromSubscription (PubsubIO/readMessages) subscription-or-topic) {} options)
      (= :topic kind) (apply-transform pipe (.fromTopic (PubsubIO/readMessages)  subscription-or-topic) {} options)
      :else (throw (ex-info (format "Wrong type of :kind for pubsub [%s], should be either :subscription or :topic" kind)
                            {:kind kind})))))

(defn write-to-pubsub
  "Write the contents of an unbounded PCollection to to a pubsub stream"
  [topic options pcoll]
  (-> pcoll
      (apply-transform (.to (PubsubIO/writeMessages) topic) {} options)))
