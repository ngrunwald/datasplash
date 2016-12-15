(ns datasplash.pubsub
  (:require [datasplash.core :refer :all])
  (:import (com.google.cloud.dataflow.sdk.io PubsubIO$Read PubsubIO$Write)
           (com.google.cloud.dataflow.sdk.values PBegin)
           (com.google.cloud.dataflow.sdk Pipeline)))

(defn read-from-pubsub
  "Create an unbounded PCollection from a pubsub stream"
  [subscription-or-topic options p]
  (cond-> p
          (instance? Pipeline p) (PBegin/in)
          (re-find #"subscriptions" subscription-or-topic) (apply-transform (PubsubIO$Read/subscription subscription-or-topic) {} options)
          (re-find #"topics" subscription-or-topic) (apply-transform (PubsubIO$Read/topic subscription-or-topic) {} options)))

(defn write-to-pubsub
  "Write the contents of an unbounded PCollection to to a pubsub stream"
  [topic options pcoll]
  (-> pcoll
      (apply-transform (PubsubIO$Write/topic topic) {} options)))
