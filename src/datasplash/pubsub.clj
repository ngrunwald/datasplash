(ns datasplash.pubsub
  (:require [datasplash.core :refer :all])
  (:import (com.google.cloud.dataflow.sdk.io PubsubIO$Read PubsubIO$Write)
           (com.google.cloud.dataflow.sdk.values PBegin)
           (com.google.cloud.dataflow.sdk Pipeline)))

(defn read-from-pubsub
  "Create an unbounded PCollection from a pubsub stream"
  [topic options p]
  (-> p
      (cond-> (instance? Pipeline p) (PBegin/in))
      (apply-transform (PubsubIO$Read/subscription topic) {} options)))

(defn write-to-pubsub
  "Write the contents of an unbounded PCollection to to a pubsub stream"
  [topic options pcoll]
  (-> pcoll
      (apply-transform (PubsubIO$Write/topic topic) {} options)))

; Example
;(def read-subscription "projects/my-project/subscriptions/my-subscription")
;(def write-transformed-topic "projects/my-project/topics/my-transformed-topic")
;(def read-transformed-subscription "projects/my-project/subscriptions/my-transformed-subscription")
;
;(defn stream-interactions-from-pubsub
;  [pipeline]
;  (->> pipeline
;       (read-from-pubsub read-subscription {:name "read-interactions-from-pubsub"})
;       (ds/map (fn [message]
;                 (do
;                   (log/info (str "Got message:\n" message))
;                   message)) {:name "log-message"})
;       (write-to-pubsub write-transformed-topic {:name "write-forwarded-interactions-to-pubsub"})))
;
;(defn stream-forwarded-interactions-from-pubsub
;  [pipeline]
;  (->> pipeline
;       (read-from-pubsub read-transformed-subscription {:name "read-transformed-interactions-from-pubsub"})
;       (ds/map (fn [message]
;                 (do
;                   (log/info (str "Got transformed message:\n" message))
;                   message)) {:name "log-transformed-message"})))