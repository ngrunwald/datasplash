(ns datasplash.pubsub
  (:require [datasplash.core :refer :all]
            [cheshire.core :as json])
  (:import (org.apache.beam.sdk.io.gcp.pubsub PubsubIO PubsubMessage PubsubMessageWithAttributesCoder)
           (org.apache.beam.sdk.values PBegin)
           (org.apache.beam.sdk Pipeline)
           (java.nio.charset StandardCharsets)))


(def message-types
  {:read {:raw (PubsubIO/readMessagesWithAttributes)
          :string (PubsubIO/readStrings)}
   :write {:raw (PubsubIO/writeMessages)
           :string (PubsubIO/writeStrings)}})

(defn pubsub-message->clj
  "Converts a pubsub message to a clojure usable object. Assumes the payload is UTF-8 encoded"
  [^PubsubMessage m]
  {:payload (String. (.getPayload m) "UTF-8")
   :attributes (into {} (.getAttributeMap m))})

(defn clj->pubsub-message
  "Converts a clojure map containing a payload and an attributes keys. payload must be a string and attributes a map"
  [{:keys [payload attributes]}]
  (let [attributes-map (into {} (map (fn [k v] [(if (keyword? k) (name k) (str k)) (str v)]) attributes))]
    (PubsubMessage. (.getBytes payload StandardCharsets/UTF_8) attributes-map)))

(defn encode-messages
  "Converts the input to PubsubMessages. To use before `write-to-pubsub` with type `:raw`"
  [options p]
  (dmap clj->pubsub-message (assoc options :coder (PubsubMessageWithAttributesCoder/of)) p))

(defn decode-messages
  "Converts the input PubsubMessages to clojure objects. To use after `read-from-pubsub` with type `:raw`"
  [options p]
  (dmap pubsub-message->clj options p))

(def read-from-pubsub-schema
  (merge
   named-schema
   {:kind {:docstr "Specifies if the input is a `:subscription` or a `:topic` (default to :topic)."}
    :type {:docstr "Specify the type of message reader, default to `:string.` Possible values are `:string`: UTF-8 encoded strings, `:raw`: pubsub message with attributes."}
    :timestamp-label {:docstr "Set the timestamp of the message using a message's attribute. The attribute should contain an Unix epoch in milliseconds."}}))

(defn read-from-pubsub
  {:doc (with-opts-docstr
          "Create an unbounded PCollection from a pubsub stream.

See https://cloud.google.com/dataflow/model/pubsub-io#reading-with-pubsubio.

Examples:
```
(ps/read-from-pubsub \"projects/my-project/subscriptions/my-subscription\" pcoll)

;; payload will be a string and attributes a map
(->> (ps/read-from-pubsub \"projects/my-project/subscriptions/my-subscription\" {:type :raw} pcoll)
     (ps/decode-messages)
     (ds/map (fn [{:keys [payload attributes]}] (json/decode payload))))
```"
          read-from-pubsub-schema)
   :added "0.4.0"}
  ([subscription-or-topic {:keys [kind timestamp-label type] :or {kind :subscription type :string} :as options} p]
   (let [pipe (if (instance? Pipeline p) (PBegin/in p) p)
         pubsub-read (cond-> (get-in message-types [:read type])
                       timestamp-label (.withTimestampAttribute timestamp-label)
                       (= :subscription kind) (.fromSubscription subscription-or-topic)
                       (= :topic kind) (.fromTopic subscription-or-topic))]
     (when-not (#{:subscription :topic} kind)
       (throw (ex-info (format "Wrong type of :kind for pubsub [%s], should be either :subscription or :topic" kind)
                       {:kind kind})))
     (apply-transform pipe pubsub-read read-from-pubsub-schema options)))
  ([subscription-or-topic p] (read-from-pubsub subscription-or-topic {} p)))

(def write-from-pubsub-schema
  (merge
   named-schema
   {:type {:docstr "Specify the type of message writer, default to `:string.` Possible values are `:string`: UTF-8 encoded strings, `:raw`: raw pubsub message."}}))

(defn write-to-pubsub
  {:doc (with-opts-docstr
          "Write the contents of an unbounded PCollection to to a pubsub stream.

See https://cloud.google.com/dataflow/model/pubsub-io#writing-with-pubsubio.

Examples:
```
(ps/write-to-pubsub \"projects/my-project/topics/my-topic\" pcoll)

;; Assuming the input pcoll's elements are {:payload \"my message\" :attributes {:key value}}
(->> (ps/encode-messages)
     (ps/write-from-to-pubsub \"projects/my-project/topics/my-topic\" {:type :raw} pcoll))
```"
          write-from-pubsub-schema)
   :added "0.4.0"}
  ([topic options pcoll]
   (-> pcoll
       (apply-transform (.to (get-in message-types [:write type]) topic) {} options)))
  ([topic pcoll] (write-to-pubsub topic {} pcoll)))
