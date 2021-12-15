(ns datasplash.pubsub
  (:require
   [datasplash.core :as ds])
  (:import
   (java.nio.charset StandardCharsets)
   (org.apache.beam.sdk Pipeline)
   (org.apache.beam.sdk.io.gcp.pubsub PubsubIO PubsubMessage PubsubMessageWithAttributesCoder)
   (org.apache.beam.sdk.values PBegin)))


(def ^:no-doc message-types
  {:read {:raw (PubsubIO/readMessagesWithAttributes)
          :string (PubsubIO/readStrings)}
   :write {:raw (PubsubIO/writeMessages)
           :string (PubsubIO/writeStrings)}})

(defn ^:no-doc pubsub-message->clj
  "Converts a pubsub message to a clojure usable object. Assumes the payload is UTF-8 encoded"
  [^PubsubMessage m]
  {:payload (String. (.getPayload m) "UTF-8")
   :attributes (into {} (.getAttributeMap m))})

(defn ^:no-doc clj->pubsub-message
  "Converts a clojure map containing a payload and an attributes keys. payload must be a string and attributes a map"
  [{:keys [payload attributes]}]
  (let [attributes-map (->> attributes
                            (ds/dmap (fn [k v] [(if (keyword? k) (name k) (str k)) (str v)]))
                            (into {}))]
    (PubsubMessage. (.getBytes payload StandardCharsets/UTF_8) attributes-map)))

(defn encode-messages
  "Converts the input to PubsubMessages. To use before `write-to-pubsub` with type `:raw`.

  Takes as input a map with a `:payload` key and an `:attributes` key, assumes the payload is UTF-8 encoded"
  [options p]
  (ds/dmap clj->pubsub-message (assoc options :coder (PubsubMessageWithAttributesCoder/of)) p))

(defn decode-messages
  "Converts the input PubsubMessages to clojure objects. To use after `read-from-pubsub` with type `:raw`.

  Returns an map with a `:payload` key and an `:attributes` key, assumes the payload is UTF-8 encoded"
  [options p]
  (ds/dmap pubsub-message->clj options p))

(def ^:no-doc read-from-pubsub-schema
  (merge
   ds/named-schema
   {:kind {:docstr "Specifies if the input is a `:subscription` or a `:topic` (default to `:topic`)."}
    :type {:docstr "Specify the type of message reader, default to `:string.` Possible values are `:string`: UTF-8 encoded strings, `:raw`: pubsub message with attributes."}
    :timestamp-label {:docstr "Set the timestamp of the message using a message's attribute. The attribute should contain an Unix epoch in milliseconds."}}))

(defn read-from-pubsub
  {:doc (ds/with-opts-docstr
          "Create an unbounded PCollection from a pubsub stream.

See https://cloud.google.com/dataflow/model/pubsub-io#reading-with-pubsubio.

Examples:
```
;; Assuming input message are UTF-8 encoded Strings:
(ps/read-from-pubsub \"projects/my-project/subscriptions/my-subscription\" pcoll)
```

If you need to access some attributes:

```
;; payload will be a string and attributes a map
(->> (ps/read-from-pubsub \"projects/my-project/subscriptions/my-subscription\" {:type :raw} pcoll)
     (ps/decode-messages {})
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
     (ds/apply-transform pipe pubsub-read read-from-pubsub-schema options)))
  ([subscription-or-topic p] (read-from-pubsub subscription-or-topic {} p)))

(def ^:no-doc write-from-pubsub-schema
  (merge
   ds/named-schema
   {:type {:docstr "Specify the type of message writer, default to `:string.` Possible values are `:string`: UTF-8 encoded strings, `:raw`: raw pubsub message."}}))

(defn write-to-pubsub
  {:doc (ds/with-opts-docstr
          "Write the contents of an unbounded PCollection to to a pubsub stream.

See https://cloud.google.com/dataflow/model/pubsub-io#writing-with-pubsubio.

Examples:
```
;; Assuming the input's pcoll are UTF-8 encoded Strings
(ps/write-to-pubsub \"projects/my-project/topics/my-topic\" pcoll)
```

If you need to specify some attributes:

```
;; Assuming the input pcoll's elements are {:payload \"my message\" :attributes {:key value}}
(->> pcoll
     (ps/encode-messages {})
     (ps/write-from-to-pubsub \"projects/my-project/topics/my-topic\" {:type :raw}))
```"
          write-from-pubsub-schema)
   :added "0.4.0"}
  ([topic {:keys [type] :or {type :string} :as options} pcoll]
   (-> pcoll
       (ds/apply-transform (.to (get-in message-types [:write type]) topic) {} options)))
  ([topic pcoll] (write-to-pubsub topic {} pcoll)))
