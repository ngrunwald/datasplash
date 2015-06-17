(ns datasplash.options
  (:require [clojure.string :as str]))

(def the-void Void/TYPE)

(defn extract-default-type
  [klass]
  (-> klass
      (.getName)
      (str/split #"\.")
      (last)))

(def annotations-mapping
  {:default (fn [{:keys [type]}] (symbol (str "com.google.cloud.dataflow.sdk.options.Default$" (extract-default-type type))))
   :description 'com.google.cloud.dataflow.sdk.options.Description
   :hidden 'com.google.cloud.dataflow.sdk.options.Hidden})

(defn capitalize-first
  [s]
  (str (.toUpperCase (subs s 0 1))
       (subs s 1)))

(defmacro defoptions
  [interface-name specs]
  `(gen-interface
    :name ~interface-name
    :extends [com.google.cloud.dataflow.sdk.options.PipelineOptions]
    :methods
    [~@(apply concat
              (for [[kw {:keys [type] :as spec}] specs
                    :let [nam (capitalize-first (name kw))
                          annotations (select-keys spec [:default :description :hidden])]]
                `([~(with-meta
                      (symbol (str "get" nam))
                      (reduce (fn [acc [k v]]
                                (assoc acc
                                       (let [m (get annotations-mapping k)]
                                         (if (fn? m) (m spec) m))
                                       (if (nil? v) true v)))
                              {} annotations))
                   [] ~type]
                  [~(with-meta (symbol (str "set" nam)) `{}) [~type] ~the-void])))]))
