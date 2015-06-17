(ns datasplash.config
  (:require [clojure.string :as str]))

(def types {:string String})

(def the-void Void/TYPE)

(def annotations-mapping {:default-string 'com.google.cloud.dataflow.sdk.options.Default$String})

(defmacro defconfig
  [interface-name specs]
  `(gen-interface
    :name ~interface-name
    :extends [com.google.cloud.dataflow.sdk.options.PipelineOptions]
    :methods
    [~@(apply concat
              (for [[kw {:keys [type annotations]}] specs
                    :let [nam (str/capitalize (name kw))]]
                `([~(with-meta
                      (symbol (str "get" nam))
                      (reduce (fn [acc [k v]]
                                (assoc acc (get annotations-mapping k) v))
                              {} annotations))
                   [] ~(get types type)]
                  [~(with-meta (symbol (str "set" nam)) `{}) [~(get types type)] ~the-void])))]))
