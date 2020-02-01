(ns datasplash.options
  (:require [clojure.string :as str]))

(def the-void Void/TYPE)

(def value-types
  {:long {:default-type 'org.apache.beam.sdk.options.Default$Long
          :option-type Long
          :value-type Long}
   :integer {:default-type 'org.apache.beam.sdk.options.Default$Integer
             :option-type Integer
             :value-type Integer}
   :string {:default-type 'org.apache.beam.sdk.options.Default$String
            :option-type String
            :value-type String}
   :boolean {:default-type 'org.apache.beam.sdk.options.Default$Boolean
             :option-type Boolean
             :value-type Boolean}
   :double {:default-type 'org.apache.beam.sdk.options.Default$Double
            :option-type Double
            :value-type Double}
   :float {:default-type 'org.apache.beam.sdk.options.Default$Float
           :option-type Float
           :value-type Float}
   :integer-value-provider {:option-type 'org.apache.beam.sdk.options.ValueProvider
                            :default-type 'org.apache.beam.sdk.options.Default$Integer
                            :value-type Integer}
   :long-value-provider {:option-type 'org.apache.beam.sdk.options.ValueProvider
                         :default-type 'org.apache.beam.sdk.options.Default$Long
                         :value-type Long}
   :string-value-provider {:option-type 'org.apache.beam.sdk.options.ValueProvider
                           :default-type 'org.apache.beam.sdk.options.Default$String
                           :value-type String}
   :boolean-value-provider {:option-type 'org.apache.beam.sdk.options.ValueProvider
                            :default-type 'org.apache.beam.sdk.options.Default$Boolean
                            :value-type Boolean}
   :double-value-provider {:option-type 'org.apache.beam.sdk.options.ValueProvider
                           :default-type 'org.apache.beam.sdk.options.Default$Double
                           :value-type Double}
   :float-value-provider {:option-type 'org.apache.beam.sdk.options.ValueProvider
                          :default-type 'org.apache.beam.sdk.options.Default$Float
                          :value-type Float}})

(defn extract-default-type
  [klass]
  (-> klass
      (.getName)
      (str/split #"\.")
      (last)))

(def annotations-mapping
  {:default (fn [{:keys [type]}]
              (if (keyword? type)
                (:default-type (value-types type))
                (symbol (str "org.apache.beam.sdk.options.Default$" (extract-default-type type)))))
   :description 'org.apache.beam.sdk.options.Description
   :hidden 'org.apache.beam.sdk.options.Hidden})

(defn capitalize-first
  [s]
  (str (.toUpperCase (subs s 0 1))
       (subs s 1)))

(defmacro defoptions
  [interface-name specs]
  `(do
     (gen-interface
      :name ~interface-name
      :extends [org.apache.beam.sdk.options.PipelineOptions]
      :methods
      [~@(apply concat
                (for [[kw {:keys [type] :as spec}] specs
                      :let [nam (capitalize-first (name kw))
                            annotations (select-keys spec [:default :description :hidden])
                            {:keys [option-type value-type]} (if (keyword? type)
                                                               (value-types type)
                                                               {:option-type type :value-type type})]]
                  `([~(with-meta
                        (symbol (str "get" nam))
                        (reduce (fn [acc [k v]]
                                  (assoc acc
                                         (let [m (get annotations-mapping k)]
                                           (if (fn? m) (m spec) m))
                                         (if (nil? v) true v)))
                                {} annotations))
                     [] ~option-type]
                    [~(with-meta (symbol (str "set" nam)) `{}) [~option-type] ~the-void])))])
     (def ~interface-name '~interface-name)))
