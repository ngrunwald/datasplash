(ns datasplash.core
  (:require [clj-stacktrace.core :as st]
            [clojure.edn :as edn]
            [clojure.java.shell :refer [sh]]
            [clojure.math.combinatorics :as combo]
            [clojure.string :as str]
            [taoensso.nippy :as nippy])
  (:import [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.coders StringUtf8Coder CustomCoder Coder$Context KvCoder]
           [com.google.cloud.dataflow.sdk.io
            TextIO$Read TextIO$Write TextIO$CompressionType]
           [com.google.cloud.dataflow.sdk.options PipelineOptionsFactory]
           [com.google.cloud.dataflow.sdk.transforms
            DoFn DoFn$Context DoFn$ProcessContext ParDo DoFnTester Create PTransform
            SerializableFunction WithKeys GroupByKey RemoveDuplicates
            Flatten Combine$CombineFn Combine View
            Sum$SumDoubleFn Sum$SumLongFn Sum$SumIntegerFn
            Max$MaxDoubleFn Max$MaxLongFn Max$MaxIntegerFn Max$MaxFn
            Min$MinDoubleFn Min$MinLongFn Min$MinIntegerFn Min$MinFn
            Mean$MeanFn Sample
            Count$Globally Count$PerElement]
           [com.google.cloud.dataflow.sdk.transforms.join KeyedPCollectionTuple CoGroupByKey]
           [com.google.cloud.dataflow.sdk.values KV PCollection TupleTag PBegin PCollectionList]
           [java.io InputStream OutputStream DataInputStream DataOutputStream]
           [java.util UUID]
           [clojure.lang MapEntry ExceptionInfo]))

(def ops-counter (atom {}))

(def ^{:private true} required-ns (atom #{}))

(defmethod print-method KV [^KV kv ^java.io.Writer w]
  (.write w (str "[" (.getKey kv) ", " (.getValue kv) "]")))

(defn unloaded-ns-from-ex
  [e]
  (loop [todo (st/parse-exception e)
         nss (list)]
    (let [{:keys [message trace-elems cause] :as current-ex} todo]
      (if message
        (if (re-find #"clojure\.lang\.Var\$Unbound|call unbound fn|dynamically bind non-dynamic var" message)
          (let [[_ missing-ns] (or (re-find #"call unbound fn: #'([^/]+)/" message)
                                   (re-find #"Can't dynamically bind non-dynamic var: ([^/]+)/" message))
                ns-to-add (->> trace-elems
                               (filter #(:clojure %))
                               (map :ns)
                               (concat (list missing-ns)))]
            (recur cause (concat ns-to-add nss)))
          (recur cause nss))
        (->> nss
             (remove nil?)
             (distinct)
             (map symbol))))))

(def get-hostname
  ^{:doc "Try to guess local hostname"}
  (memoize
   (fn []
     (try
       (str/trim-newline (:out (sh "hostname")))
       (catch Exception e
         "unknown-hostname")))))

(defmacro safe-exec
  "Executes body while trying to sanely require missing ns if the runtime is not yet properly loaded for Clojure"
  [& body]
  `(try
     ~@body
     (catch ExceptionInfo e#
       (throw e#))
     (catch Exception e#
       ;; lock on something that should exist!
       (locking #'locking
         (when-not (bound? (find-var (symbol "datasplash.core" "required-ns")))
           (require 'datasplash.core)
           (require 'datasplash.api)
           (swap! required-ns conj 'datasplash.core 'datasplash.api)))
       (let [required-at-start# (deref required-ns)]
         (locking #'locking
           (let [nss# (unloaded-ns-from-ex e#)]
             (if (empty? nss#)
               (throw (ex-info "Runtime exception intercepted" {:hostname (get-hostname)} e#))
               (let [already-required# (deref required-ns)
                     missing# (first (remove already-required# nss#))
                     missing-at-start# (first (remove required-at-start# nss#))]
                 (if missing#
                   (do
                     (require missing#)
                     (swap! required-ns conj missing#)
                     ~@body)
                   (if missing-at-start#
                     ~@body
                     (throw (ex-info "Dynamic reloading of namespace seems not to work"
                                     {:ns-from-exception nss#
                                      :ns-load-attempted already-required#
                                      :hostname (get-hostname)}
                                     e#))))))))))))

(defn kv->clj
  "Coerce from KV to Clojure MapEntry"
  [^KV kv]
  (MapEntry. (.getKey kv) (.getValue kv)))

(defn make-keyed-pcollection-tuple
  [pcolls]
  (let [empty-kpct (KeyedPCollectionTuple/empty (.getPipeline (first pcolls)))]
    (reduce
     (fn [coll-tuple [idx pcoll]]
       (let [tag (TupleTag. (str idx))
             new-coll-tuple (.and coll-tuple tag pcoll)]
         new-coll-tuple))
     empty-kpct (map-indexed (fn [idx v] [idx v]) pcolls))))

(def ^{:dynamic true :no-doc true} *coerce-to-clj* true)
(def ^{:dynamic true :no-doc true} *context* nil)
(def ^{:dynamic true :no-doc true} *side-inputs* {})

(defn dofn
  {:doc "Returns an Instance of DoFn from given Clojure fn"
   :added "0.1.0"}
  ^DoFn
  ([f {:keys [start-bundle finish-bundle without-coercion-to-clj
              side-inputs side-outputs]
       :or {start-bundle (fn [_] nil)
            finish-bundle (fn [_] nil)}
       :as opts}]
   (proxy [DoFn] []
     (processElement [^DoFn$ProcessContext context]
       (safe-exec
        (let [side-ins (persistent!
                        (reduce
                         (fn [acc [k pview]]
                           (assoc! acc k (.sideInput context pview)))
                         (transient {}) side-inputs))]
          (binding [*context* context
                    *coerce-to-clj* (not without-coercion-to-clj)
                    *side-inputs* side-ins]
            (f context)))))
     (startBundle [^DoFn$Context context] (safe-exec (start-bundle context)))
     (finishBundle [^DoFn$Context context] (safe-exec (finish-bundle context)))))
  ([f] (dofn f {})))

(defn context
  {:added "0.1.0"
   :doc "In the context of a ParDo, contains the corresponding Context object.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/DoFn.ProcessContext.html"}
  [] *context*)

(defn side-inputs
  {:doc "In the context of a ParDo, returns the corresponding side inputs as a map from names to values.
  Example:
    (let [input (ds/generate-input [1 2 3 4 5] p)
          side-input (ds/view (ds/generate-input [{1 :a 2 :b 3 :c 4 :d 5 :e}] p))
          proc (ds/map (fn [x] (get-in (ds/side-inputs) [:mapping x]))
                       {:side-inputs {:mapping side-input}} input)])"
   :added "0.1.0"}
  [] *side-inputs*)

(defn get-element-from-context
  "Get element from context in ParDo while applying relevent Clojure type conversions"
  [^DoFn$ProcessContext c]
  (let [element (.element c)]
    (if *coerce-to-clj*
      (if (instance? KV element)
        (kv->clj element)
        element)
      element)))

(defn map-fn
  "Returns a function that corresponds to a Clojure map operation inside a ParDo"
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (get-element-from-context c)
          result (f elt)]
      (.output c result))))

(defn mapcat-fn
  "Returns a function that corresponds to a Clojure mapcat operation inside a ParDo"
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (get-element-from-context c)
          result (f elt)]
      (doseq [res result]
        (.output c res)))))

(defn pardo-fn
  "Returns a function that uses the raw ProcessContext from ParDo"
  [f]
  (fn [^DoFn$ProcessContext c]
    (f c)))

(defn filter-fn
  "Returns a function that corresponds to a Clojure filter operation inside a ParDo"
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (get-element-from-context c)
          result (f elt)]
      (when result
        (.output c elt)))))

(defn didentity
  {:doc "Identity function for use in a ParDo"
   :added "0.1.0"}
  [^DoFn$ProcessContext c]
  (.output c (.element c)))

(defn make-nippy-coder
  {:doc "Returns an instance of a CustomCoder using nippy for serialization"
   :added "0.1.0"}
  []
  (proxy [CustomCoder] []
    (encode [obj ^OutputStream out ^Coder$Context context]
      (safe-exec
       (let [dos (DataOutputStream. out)]
         (nippy/freeze-to-out! dos obj))))
    (decode [^InputStream in ^Coder$Context context]
      (safe-exec
       (let [dis (DataInputStream. in)]
         (nippy/thaw-from-in! dis))))
    (verifyDeterministic [] nil)
    (consistentWithEquals [] true)))

(defmacro with-opts
  [schema opts & body]
  `(let [transform# (do ~@body)
         full-name# (name
                     (or (:name ~opts)
                         (let [label# (name (get ~opts :label "cljfn"))
                               new-idx# (get
                                         (swap! ~'ops-counter update-in [label#]
                                                (fn [i#] (if i# (inc i#) 1)))
                                         label#)]
                           (str (name (get ~opts :label "cljfn")) "_"  new-idx#))))]
     (reduce
      (fn [f# [k# specs#]]
        (if-let [v# (get (assoc ~opts :name full-name#) k#)]
          (if-let [action# (get specs# :action)]
            (action# f# v#)
            f#)
          f#))
      transform# ~schema)))

(defn with-opts-docstr
  [doc-string & schemas]
  (apply str doc-string "\n\nAvailable options:\n"
         (->> (for [schema schemas
                    [k {:keys [docstr enum]}] schema]
                (-> (str k " => " docstr)
                    (cond-> enum (str " one of " (keys enum) "."))
                    (str "\n")))
              (distinct)
              (sort))))

(defn select-enum-option-fn
  [option-name enum-map action]
  (fn [transform kw]
    (let [enum (get enum-map (keyword kw))]
      (if enum
        (action transform enum)
        (throw
         (ex-info (format "%s must be one of %s, %s given"
                          option-name (keys enum-map) kw)
                  {:expected (keys enum-map)
                   :given kw}))))))

(def base-schema
  {:name {:docstr "Adds a name to the Transform."
          :action (fn [transform n] (.withName transform n))}
   :coder {:docstr "Uses a specific Coder for the results of this transform. Usually defaults to some form of nippy-coder."}})

(def pardo-schema
  (merge
   base-schema
   {:side-inputs {:docstr "Adds a map of PCollectionViews as side inputs to the underlying ParDo Transform. They can be accessed there by key in the return of side-inputs fn."
                  :action (fn [transform inputs]
                            (.withSideInputs transform (map val (sort-by key inputs))))}
    :without-coercion-to-clj {:docstr "Avoids coercing Dataflow types to Clojure, like KV. Coercion will happen by default"}}))

(defn map-op
  ([transform label coder]
   (fn make-map-op
     ([f options ^PCollection pcoll]
      (let [opts (assoc options :label label)]
        (-> pcoll
            (.apply (with-opts pardo-schema opts
                      (ParDo/of (dofn (transform f) opts))))
            (.setCoder (or (:coder opts) coder)))))
     ([f pcoll] (make-map-op f {} pcoll))))
  ([transform label]
   (map-op transform label (make-nippy-coder))))

(def
  ^{:arglists [['f 'pcoll] ['f 'options 'pcoll]]
    :added "0.1.0"
    :doc
    (with-opts-docstr
      "Returns a PCollection of f applied to every item in the source PCollection.
Function f should be a function of one argument.

  Example:
    (ds/map inc foo)
    (ds/map (fn [x] (* x x)) foo)

  Note: Unlike clojure.core/map, datasplash.api/map takes only one PCollection."
      pardo-schema)}
  dmap (map-op map-fn :map))

(def pardo (map-op pardo-fn :raw-pardo))

(def
  ^{:arglists [['f 'pcoll] ['f 'options 'pcoll]]
    :added "0.1.0"
    :doc (with-opts-docstr
           "Returns the result of applying concat, or flattening, the result of applying
f to each item in the PCollection. Thus f should return a Clojure or Java collection.

  Example:

    (ds/mapcat (fn [x] [(dec x) x (inc x)]) foo)"
           pardo-schema)}
  dmapcat (map-op mapcat-fn :mapcat))

(def
  ^{:arglists [['pred 'pcoll] ['f 'options 'pcoll]]
    :added "0.1.0"
    :doc (with-opts-docstr
           "Returns a PCollection that only contains the items for which (pred item)
returns true.

  Example:

    (ds/filter even? foo)
    (ds/filter (fn [x] (even? (* x x))) foo)"
           pardo-schema)}
  dfilter (map-op filter-fn :filter))

(defn generate-input
  {:doc (with-opts-docstr
          "Generates a pcollection from the given collection.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/Create.html

  Example:
    (ds/generate-input (range 0 1000) pipeline)"
          base-schema)
   :added "0.1.0"}
  ([coll options ^Pipeline p]
   (let [opts (assoc options :label :generate-input)]
     (-> p
         (.apply (with-opts base-schema opts
                   (Create/of (seq coll))))
         (.setCoder (or (:coder opts) (make-nippy-coder))))))
  ([coll p] (generate-input coll {} p)))

(defn view
  ([pcoll {:keys [view-type]
           :or {view-type :singleton}
           :as options}]
   (let [opts (assoc options :label :view)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (case view-type
                     :singleton (View/asSingleton)
                     :iterable (View/asIterable)
                     :map (View/asMap)))))))
  ([pcoll] (view pcoll {})))

(defn- to-edn*
  [^DoFn$Context c]
  (let [elt (.element c)
        result (pr-str elt)]
    (.output c result)))

(def to-edn (partial (map-op identity :to-edn (StringUtf8Coder/of)) to-edn*))
(def from-edn (partial dmap #(read-string %)))

(defn sfn
  "Returns an instance of SerializableFunction equivalent to f."
  ^SerializableFunction
  [f]
  (proxy [SerializableFunction clojure.lang.IFn] []
    (apply [input]
      (safe-exec (f input)))
    (invoke [input]
      (safe-exec (f input)))))

(definterface ICombineFn
  (getReduceFn [])
  (getExtractFn [])
  (getMergeFn [])
  (getInitFn []))

(defn combine-fn
  {:doc "Returns a CombineFn instance from given args. See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/Combine.CombineFn.html"
   :added "0.1.0"}
  ^Combine$CombineFn
  ([reducef extractf combinef initf output-coder acc-coder]
   (proxy [Combine$CombineFn ICombineFn clojure.lang.IFn] []
     (createAccumulator [] (safe-exec (initf)))
     (addInput [acc elt] (safe-exec (reducef acc elt)))
     (mergeAccumulators [accs] (safe-exec (apply combinef accs)))
     (extractOutput [acc] (safe-exec (extractf acc)))
     (getDefaultOutputCoder [_ _] output-coder)
     (getAccumulatorCoder [_ _] acc-coder)
     (getReduceFn [] reducef)
     (getExtractFn [] extractf)
     (getMergeFn [] combinef)
     (getInitFn [] initf)
     (invoke [& args] (let [args-nb (count args)]
                        (cond
                          (= args-nb 0) (safe-exec (initf))
                          (= args-nb 2) (safe-exec (reducef (first args) (second args)))
                          (> args-nb 2) (throw (ex-info "Cannot call a combineFn with more than 2 arguments"
                                                        {:args-number args-nb
                                                         :args args}))
                          :else (if (sequential? (first args))
                                  (safe-exec (apply combinef (first args)))
                                  (safe-exec (extractf (first args)))))))))
  ([reducef extractf combinef initf output-coder] (combine-fn reducef extractf combinef initf output-coder (make-nippy-coder)))
  ([reducef extractf combinef initf] (combine-fn reducef extractf combinef initf (make-nippy-coder)))
  ([reducef extractf combinef] (combine-fn reducef extractf combinef reducef))
  ([reducef extractf] (combine-fn reducef extractf reducef))
  ([reducef] (combine-fn reducef identity)))

(defn ->combine-fn
  "Returns a CombineFn if f is not one already."
  ^Combine$CombineFn
  [f]
  (if (instance? Combine$CombineFn f) f (combine-fn f)))

(defn djuxt
  {:doc "Creates a CombineFn that applies multiple combiners in one go. Produces a vector of combined results.
'sibling fusion' in Dataflow optimizes multiple independant combiners in the same way, but you might find juxt more concise.
Only works with functions created with combine-fn or native clojure functions, and not with native Dataflow CombineFn

  Example:
    (ds/combine (ds/juxt + *) pcoll)"
   :added "0.1.0"}
  [& fns]
  (let [cfs (map ->combine-fn fns)]
    (combine-fn
     (fn [accs elt]
       (map-indexed
        (fn [idx acc] (let [f (.getReduceFn (nth cfs idx))]
                        (f acc elt))) accs))
     (fn [accs]
       (map-indexed
        (fn [idx acc] (let [f (.getExtractFn (nth cfs idx))]
                        (f acc))) accs))
     (fn [& accs]
       (map-indexed
        (fn [idx cf] (let [f (.getMergeFn cf)]
                       (apply f (map #(nth % idx) accs)))) cfs))
     (fn []
       (map (fn [cf] (let [f (.getInitFn cf)]
                       (f))) cfs))
     (.getDefaultOutputCoder (first cfs) nil nil)
     (.getAccumulatorCoder (first cfs) nil nil))))

(defn clj->kv
  "Coerce from Clojure data to KV objects"
  ^KV
  [obj]
  (cond
    (instance? KV obj) obj
    (and (sequential? obj) (= 2 (count obj))) (KV/of (first obj) (second obj))
    :else (throw (ex-info "Cannot coerce given object to KV"
                          {:hostname (get-hostname)
                           :input-object obj
                           :input-object-type (type obj)}))))

(def kv-coder-schema
  {:key-coder {:docstr "Coder to be used for encoding keys in the resulting KV PColl."}
   :value-coder {:docstr "Coder to be used for encoding values in the resulting KV PColl."}})

(defn with-keys
  {:doc (with-opts-docstr
          "Returns a PCollection of KV by applying f on each element of the input PColelction and using the return value as the key and the element as the value.
  See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/WithKeys.html

  Example:
    (with-keys even? pcoll)"
          base-schema kv-coder-schema)
   :added "0.1.0"}
  ([f {:keys [key-coder value-coder] :as options} ^PCollection pcoll]
   (let [opts (assoc options :label :with-keys)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (WithKeys/of (sfn f))))
         (.setCoder (or
                     (:coder opts)
                     (KvCoder/of
                      (or key-coder (make-nippy-coder))
                      (or value-coder (make-nippy-coder))))))))
  ([f pcoll] (with-keys f {} pcoll)))

(defn group-by-key
  {:doc "Takes a KV PCollection as input and returns a KV PCollection as output of K to list of V.
  See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/GroupByKey.html"
   :added "0.1.0"}
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :group-by-keys)]
     (-> pcoll
         (.apply (GroupByKey/create)))))
  ([pcoll] (group-by-key {} pcoll)))

(defn- group-by-transform
  [f options]
  (let [safe-opts (dissoc options :name)]
    (proxy [PTransform] []
      (apply [^PCollection pcoll]
        (->> pcoll
             (with-keys f safe-opts)
             (group-by-key safe-opts))))))

(defn dgroup-by
  {:doc (with-opts-docstr
          "Groups a Pcollection by the result of calling (f item) for each item.
This produces a sequence of KV values, similar to using seq with a
map. Each value will be a list of the values that match key.

  Example:

    (ds/group-by :a foo)
    (ds/group-by count foo)"
          base-schema)
   :added "0.1.0"}
  ([f options ^PCollection pcoll]
   (let [opts (assoc options :label :group-by)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (group-by-transform f options))))))
  ([f pcoll] (dgroup-by f {} pcoll)))

(defn interface->class
  [itf]
  (if (instance? Class itf)
    itf
    (Class/forName (name itf))))

(defn make-pipeline
  {:doc "Builds a Pipeline from command lines args"
   :added "0.1.0"}
  ([itf str-args]
   (let [builder (PipelineOptionsFactory/fromArgs
                  (into-array String str-args))
         options (if itf
                   (.as builder (interface->class itf))
                   (.create builder))
         pipeline (Pipeline/create options)
         coder-registry (.getCoderRegistry pipeline)]
     (doto coder-registry
       (.registerCoder clojure.lang.IPersistentCollection (make-nippy-coder))
       (.registerCoder clojure.lang.Keyword (make-nippy-coder)))
     pipeline))
  ([str-args]
   (make-pipeline nil str-args)))

(defn run-pipeline
  {:doc "Run the computation for a given pipeline or PCollection."
   :added "0.1.0"}
  [topology]
  (if (instance? Pipeline topology)
    (.run topology)
    (-> topology
        (.getPipeline)
        (.run))))

(defn get-pipeline-configuration
  {:doc "Returns a map corresponding to the bean of options. With arity one, can be called on a Pipeline. With arity zero, returns the same thing inside a ParDo."
   :added "0.1.0"}
  ([^Pipeline p]
   (dissoc (bean (.getOptions p)) :class))
  ([]
   (when-let [^DoFn$Context c *context*]
     (-> (.getPipelineOptions c)
         (bean)
         (dissoc :class)))))

;;;;;;;;;;;;;
;; Text IO ;;
;;;;;;;;;;;;;

(defn clean-filename
  "Clean filename for transform name building purpose."
  [s]
  (-> s
      (str/replace #"^\w+:/" "")
      ;; (str/replace #"/" "\\")
      ))

(def compression-type-enum
  {:auto TextIO$CompressionType/AUTO
   :bzip2 TextIO$CompressionType/BZIP2
   :gzip TextIO$CompressionType/GZIP
   :uncompressed TextIO$CompressionType/UNCOMPRESSED})

(def text-reader-schema
  {:without-validation {:docstr "Disables validation of path existence in Google Cloud Storage until runtime."
                        :action (fn [transform] (.withoutValidation transform))}
   :compression-type {:docstr "Choose compression type. :auto by default."
                      :enum compression-type-enum
                      :action (select-enum-option-fn
                               :compression-type
                               compression-type-enum
                               (fn [transform enum] (.withCompressionType transform enum)))}})

(def text-writer-schema
  {:num-shards {:docstr "Selects the desired number of output shards (file fragments). 0 to let the system decide (recommended)."
                :action (fn [transform shards] (.withNumShards transform shards))}
   :without-sharding {:docstr "Forces a single file output."
                      :action (fn [transform] (.withoutSharding transform))}
   :without-validation {:docstr "Disables validation of path existence in Google Cloud Storage until runtime."
                        :action (fn [transform] (.withoutValidation transform))}
   :shard-name-template {:docstr "Uses the given shard name template."
                         :action (fn [transform tpl] (.withShardNameTemplate transform tpl))}
   :suffix {:docstr "Uses the given filename suffix."
            :action (fn [transform suffix] (.withSuffix transform suffix))}})

(defn write-text-file
  {:doc (with-opts-docstr
          "Writes a PCollection of Strings to disk or Google Storage, with records separated by newlines.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/TextIO.Write.html

  Example:
    (write-text-file \"gs://target/path\" pcoll)"
          base-schema text-writer-schema)
   :added "0.1.0"}
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label (str "write-text-file-to-"
                                         (clean-filename to)))]
     (-> pcoll
         (.apply (with-opts (merge base-schema text-writer-schema) opts
                   (TextIO$Write/to to))))))
  ([to pcoll] (write-text-file to {} pcoll)))

(defn read-text-file
  {:doc (with-opts-docstr "Reads a PCollection of Strings from disk or Google Storage, with records separated by newlines.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/TextIO.Read.html

  Example:
    (read-text-file \"gs://target/path\" pcoll)"
          base-schema text-reader-schema)
   :added "0.1.0"}
  ([from options p]
   (let [opts (assoc options :label (str "read-text-file-from"
                                         (clean-filename from)))]
     (-> p
         (cond-> (instance? Pipeline p) (PBegin/in))
         (.apply (with-opts base-schema opts
                   (TextIO$Read/from from)))
         (.setCoder (or
                     (:coder opts)
                     (StringUtf8Coder/of))))))
  ([from p] (read-text-file from {} p)))

(defn- read-edn-file-transform
  [from options]
  (let [safe-opts (dissoc options :name)]
    (proxy [PTransform] []
      (apply [p]
        (->> p
             (read-text-file from options)
             (from-edn options))))))

(defn read-edn-file
  {:doc (with-opts-docstr "Reads a PCollection of edn strings from disk or Google Storage, with records separated by newlines.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/TextIO.Read.html

  Example:
    (read-edn-file \"gs://target/path\" pcoll)"
          base-schema text-reader-schema)
   :added "0.1.0"}
  ([from options ^Pipeline p]
   (let [opts (assoc options :label (str "read-edn-file-from-"
                                         (clean-filename from)))]
     (-> p
         (.apply (with-opts (merge base-schema text-reader-schema) opts
                   (read-edn-file-transform from opts))))))
  ([from p] (read-edn-file from {} p)))

(defn- write-edn-file-transform
  [to options]
  (let [safe-opts (dissoc options :name)]
    (proxy [PTransform] []
      (apply [^PCollection pcoll]
        (->> pcoll
             (to-edn options)
             (write-text-file to options))))))

(defn write-edn-file
  {:doc (with-opts-docstr
          "Writes a PCollection of edn strings to disk or Google Storage, with records separated by newlines.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/TextIO.Write.html

  Example:
    (write-edn-file \"gs://target/path\" pcoll)"
          base-schema text-writer-schema)
   :added "0.1.0"}
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label (str "write-edn-file-to-" (clean-filename to)))]
     (-> pcoll
         (.apply (with-opts (merge base-schema text-writer-schema) opts
                   (write-edn-file-transform to opts))))))
  ([to pcoll] (write-edn-file to {} pcoll)))

(defn cogroup-transform
  ([options]
   (let [opts (assoc options :label :raw-cogroup)]
     (proxy [PTransform] []
       (apply [^KeyedPCollectionTuple pcolltuple]
         (let [ordered-tags (->> pcolltuple
                                 (.getKeyedCollections)
                                 (map #(.getTupleTag %))
                                 (sort-by #(.getId %)))
               rel (.apply pcolltuple (with-opts base-schema opts (CoGroupByKey/create)))
               final-rel (dmap (fn [elt]
                                 (let [k (.getKey elt)
                                       raw-values (.getValue elt)
                                       values (for [tag ordered-tags]
                                                (into [] (first (.getAll raw-values tag))))]
                                   (into [] (conj values k))))
                               (assoc opts :without-coercion-to-clj true)
                               rel)]
           final-rel))))))

(defn cogroup
  [options pcolls]
  (let [opts (assoc options :label :cogroup)
        pcolltuple (make-keyed-pcollection-tuple pcolls)]
    (-> pcolltuple
        (.apply (with-opts base-schema opts
                  (cogroup-transform opts))))))

(defn cogroup-by
  ([options specs reduce-fn]
   (let [operations (for [[pcoll f type] specs]
                      [(dgroup-by f options pcoll) type])
         required-idx (remove nil? (map-indexed (fn [idx [_ b]] (when b idx)) operations))
         pcolls (map first operations)
         grouped-coll (cogroup options pcolls)
         filtered-coll (if (empty? required-idx)
                         grouped-coll
                         (dfilter (fn [[_ all-vals]]
                                    (let [idx-vals (into [] all-vals)]
                                      (every? #(not (empty? (get idx-vals %))) required-idx)))
                                  options
                                  grouped-coll))]
     (if reduce-fn
       (dmap reduce-fn options filtered-coll)
       filtered-coll)))
  ([options specs] (cogroup-by options specs nil)))

(defn join-by
  ([options specs join-fn]
   (->> (cogroup-by options specs)
        (dmapcat (fn [[k & results]]
                   (let [results-ok (map #(if (empty? %) [nil] %) results)
                         raw-res (apply combo/cartesian-product results-ok)
                         res (map (fn [prod] (apply join-fn prod)) raw-res)]
                     res)))))
  ([specs join-fn] (join-by {} specs join-fn)))

(defn ddistinct
  {:doc (with-opts-docstr
          "Removes duplicate element from PCollection.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/RemoveDuplicates.html
  Example:
    (ds/distinct pcoll)"
          base-schema)
   :added "0.1.0"}
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :distinct)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (RemoveDuplicates/create))))))
  ([pcoll] (ddistinct {} pcoll)))

(def scoped-ops-schema
  {:scope {:docstr "Scope given to the combinating operation. One of (:globally :per-key)."}})

(defn sample
  {:doc (with-opts-docstr
          "Takes samples of the elements in a PCollection, or samples of the values associated with each key in a PCollection of KVs.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/Sample.html

  Example:
    (ds/sample 100 {:scope :per-key} pcoll)"
          base-schema scoped-ops-schema)
   :added "0.1.0"}
  ([size {:keys [scope] :as options} ^PCollection pcoll]
   (let [opts (assoc options :label (keyword "sample-" (if scope (name scope) "any")))]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (cond
                     (#{:global :globally} scope) (Sample/fixedSizeGlobally size)
                     (#{:local :per-key} scope) (Sample/fixedSizePerKey size)
                     :else (Sample/any size))))
         (cond-> (:coder opts) (.setCoder (:coder opts))))))
  ([size pcoll] (sample size {} pcoll)))

(defn dflatten
  {:doc "Returns a single Pcollection containing all the pcolls in the given pcolls iterable.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/Flatten.html

  Example:
    (ds/flatten [pcoll1 pcoll2 pcoll3])"
   :added "0.1.0"}
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :flatten)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (Flatten/iterables))))))
  ([pcoll] (dflatten {} pcoll)))

(defn dconcat
  {:doc "Returns a single PCollection containing all the given pcolls. Accepts an option map as an optional first arg.

  Example:
    (ds/concat pcoll1 pcoll2)
    (ds/concat {:name :concat-node} pcoll1 pcoll2)"
   :added "0.1.0"}
  [options & colls]
  (let [[real-options ^Iterable all-colls]
        (if (map? options)
          [options colls]
          [{} (conj colls options)])
        opts (assoc real-options :label :concat)
        coll-list (if (and (= 1 (count all-colls)) (instance? PCollectionList (first all-colls)))
                    (first all-colls)
                    (PCollectionList/of all-colls))]
    (-> coll-list
        (.apply (with-opts base-schema opts
                  (Flatten/pCollections))))))

;; https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/Combine.Globally

(def base-combine-schema
  {:fanout {:docstr "Uses an intermediate node to combine parts of the data to reduce load on the final global combine step. Can be either an integer or a fn from key to integer (for combine-by-key scope)."
            :action (fn [transform fanout] (.withHotKeyFanout transform
                                                              (if (fn? fanout) (sfn fanout) fanout)))}
   :without-defaults {:docstr "Boolean indicating if the transform should attempt to provide a default value in the case of empty input."
                      :action (fn [transform] (.withoutDefaults transform))}})

(def combine-schema
  (merge
   base-schema
   base-combine-schema
   {:as-singleton-view {:docstr "The transform returns a PCollectionView whose elements are the result of combining elements per-window in the input PCollection."
                        :action (fn [transform] (.asSingletonView transform))}}))

(defn combine
  ([f options ^PCollection pcoll]
   (let [scope (or (:scope options) :global)
         opts (assoc options
                     :label (or (:label options) (keyword (str "combine-" scope)))
                     :scope scope)
         cfn (->combine-fn f)
         ready-pcoll (cond
                       (#{:local :per-key} scope) (.apply pcoll (with-opts combine-schema opts
                                                                  (Combine/perKey cfn)))
                       (#{:global :globally} scope) (.apply pcoll (with-opts combine-schema opts
                                                                    (Combine/globally cfn)))
                       :else (throw (ex-info (format "Option %s is not recognized" scope)
                                             {:scope-given scope :allowed-scopes #{:global :per-key}})))]
     (.setCoder ready-pcoll (or
                             (:coder opts)
                             (make-nippy-coder)))))
  ([f pcoll] (combine f {} pcoll)))

(defn- combine-by-transform
  [key-fn f options]
  (let [safe-opts (-> options
                      (dissoc :name)
                      (assoc :scope :per-key))]
    (proxy [PTransform] []
      (apply [^PCollection pcoll]
        (let [raw-pcoll (->> pcoll
                             (with-keys key-fn safe-opts)
                             (combine f safe-opts))]
          (if-not (:as-kv-output safe-opts)
            (dmap (fn [^KV y] [(.getKey y) (.getValue y)]) safe-opts raw-pcoll)
            raw-pcoll))))))

(defn combine-by
  ([key-fn f options ^PCollection pcoll]
   (let [opts (-> options
                  (assoc :label :combine-by :scope :per-key))]
     (.apply pcoll (with-opts base-schema opts
                     (combine-by-transform key-fn f options)))))
  ([key-fn f pcoll] (combine-by key-fn f {} pcoll)))

(defn sum
  ([{:keys [type] :as options} ^PCollection pcoll]
   (let [opts (assoc options :label :sum)
         cfn (case type
               :double  (Sum$SumDoubleFn.)
               :long    (Sum$SumLongFn.)
               :integer (Sum$SumIntegerFn.)
               +)]
     (combine cfn opts pcoll)))
  ([pcoll] (sum {} pcoll)))

(defn dmax
  ([{:keys [type initial-value] :as options} ^PCollection pcoll]
   (let [opts (assoc options :label :max)
         cfn (case type
               :double  (Max$MaxDoubleFn.)
               :long    (Max$MaxLongFn.)
               :integer (Max$MaxIntegerFn.)
               (Max$MaxFn. initial-value))]
     (combine cfn opts pcoll)))
  ([pcoll] (dmax {} pcoll)))

(defn dmin
  ([{:keys [type initial-value] :as options} ^PCollection pcoll]
   (let [opts (assoc options :label :max)
         cfn (case type
               :double  (Min$MinDoubleFn.)
               :long    (Min$MinLongFn.)
               :integer (Min$MinIntegerFn.)
               (Min$MinFn. initial-value))]
     (combine cfn opts pcoll)))
  ([pcoll] (dmin {} pcoll)))

(defn mean
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :mean)]
     (combine (Mean$MeanFn.) opts pcoll)))
  ([pcoll] (mean {} pcoll)))

(defn dcount
  {:doc (with-opts-docstr
          "Counts the elements in the input PCollection.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/Count.Globally.html

  example:

    (ds/count pcoll)"
          base-schema base-combine-schema)
   :added "0.1.0"}
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :count)]
     (-> pcoll
         (.apply (with-opts (merge base-schema base-combine-schema) opts
                   (Count$Globally.)))
         (cond-> (:coder opts) (.setCoder (:coder opts))))))
  ([pcoll] (dcount {} pcoll)))

(defn dfrequencies
  {:doc (with-opts-docstr
          "Returns the frequency of each unique element of the input PCollection.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/Count.PerElement.html

  Example:

    (ds/frequencies pcoll)"
          base-schema)
   :added "0.1.0"}
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :frequencies)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (Count$PerElement.)))
         (cond-> (:coder opts) (.setCoder (:coder opts))))))
  ([pcoll] (dfrequencies {} pcoll)))
