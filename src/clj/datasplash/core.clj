(ns ^:no-doc datasplash.core
  (:require
   [charred.api :as charred]
   [clj-stacktrace.core :as st]
   [clj-time.coerce :as timc]
   [clj-time.format :as timf]
   [clojure.edn :as edn]
   [clojure.java.shell :refer [sh]]
   [clojure.math.combinatorics :as combo]
   [clojure.tools.logging :as log]
   [superstring.core :as str]
   ;; to make aot work
   [taoensso.nippy :as nippy])
  (:import
   (clojure.lang ExceptionInfo MapEntry)
   (datasplash.fns ClojureCombineFn ClojureCustomCoder ClojureDoFn ClojurePTransform ClojureStatefulDoFn)
   (datasplash.pipelines PipelineWithOptions)
   (java.io DataInputStream DataOutputStream InputStream OutputStream StringWriter)
   (org.apache.beam.sdk Pipeline)
   (org.apache.beam.sdk.coders KvCoder StringUtf8Coder)
   (org.apache.beam.sdk.io Compression FileIO TextIO)
   (org.apache.beam.sdk.io.fs EmptyMatchTreatment)
   (org.apache.beam.sdk.options PipelineOptionsFactory)
   (org.apache.beam.sdk.transforms Combine
                                   Combine$CombineFn
                                   Contextful
                                   Count
                                   Create
                                   Distinct
                                   DoFn
                                   DoFn$ProcessContext
                                   Flatten
                                   GroupByKey
                                   ParDo
                                   Partition
                                   Partition$PartitionFn
                                   PTransform
                                   Sample
                                   SerializableFunction
                                   Sets
                                   View
                                   View$AsSingleton
                                   Watch$Growth
                                   WithKeys)
   (org.apache.beam.sdk.transforms.join CoGbkResult CoGroupByKey KeyedPCollectionTuple)
   (org.apache.beam.sdk.transforms.windowing BoundedWindow FixedWindows Sessions SlidingWindows Trigger Window)
   (org.apache.beam.sdk.util UserCodeException)
   (org.apache.beam.sdk.values KV PBegin PCollection PCollectionList PCollectionTuple PInput TupleTag TupleTagList)
   (org.joda.time DateTimeUtils DateTimeZone Duration Instant)
   (org.joda.time.format DateTimeFormat)))

(def required-ns (atom #{}))

(def fast-charred-write (charred/write-json-fn {:indent-str nil :escape-slash false}))

(defn write-json-str [data]
  (let [w (StringWriter.)]
    (fast-charred-write w data)
    (.toString w)))

(defn- ->duration
  [time]
  (or (and (instance? Duration time) time)
      (.toStandardDuration time)))

(defn val->clj
  [^KV kv]
  (let [v (.getValue kv)]
    (if (and
         (instance? Iterable v)
         (not (instance? java.util.Set v))
         (not (instance? java.util.Map v)))
      (vec v)
      v)))

(defn kv->clj
  "Coerce from KV to Clojure MapEntry."
  [^KV kv]
  (MapEntry. (.getKey kv) (val->clj kv)))

(defmethod print-method KV [kv ^java.io.Writer w]
  (.write w (pr-str (kv->clj kv))))

(defn unloaded-ns-from-ex
  [e]
  (loop [todo (st/parse-exception e)
         nss (list)]
    (let [{:keys [message trace-elems cause]} todo]
      (if message
        (if (re-find #"clojure\.lang\.Var\$Unbound|call unbound fn|dynamically bind non-dynamic var|Unbound:|Unable to resolve spec:" message)
          (let [[_ missing-ns] (or (re-find #"call unbound fn: #'([^/]+)/" message)
                                   (re-find #"Unbound: #'([^/]+)/" message)
                                   (re-find #"Can't dynamically bind non-dynamic var: ([^/]+)/" message)
                                   (re-find #"Unable to resolve spec: :([^/]+)/" message))
                ns-to-add (->> trace-elems
                               (filter :clojure)
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
         (str "unknown-hostname error (" (.getMessage e) ")"))))))

(defmacro try-deref
  [at]
  `(try (deref ~at) (catch ClassCastException e# (require 'datasplash.core) #{})))

(defmacro unwrap-ex-info
  [e]
  `(let [c# (.getCause ~e)]
     (if (and c# (instance? ~e UserCodeException) (instance? ExceptionInfo c#)) c# ~e)))

(defmacro safe-exec-cfg
  "Like [[safe-exec]], but takes a map as first argument containing the name of the ptransform for better error message."
  [config & body]
  `(let [pt-name# (-> ~config
                      (get :name)
                      (some-> (name)))]
     (try
       ~@body
       (catch ExceptionInfo e#
         (if (or (:name (ex-data e#)) (nil? pt-name#))
           (throw e#)
           (throw (ExceptionInfo. (.getMessage e#)
                                  (if pt-name# (assoc (ex-data e#) :name pt-name#) (ex-data e#))
                                  (if-let [root# (.getCause e#)] root# e#)))))
       (catch UserCodeException e#
         (throw e#))
       (catch Exception e#
         ;; if var is unbound, nothing has been required
         (let [required-at-start# (try-deref required-ns)]
           ;; About the use of clojure.lang.RT/REQUIRE_LOCK
           ;; https://ask.clojure.org/index.php/9893/require-is-not-thread-safe?show=9902#c9902
           (locking clojure.lang.RT/REQUIRE_LOCK
             (let [already-required# (try-deref required-ns)]
               (let [nss# (unloaded-ns-from-ex e#)]
                 (log/debugf "Caught exception %s at runtime with message -> %s => already initialized : %s / candidates for init : %s"
                             (type e#) (.getMessage e#) (into #{} already-required#) (into [] nss#))
                 (if (empty? nss#)
                   (throw (ex-info "Runtime exception intercepted"
                                   (-> {:hostname (get-hostname)}
                                       (cond-> pt-name# (assoc :name pt-name#))) e#))
                   (let [missings# nss# ;; (remove already-required# nss#)
                         missing-at-start?# (not (empty? (remove required-at-start# nss#)))]
                     (if-not (empty? missings#)
                       (do
                         (log/debugf "Requiring missing namespaces at runtime: %s" (into [] missings#))
                         (doseq [missing# missings#]
                           (require missing#)
                           (swap! required-ns conj missing#))
                         ~@body)
                       (if missing-at-start?#
                         ~@body
                         (do
                           (log/fatalf
                            "Dynamic reloading of namespace failure. Already required: %s Attempted: %s"
                            (into [] nss#) (into [] already-required#))
                           (throw (ex-info "Dynamic reloading of namespace seems not to work"
                                           (-> {:ns-from-exception (into [] nss#)
                                                :ns-load-attempted (into [] already-required#)
                                                :hostname (get-hostname)}
                                               (cond-> pt-name# (assoc :name pt-name#)))
                                           e#)))))))))))))))

(defmacro safe-exec
  "Executes body while trying to sanely require missing ns if the runtime is not yet properly loaded for Clojure in distributed mode. Always wrap try block with this if you intend to eat every Exception produced.
  ```
  (ds/map (fn [elt]
            (try
              (ds/safe-exec (dangerous-parse-fn elt))
              (catch Exception e
                (log/error e \"parsing error\"))))
          pcoll)
  ```"
  [& body]
  `(safe-exec-cfg {} ~@body))

(defn make-kv
  {:doc "Returns a KV object from the given arg(s), either [k v] or a MapEntry or seq of two elements."
   :added "0.1.0"}
  ([k v]
   (KV/of k v))
  ([kv] (make-kv (first kv) (second kv))))

(defn dkey
  {:doc "Returns the key part of a KV or MapEntry."
   :added "0.1.0"}
  [elt]
  (if (instance? KV elt)
    (let [^KV kv elt]
      (.getKey kv))
    (key elt)))

(defn dval
  {:doc "Returns the value part of a KV or MapEntry."
   :added "0.1.0"}
  [elt]
  (if (instance? KV elt)
    (val->clj ^KV elt)
    (val elt)))

(def ^{:dynamic true :no-doc true} *coerce-to-clj* true)
(def ^{:dynamic true :no-doc true} *context* nil)
(def ^{:dynamic true :no-doc true} *side-inputs* {})
(def ^{:dynamic true :no-doc true} *main-output* nil)
(def ^{:dynamic true :no-doc true} *extra* {})

(def ^{:private true :no-doc true} no-op (constantly nil))

(defn dofn
  {:doc "Returns an Instance of DoFn from given Clojure fn."
   :added "0.1.0"}
  ^DoFn
  ([f {:keys [start-bundle finish-bundle without-coercion-to-clj
              side-inputs side-outputs window-fn
              stateful? initialize-fn teardown-fn]
       :or {start-bundle no-op
            finish-bundle no-op
            window-fn no-op
            teardown-fn no-op}
       :as opts}]
   (let [process-ctx-fn (fn [^DoFn$ProcessContext context, ^java.util.Map extra]
                          (safe-exec-cfg
                           opts
                           (let [side-ins (persistent!
                                           (reduce
                                            (fn [acc [k pview]]
                                              (assoc! acc k (.sideInput context pview)))
                                            (transient {}) side-inputs))]
                             (binding [*context* context
                                       *coerce-to-clj* (not without-coercion-to-clj)
                                       *side-inputs* side-ins
                                       *main-output* (when side-outputs (first (sort side-outputs)))
                                       *extra* (persistent!
                                                (reduce
                                                 (fn [acc [k v]]
                                                   (assoc! acc (keyword k) v))
                                                 (transient {}) extra))]
                               (f context)))))
         args {"dofn" process-ctx-fn
               "window-fn" window-fn
               "start-bundle" start-bundle
               "finish-bundle" finish-bundle
               "initialize-fn" initialize-fn
               "teardown-fn" teardown-fn}]
     (if stateful?
       (ClojureStatefulDoFn. args)
       (ClojureDoFn. args))))
  ([f] (dofn f {})))

(defn context
  {:added "0.1.0"
   :doc "In the context of a ParDo, contains the corresponding Context object.
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/DoFn.ProcessContext.html"}
  [] *context*)

(defn state
  {:added "0.7.0"
   :doc "In the context of a ParDo, contains the mutable ValueState."}
  [] (*extra* :state))

(defn system
  {:added "0.7.0"
   :doc "In the context of a ParDo, contains the mutable ValueState."}
  [] (*extra* :system))

(defn side-inputs
  {:doc "In the context of a ParDo, returns the corresponding side inputs as a map from names to values.

  Example:
```
    (let [input (ds/generate-input [1 2 3 4 5] p)
          side-input (ds/view (ds/generate-input [{1 :a 2 :b 3 :c 4 :d 5 :e}] p))
          proc (ds/map (fn [x] (get-in (ds/side-inputs) [:mapping x]))
                       {:side-inputs {:mapping side-input}} input)])
```"
   :added "0.1.0"}
  [] *side-inputs*)

(defn get-element-from-context
  "Get element from context in ParDo while applying relevent Clojure type conversions."
  [^DoFn$ProcessContext c]
  (let [element (.element c)]
    (if *coerce-to-clj*
      (if (instance? KV element)
        (kv->clj element)
        element)
      element)))

(defrecord MultiResult [kvs])

(defn side-outputs
  "Returns multiple outputs keyed by keyword.
   Example:
   ```
(let [input (ds/generate-input [1 2 3 4 5] p)
      ;; simple and multi are pcoll with their respective elements)
      {:keys [simple multi]} (ds/map (fn [x] (ds/side-outputs :simple x :multi (* x 10)))
                                     {:side-outputs [:simple :multi]} input)])
   ```"
  [& kvs]
  (MultiResult. (partition 2 kvs)))

(defrecord TimeStamped [timestamp result])

(defn with-timestamp
  "Returns element(s) with the given timestamp as Timestamp. Anything that can be coerced by clj-time can be given as input.
   It can be nested inside a `(side-outputs)` or outside (in which case it applies to all results).
   Example:
  ```
  (ds/map (fn [e] (ds/with-timestamp (clj-time.core/now) (* 2 e)) pcoll))
  ```"
  [timestamp result]
  (->TimeStamped (Instant. (timc/to-long timestamp)) result))

(defn output-value!
  [^DoFn$ProcessContext context entity bindings]
  (let [{:keys [tag timestamp]} bindings]
    (cond
      (and tag timestamp) (if (= (name tag) (name *main-output*))
                            (.outputWithTimestamp context entity timestamp)
                            (.outputWithTimestamp context (TupleTag. (name tag)) entity timestamp))
      tag (if (= (name tag) (name *main-output*))
            (.output context entity)
            (.output context (TupleTag. (name tag)) entity))
      timestamp (.outputWithTimestamp context entity timestamp)
      :else (.output context entity))))

(defn output-to-context
  ([tx context result]
   (loop [todo [{:entity result :bindings {}}]]
     (when-let [{:keys [entity bindings]} (first todo)]
       (cond
         (instance? MultiResult entity) (recur
                                         (concat (rest todo)
                                                 (map (fn [[tag sub-entity]]
                                                        {:entity sub-entity
                                                         :bindings (assoc bindings :tag tag)})
                                                      (:kvs entity))))
         (instance? TimeStamped entity) (recur
                                         (conj (rest todo)
                                               {:entity (:result entity)
                                                :bindings (assoc bindings :timestamp (:timestamp entity))}))
         :else (do
                 (output-value! context (tx entity) bindings)
                 (recur (rest todo)))))))
  ([context result]
   (output-to-context identity context result)))

(defn clj->kv
  "Coerce from Clojure data to KV objects."
  ^KV
  [obj]
  (cond
    (instance? KV obj) obj
    (and (sequential? obj) (= 2 (count obj))) (KV/of (first obj) (second obj))
    :else (throw (ex-info "Cannot coerce given object to KV"
                          {:hostname (get-hostname)
                           :input-object obj
                           :input-object-type (type obj)}))))

(defn map-fn
  "Returns a function that corresponds to a Clojure map operation inside a ParDo."
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (get-element-from-context c)
          result (f elt)]
      (output-to-context c result))))

(defn map-kv-fn
  "Returns a function that corresponds to a Clojure map operation inside a ParDo coercing to KV the return."
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (get-element-from-context c)
          result (f elt)]
      (output-to-context clj->kv c result))))

(defn mapcat-fn
  "Returns a function that corresponds to a Clojure mapcat operation inside a ParDo."
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (get-element-from-context c)
          result (f elt)]
      (doseq [res result]
        (output-to-context c res)))))

(defn pardo-fn
  "Returns a function that uses the raw ProcessContext from ParDo."
  [f]
  (fn [^DoFn$ProcessContext c]
    (f c)))

(defn filter-fn
  "Returns a function that corresponds to a Clojure filter operation inside a ParDo."
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (get-element-from-context c)
          result (f elt)]
      (when result
        (.output c (.element c))))))

(defn keep-fn
  "Returns a function that map f on elements but keep only non nil results.
   It is almost like Clojure `keep` but it discards only nil values."
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (get-element-from-context c)
          result (f elt)]
      (when (some? result)
        (output-to-context c result)))))

(defn didentity
  {:doc "Identity function for use in a ParDo."
   :added "0.1.0"}
  [^DoFn$ProcessContext c]
  (.output c (.element c)))

(alter-var-root #'nippy/*thaw-serializable-allowlist*
                (fn [_]
                  (into nippy/default-thaw-serializable-allowlist
                        #{"org.apache.beam.sdk.values.KV"
                          "com.google.datastore.v1.Entity"})))

(defn make-nippy-coder
  {:doc "Returns an instance of a CustomCoder using nippy for serialization."
   :added "0.1.0"}
  []
  (let [encode-fn (fn [obj ^OutputStream out]
                    (safe-exec
                     (let [dos (DataOutputStream. out)]
                       (nippy/freeze-to-out! dos obj))))
        decode-fn (fn [^InputStream in]
                    (safe-exec
                     (let [dis (DataInputStream. in)]
                       (nippy/thaw-from-in! dis))))]
    (ClojureCustomCoder. {"decode-fn" decode-fn "encode-fn" encode-fn})))

(defn make-kv-coder
  {:doc "Returns an instance of a KvCoder using by default nippy for serialization."
   :added "0.1.0"}
  ([k-coder v-coder]
   (KvCoder/of k-coder v-coder))
  ([] (make-kv-coder (make-nippy-coder) (make-nippy-coder))))

(defn with-opts
  [schema opts ^PTransform ptransform]
  (reduce
   (fn [tr [k specs]]
     (if-let [v (get opts k)]
       (if-let [action (get specs :action)]
         (action tr v)
         tr)
       tr))
   ptransform schema))

(definterface IApply
  (apply [nam ptrans])
  (apply [ptrans]))

(defrecord GroupSpecs [specs]
  PInput
  (expand [_this] (into {} (map-indexed (fn [idx x] [(TupleTag. (str idx)) (first x)]) specs)))
  (getPipeline [_this] (let [^PInput pval (-> specs (first) (first))]
                         (.getPipeline pval)))
  IApply
  (apply [this nam ptrans] (Pipeline/applyTransform (name nam) this ptrans))
  (apply [this ptrans] (Pipeline/applyTransform this ptrans)))

(defn tapply
  [pcoll nam tr]
  (if (and nam (seq nam))
    (.apply pcoll nam tr)
    (.apply pcoll tr)))

(defn pcolltuple->map
  [^PCollectionTuple pcolltuple]
  (let [all (.getAll pcolltuple)]
    (persistent!
     (reduce
      (fn [acc [^TupleTag tag pcoll]]
        (assoc! acc (keyword (.getId tag)) pcoll))
      (transient {}) all))))

(declare write-edn-file)

(defn apply-transform
  "Apply the PTransform to the given Pcoll applying options according to schema."
  [pcoll ^PTransform transform schema
   {:keys [coder coll-name side-outputs checkpoint] :as options}]
  (let [nam (some-> options (:name) (name))
        clean-opts (dissoc options :name :coder :coll-name)
        configured-transform (with-opts schema clean-opts transform)
        bound (tapply pcoll nam configured-transform)
        rcoll (if-not side-outputs
                (-> bound
                    (cond-> coder (.setCoder coder))
                    (cond-> coll-name (.setName coll-name)))
                (let [pct (pcolltuple->map bound)]
                  (if coder
                    (do
                      (doseq [^PCollection pcoll (vals pct)]
                        (.setCoder pcoll coder))
                      pct)
                    pct)))]
    (when checkpoint
      (write-edn-file checkpoint rcoll))
    rcoll))

(defn with-opts-docstr
  [doc-string & schemas]
  (apply str doc-string "\n\nAvailable options:\n\n"
         (->> (for [schema schemas
                    [k {:keys [docstr enum default]}] schema]
                (-> (str "  - " k " => " docstr)
                    (cond-> enum (str " | One of " (if (map? enum) (keys enum) enum)))
                    (cond-> default (str " | Defaults to " default))
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

(defn select-enum-option-fn-set
  [options enum-map action]
  (fn [transform options-list]
    (let [enums (->  (select-keys enum-map options-list) vals set)]
      (if (seq enums)
        (action transform enums)
        (throw
         (ex-info (format "%s must be list of at least one of %s, %s given"
                          options (keys enum-map) options-list)
                  {:expected [(keys enum-map)]
                   :given options-list}))))))

(def base-schema
  {:coder {:docstr "Uses a specific Coder for the results of this transform. Usually defaults to some form of nippy-coder."}
   :checkpoint {:docstr "Given a path, will store the resulting pcoll at this path in edn to facilitate dev/debug."}})

(def named-schema
  (merge
   base-schema
   {:name {:docstr "Adds a name to the Transform."
           :action (fn [transform ^String n] (.named transform n))}}))

(def pardo-schema
  (merge
   named-schema
   {:side-inputs {:docstr "Adds a map of PCollectionViews as side inputs to the underlying ParDo Transform. They can be accessed there by key in the return of side-inputs fn."
                  :action (fn [transform inputs]
                            (.withSideInputs transform (map val (sort-by key inputs))))}
    :side-outputs {:docstr "Defines as a seq of keywords the output tags for the underlying ParDo Transform. The map fn should return a map with keys set to the same set of keywords."
                   :action (fn [transform kws]
                             (let [ordered (sort kws)]
                               (.withOutputTags transform
                                                (TupleTag. (name (first ordered)))
                                                (TupleTagList/of (map (comp #(TupleTag. %) name)
                                                                      (rest ordered))))))}
    :without-coercion-to-clj {:docstr "Avoids coercing Dataflow types to Clojure, like KV. Coercion will happen by default"}
    :initialize-fn {:doc-str "Function of 0 arguments called at worker init. It shoud return an initialized state that can be retrieved at runtime with [[system]] function."
                    :added "0.7.0"}}))

(defn map-op
  [transform {:keys [isomorph? kv?] :as base-options}]
  (fn make-map-op
    ([f {:keys [key-coder value-coder coder] :as options}
      ^PCollection pcoll]
     (let [default-coder (cond
                           isomorph? (.getCoder pcoll)
                           kv? (or coder
                                   (KvCoder/of
                                    (or key-coder (make-nippy-coder))
                                    (or value-coder (make-nippy-coder))))
                           :else (make-nippy-coder))
           opts (merge (assoc base-options :coder default-coder) options)
           ^DoFn bare-dofn (dofn (transform f) opts)
           pardo (ParDo/of bare-dofn)]
       (apply-transform pcoll pardo pardo-schema opts)))
    ([f pcoll] (make-map-op f {} pcoll))))

(def
  ^{:arglists [['f 'pcoll] ['f 'options 'pcoll]]
    :added "0.2.0"
    :doc
    (with-opts-docstr
      "Uses a raw pardo-fn as a pardo transform
Function f should be a function of one argument, the Pardo$Context object."
      pardo-schema)}
  pardo (map-op pardo-fn {:label :pardo}))

(def
  ^{:arglists [['f 'pcoll] ['f 'options 'pcoll]]
    :added "0.1.0"
    :doc
    (with-opts-docstr
      "Returns a PCollection of f applied to every item in the source PCollection.
Function f should be a function of one argument.

Example:
```
(ds/map inc foo)
(ds/map (fn [x] (* x x)) foo)
```

Note: Unlike clojure.core/map, datasplash.api/map takes only one PCollection."
      pardo-schema)}
  dmap (map-op map-fn {:label :map}))

(def
  ^{:arglists [['f 'pcoll] ['f 'options 'pcoll]]
    :added "0.1.0"
    :doc
    (with-opts-docstr
      "Returns a KV PCollection of f applied to every item in the source PCollection.
Function f should be a function of one argument and return seq of keys/values.

Example:
```
(ds/map-kv (fn [{:keys [month revenue]}] [month revenue]) foo)
```

Note: Unlike clojure.core/map, datasplash.api/map-kv takes only one PCollection."
      pardo-schema)}
  map-kv (map-op map-kv-fn {:label :map-kv :kv? true}))

(def
  ^{:arglists [['f 'pcoll] ['f 'options 'pcoll]]
    :added "0.1.0"
    :doc (with-opts-docstr
           "Returns the result of applying concat, or flattening, the result of applying
f to each item in the PCollection. Thus f should return a Clojure or Java collection.

Example:
```
(ds/mapcat (fn [x] [(dec x) x (inc x)]) foo)
```"
           pardo-schema)}
  dmapcat (map-op mapcat-fn {:label :mapcat}))

(def
  ^{:arglists [['pred 'pcoll] ['f 'options 'pcoll]]
    :added "0.1.0"
    :doc (with-opts-docstr
           "Returns a PCollection that only contains the items for which (pred item)
returns true.

  Example:
```
    (ds/filter even? foo)
(ds/filter (fn [x] (even? (* x x))) foo)
```"
           pardo-schema)}
  dfilter (map-op filter-fn {:label :filter :isomorph? true}))

(def
  ^{:arglists [['f 'pcoll] ['f 'options 'pcoll]]
    :added "0.7.17"
    :doc
    (with-opts-docstr
      "Returns a PCollection of non nil results of f applied to every item in the source PCollection.
Function f should be a function of one argument.

Example:
```
(ds/keep (fn [x] (when (even? x) x)) numbers-pcoll)
```

Note: Unlike Clojure `keep` it discards only nil results."
      pardo-schema)}
  dkeep (map-op keep-fn {:label :keep}))

(defn generate-input
  {:doc (with-opts-docstr
          "Generates a pcollection from the given collection.
Also accepts empty collections.
See https://beam.apache.org/documentation/transforms/java/other/create/

Example:
```
(ds/generate-input (range 0 1000) pipeline)
```"
          base-schema)
   :added "0.1.0"}
  ([coll options ^Pipeline p]
   (let [{:keys [coder] :as opts} (merge {:coder (make-nippy-coder)}
                                         (assoc options :label :generate-input))
         ptrans (if (empty? coll)
                  (Create/empty coder)
                  (-> (Create/of coll)
                      (.withCoder coder)))]
     (apply-transform p ptrans base-schema opts)))
  ([coll p] (generate-input coll {} p))
  ([p] (generate-input [] {} p)))

(definterface ICombineFn
  (getReduceFn [])
  (getExtractFn [])
  (getMergeFn [])
  (getInitFn []))

(defn combine-fn
  {:doc "Returns a CombineFn instance from given args.
See https://beam.apache.org/documentation/programming-guide/#combine

Arguments in order:

- reducef: adds element to accumulator: fn of two arguments, returns updated accumulator
```
(fn [acc elt] (assoc acc (ds/key elt) (ds/val elt)))
```
- extractf: fn taking a single accumulator as arg and returning the final result. Defaults to identity
- combinef: fn taking a variable number of accumulators and returning a single merged accumulator. Defaults to using the reduce fn
```
(fn [& accs] (apply merge accs))
```
- initf: fn of 0 args, returns empty accumulator. Defaults to reduce fn with no args
```
(fn [] {})
```
- output-coder: coder for the resulting PCollection. Defaults to nippy-coder
- acc-coder: coder for the accumulator. Defaults to nippy-coder


This function is reminiscent of the reducers api. In has sensible defaults in order to reuse existing functions. For example, this a a perfectly valid combine-fn that sums all numbers in a pcoll:
```
(combine-fn +)
```
"
   :added "0.1.0"}
  ^Combine$CombineFn
  ([reducef extractf combinef initf output-coder acc-coder]
   (let [extractf (or extractf identity)
         combinef (or combinef reducef)
         initf (or initf reducef)
         output-coder (or output-coder (make-nippy-coder))
         acc-coder (or acc-coder (make-nippy-coder))

         init-fn (fn [] (safe-exec (initf)))
         reduce-fn (fn [acc elt] (safe-exec (reducef acc elt)))
         combine-fn (fn [accs] (safe-exec (apply combinef accs)))
         extract-fn (fn [acc] (safe-exec (extractf acc)))]
     (ClojureCombineFn. {"init-fn" init-fn "reduce-fn" reduce-fn "combine-fn" combine-fn
                         "combine-fn-raw" combinef "extract-fn" extract-fn}
                        output-coder acc-coder)))

  ([reducef extractf combinef initf output-coder] (combine-fn reducef extractf combinef initf output-coder nil))
  ([reducef extractf combinef initf] (combine-fn reducef extractf combinef initf nil))
  ([reducef extractf combinef] (combine-fn reducef extractf combinef nil))
  ([reducef extractf] (combine-fn reducef extractf nil))
  ([reducef]
   (if (map? reducef)
     (apply combine-fn ((juxt :reduce :extract :combine :init :output-coder :acc-coder) reducef))
     (combine-fn reducef nil))))

(def view-schema
  (merge
   base-schema
   {:default {:docstr "Sets a default value for SingletonView"
              :action (fn [transform v]
                        (assert (instance? transform View$AsSingleton) "Default values can only be set for Singleton views")
                        (if v
                          (.withDefaultValue transform v)
                          transform))}
    :type {:docstr "Type of View"
           :enum [:singleton :iterable :list :map :multi-map]
           :default :singleton}}))

(defn view
  {:doc (with-opts-docstr
          "Produces a View out of a PColl, to be later consumed as a side-input for example.

          See https://beam.apache.org/documentation/transforms/java/other/view/"
          view-schema)
   :added "0.1.0"}
  ([{:keys [type]
     :or {type :singleton}
     :as options}
    pcoll]
   (let [opts (assoc options :label :view :coder nil)
         ptrans (case type
                  :singleton (View/asSingleton)
                  :iterable  (View/asIterable)
                  :list      (View/asList)
                  :map       (View/asMap)
                  :multi-map (View/asMultimap))]
     (apply-transform pcoll ptrans view-schema opts)))
  ([pcoll] (view {} pcoll)))

(defn- to-edn*
  [^DoFn$ProcessContext c]
  (let [elt (.element c)
        result (pr-str elt)]
    (.output c result)))

(def to-edn (partial (map-op identity {:label :to-edn :coder (StringUtf8Coder/of)}) to-edn*))
(def from-edn (partial dmap #(edn/read-string %)))

(defn sfn
  "Returns an instance of SerializableFunction equivalent to f."
  ^SerializableFunction
  [f]
  (reify
    SerializableFunction
    (apply [_this input]
      (safe-exec (f input)))
    clojure.lang.IFn
    (invoke [_this input]
      (safe-exec (f input)))))

(defn partition-fn
  "Returns a Partition.PartitionFn if possible."
  ^Partition$PartitionFn
  [f]
  (if (instance? Partition$PartitionFn f)
    f
    (reify
      Partition$PartitionFn
      (partitionFor [_this elem num]
        (safe-exec (f elem num))))))

(defn dpartition-by
  {:doc (with-opts-docstr
          "Partitions the content of pcoll according to the PartitionFn.
See https://beam.apache.org/documentation/programming-guide/#partition
The partition function is given two arguments: the current element and the number of partitions."
          named-schema)
   :added "0.1.0"}
  ([f num options ^PCollection pcoll]
   (let [opts (assoc options :label :partition-by)
         ptrans (Partition/of num (partition-fn f))]
     (apply-transform pcoll ptrans base-schema opts)))
  ([f num pcoll] (dpartition-by f num {} pcoll)))

(defn ->combine-fn
  "Returns a CombineFn if f is not one already."
  [f]
  (if (or
       (instance? Combine$CombineFn f)
       (instance? SerializableFunction f))
    f
    (combine-fn f)))

(defn djuxt
  {:doc "Creates a CombineFn that applies multiple combiners in one go. Produces a vector of combined results.
'sibling fusion' in Dataflow optimizes multiple independant combiners in the same way, but you might find juxt more concise.

Only works with functions created with combine-fn or native clojure functions, and not with native Dataflow CombineFn

Example:
```
(ds/combine (ds/juxt + *) pcoll)
```"
   :added "0.1.0"}
  [& fns]
  (let [cfs (map ->combine-fn fns)]
    (combine-fn
     (fn [accs elt]
       (into []
             (map-indexed
              (fn [idx acc] (let [f (.getReduceFn (nth cfs idx))]
                              (f acc elt))) accs)))
     (fn [accs]
       (into [] (map-indexed
                 (fn [idx acc] (let [f (.getExtractFn (nth cfs idx))]
                                 (f acc))) accs)))
     (fn [& accs]
       (into []
             (map-indexed
              (fn [idx cf] (let [f (.getMergeFn cf)]
                             (apply f (mapv (fn [acc] (nth acc idx)) accs))))
              cfs)))
     (fn []
       (mapv (fn [cf] (let [f (.getInitFn cf)]
                        (f))) cfs))
     (.getDefaultOutputCoder (first cfs) nil nil)
     (.getAccumulatorCoder (first cfs) nil nil))))

(def kv-coder-schema
  {:key-coder {:docstr "Coder to be used for encoding keys in the resulting KV PColl."}
   :value-coder {:docstr "Coder to be used for encoding values in the resulting KV PColl."}})

(defn with-keys
  {:doc (with-opts-docstr
          "Returns a PCollection of KV by applying f on each element of the input PCollection and using the return value as the key and the element as the value.

See https://beam.apache.org/documentation/transforms/java/elementwise/withkeys/

Example:
```
(with-keys even? pcoll)
```"
          base-schema kv-coder-schema)
   :added "0.1.0"}
  ([f {:keys [key-coder value-coder coder] :as options} ^PCollection pcoll]
   (let [opts (assoc options
                     :coder (or coder
                                (KvCoder/of
                                 (or key-coder (make-nippy-coder))
                                 (or value-coder (.getCoder pcoll))))
                     :label :with-keys)
         ptrans (WithKeys/of (sfn f))]
     (apply-transform pcoll ptrans base-schema opts)))
  ([f pcoll] (with-keys f {} pcoll)))

(defn group-by-key
  {:doc "Takes a KV PCollection as input and returns a KV PCollection as output of K to list of V.

See https://beam.apache.org/documentation/programming-guide/#groupbykey"
   :added "0.1.0"}
  ([options ^PCollection pcoll]
   (let [;parent-coder (.getCoder pcoll)
         opts (assoc options :label :group-by-keys)]
     (apply-transform pcoll (GroupByKey/create) base-schema opts)))
  ([pcoll] (group-by-key {} pcoll)))

(defmacro ptransform
  {:doc "Generates a PTransform with the given name, apply signature and body. Should rarely by used in user code, see [[pt->>]] for the more general use case in application code.

Example (actual implementation of the group-by transform):
```
(ptransform
 :group-by
 [^PCollection pcoll]
 (->> pcoll
      (ds/with-keys f opts)
      (ds/group-by-key opts)))
```"
   :added "0.1.0"}
  [_ input & body]
  `(let [body-fn# (fn [~(last input)] ~@body)]
     (ClojurePTransform. body-fn#)))

(defmacro pt->>
  {:doc "Creates and applies a single named PTransform from a sequence of transforms on a single PCollection. You can use it as you would use ->> in Clojure.

Example:
```
(ds/pt->> :transform-name input-pcollection
          (ds/map inc {:name :inc})
          (ds/filter even? {:name :even?}))
```"
   :added "0.2.0"}
  [nam input & body]
  `(let [ptrans# (ptransform
                  ~nam
                  [pcoll#]
                  (->> pcoll#
                       ~@body))]
     (apply-transform ~input ptrans# base-schema {:name ~nam})))

(defmacro pt-cond->>
  {:doc "Creates and applies a single named PTransform from a sequence of transforms on a single PCollection according to the results of the given predicates. You can use it as you would use cond->> in Clojure.

Example:
```
(ds/cond->> :transform-name input-pcollection
          (:do-inc? config) (ds/map inc {:name :inc})
          (:do-filter? config) (ds/filter even? {:name :even?}))
```"
   :added "0.2.3"}
  [nam input & body]
  `(let [ptrans# (ptransform
                  ~nam
                  [pcoll#]
                  (cond->> pcoll#
                    ~@body))]
     (apply-transform ~input ptrans# base-schema {:name ~nam})))

(defn- group-by-transform
  [f options]
  (let [safe-opts (dissoc options :name)]
    (ptransform
     :group-by
     [^PCollection pcoll]
     (->> pcoll
          (with-keys f safe-opts)
          (group-by-key safe-opts)))))

(defn dgroup-by
  {:doc (with-opts-docstr
          "Groups a Pcollection by the result of calling (f item) for each item.

This produces a sequence of KV values, similar to using seq with a
map. Each value will be a list of the values that match key.

  Example:
```
    (ds/group-by :a foo)
(ds/group-by count foo)
```"
          base-schema)
   :added "0.1.0"}
  ([f {:keys [_key-coder _value-coder coder] :as options} ^PCollection pcoll]
   (let [opts (-> options
                  (assoc :coder (or coder nil))
                  (assoc :label :group-by))
         ptrans (group-by-transform f opts)]
     (apply-transform pcoll ptrans base-schema opts)))
  ([f pcoll] (dgroup-by f {} pcoll)))

(defn interface->class
  [itf]
  (if (instance? Class itf)
    itf
    (Class/forName (name itf))))

(def ^:dynamic *pipeline-builder-caller* "unknown")

(defn create-timestamp
  []
  (let [formatter (-> "MMddHHmmss"
                      (DateTimeFormat/forPattern)
                      (.withZone DateTimeZone/UTC))]
    (.print formatter (DateTimeUtils/currentTimeMillis))))

(defn job-name-template
  [tpl args]
  (-> tpl
      (str/replace #"%U" (or (System/getProperty "user.name")
                             (:user (:job args))
                             "nemo"))
      (str/replace #"%A" *pipeline-builder-caller*)
      (str/replace #"%T" (create-timestamp))
      (str/lower-case)
      (str/replace #"[^-a-z0-9]" "0")))

(defn args->cli-args
  "Merge args and convert to cli args to be used with `PipelineOptionsFactory/fromArgs`."
  [str-args kw-args]
  (let [atomic-args (into {} (map (fn [kv]
                                    (let [[k v] (str/split kv #"=" 2)]
                                      [(str/camel-case (str/replace k #"^--" "")) v]))
                                  str-args))
        clean-args (into {} (map (fn [[k v]] [(str/camel-case (name k)) v]) kw-args))
        args (merge clean-args atomic-args)
        args-with-name (if (args "appName")
                         args
                         (assoc args "appName" *pipeline-builder-caller*))
        args-with-jobname (if-let [tpl (args-with-name "jobNameTemplate")]
                            (-> args-with-name
                                (assoc "jobName" (job-name-template tpl args-with-name))
                                (dissoc "jobNameTemplate"))
                            args-with-name)]
    (->> args-with-jobname
         (mapcat (fn [[k v]]
                   (for [v (if (coll? v) v [v])]
                     (str "--" k "=" v))))
         (mapv (fn [x] (str/replace x #"=$" ""))))))

(defn make-pipeline*
  ([itf str-args kw-args]
   (let [reformed-args (args->cli-args str-args kw-args)
         builder (PipelineOptionsFactory/fromArgs
                  (into-array String reformed-args))
         options (if itf
                   (.as builder (interface->class itf))
                   (.create builder))
         pipeline (PipelineWithOptions/create options)
         coder-registry (.getCoderRegistry pipeline)]
     (doto coder-registry
       (.registerCoderForClass clojure.lang.IPersistentCollection (make-nippy-coder))
       (.registerCoderForClass clojure.lang.Keyword (make-nippy-coder)))
     pipeline))
  ([arg1 arg2]
   (if (or (symbol? arg1) (string? arg1))
     (make-pipeline* arg1 arg2 {})
     (make-pipeline* nil arg1 arg2)))
  ([arg]
   (cond (or (symbol? arg) (string? arg)) (make-pipeline* arg [] {})
         (seq? arg) (make-pipeline* nil arg {})
         :else (make-pipeline* nil [] arg))))

(defmacro make-pipeline
  {:doc "Builds a Pipeline from command lines args and configuration.
Also accepts a jobNameTemplate param which is a string in which the following var are interpolated:

  - %A -> Application name
  - %U -> User name
  - %T -> Timestamp

It means the template %A-%U-%T is equivalent to the default jobName.

See https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline"
   :added "0.1.0"}
  [& args]
  `(binding [*pipeline-builder-caller* ~(str *ns*)]
     (make-pipeline* ~@args)))

(defn run-pipeline
  {:doc "Run the computation for a given pipeline or PCollection.

  See https://beam.apache.org/documentation/pipelines/create-your-pipeline/#running-your-pipeline"
   :added "0.1.0"}
  [topology]
  (if (instance? Pipeline topology)
    (.run topology)
    (-> topology
        (.getPipeline)
        (.run))))

(defn wait-pipeline-result
  {:doc "Blocks until this PipelineResult finishes. Returns the final state."
   :added "0.5.1"}
  ([pip-res timeout]
   (-> (if-not timeout
         (.waitUntilFinish pip-res)
         (.waitUntilFinish pip-res timeout))
       (.name)
       (str/lower-case)
       (keyword)))
  ([pip-res]
   (wait-pipeline-result pip-res nil)))

(defn get-pipeline-options
  {:doc "Returns a map corresponding to the bean of options the given pipeline was built with. Must be called on a `PipelineWithOptions` object produced by `make-pipeline`."
   :added "0.5.2"}
  [^PipelineWithOptions pipeline]
  (some-> pipeline
          (.getPipelineOptions)
          (bean)
          (dissoc :class)))

(defn get-pipeline-configuration
  {:doc "Returns a map corresponding to the bean of configuration the current pipeline was run with. Must be called inside a function wrapping a ParDo, e.g. ds/map or ds/mapcat."
   :added "0.1.0"}
  ([]
   (when-let [^DoFn$ProcessContext c *context*]
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

(defn split-path
  [p]
  (let [[_ base-path filename :as all] (->> (re-find #"^(.*/)([^/]*)$" p)
                                            (remove #(= "" %)))
        filename (or filename (when (empty? all) p))]
    [base-path filename]))

(defn ->options
  [o]
  (if (instance? Pipeline o)
    (.getOptions o)
    o))

(def compression-type-enum
  (into {}
        (map (fn [compression]
               (vector (-> (.name compression) str/lower-case keyword)
                       compression)))
        (Compression/values)))

(def empty-match-treatment-enum
  {:allow EmptyMatchTreatment/ALLOW
   :allow-if-wildcard EmptyMatchTreatment/ALLOW_IF_WILDCARD
   :disallow EmptyMatchTreatment/DISALLOW})

(def text-reader-schema
  {:many-files {:docstr "Hints that the filepattern specified matches a very large number of files."
                :action (fn [transform _] (.withHintMatchesManyFiles transform))}
   :empty-match-treatment {:docstr "Options for allowing or disallowing filepatterns that match no resources"
                           :enum empty-match-treatment-enum
                           :action (select-enum-option-fn
                                    :empty-match-treatment
                                    empty-match-treatment-enum
                                    (fn [transform enum] (.withEmptyMatchTreatment transform enum)))}
   :delimiter {:docstr "Specify delimiter"
               :action (fn [transform delimiter] (.withDelimiter transform delimiter))}
   :compression-type {:docstr "Choose compression type. :auto by default."
                      :enum compression-type-enum
                      :action (select-enum-option-fn
                               :compression-type
                               compression-type-enum
                               (fn [transform enum] (.withCompression transform enum)))}
   :watch-new-files {:docstr "watch if new files arrives and handle them in streaming"
                     :action (fn [transform {:keys [poll-interval termination-strategy termination-duration]}]
                               (.watchForNewFiles transform
                                                  (->duration poll-interval)
                                                  (case termination-strategy
                                                    :never (Watch$Growth/never)
                                                    :after (Watch$Growth/afterTotalOf
                                                            (->duration termination-duration))
                                                    :inactivity (Watch$Growth/afterTimeSinceNewOutput
                                                                 (->duration termination-duration))
                                                    termination-strategy)))}})

(def text-writer-schema
  {:file-format {:docstr "Choose file format."
                 :action
                 (fn [transform file-format]
                   (if (= :default file-format)
                     (.via transform (TextIO/sink))
                     (.via transform
                           (Contextful/fn
                             (sfn
                              (fn [x]
                                (case file-format
                                  :json (write-json-str x)
                                  :edn (pr-str x)
                                  (file-format x)))))
                           (TextIO/sink))))}
   :compression-type {:docstr "Choose compression type."
                      :enum compression-type-enum
                      :action (select-enum-option-fn
                               :compression-type
                               compression-type-enum
                               (fn [transform enum] (.withCompression transform enum)))}
   :num-shards {:docstr "Selects the desired number of output shards (file fragments). 0 to let the system decide (recommended)."
                :action (fn [transform shards] (.withNumShards transform shards))}

   :temp-directory {:docstr "Use temp directory when using Filename Policy as output (see filename-policy fn)"
                    :action (fn [transform prefix] (when prefix
                                                     (.withTempDirectory transform prefix)))}

   :suffix {:docstr "Uses the given filename suffix."
            :action (fn [transform suffix] (.withSuffix transform suffix))}
   :prefix {:docstr "Uses the given filename prefix."
            :action (fn [transform suffix] (.withPrefix transform suffix))}
   :naming-fn {:docstr "Uses the naming fn"
               :action (fn [transform naming-fn] (.withNaming transform (sfn naming-fn)))}
   :dynamic-fn {:docstr "Uses the dynamic write to change destination file according to the content of the item"}})

(defn write-text-file
  {:doc (with-opts-docstr
          "Writes a PCollection of Strings to disk or Google Storage, with records separated by newlines.

See https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TextIO.Write.html

  Example:
```
    (write-text-file \"gs://target/path\" pcoll)
```"
          base-schema text-writer-schema)
   :added "0.6.2"}
  [to {:keys [dynamic? dynamic-fn file-format] :as options} ^PCollection pcoll]
  (let [[base-path filename] (split-path to)
        opts (-> options
                 (assoc :label (str "write-text-file-to-"
                                    (clean-filename to))
                        :coder nil
                        :file-format (or file-format :default))
                 (cond-> filename (assoc :prefix filename)))]
    (apply-transform pcoll
                     (-> (if dynamic?
                           (-> (FileIO/writeDynamic)
                               (.withDestinationCoder (make-nippy-coder))
                               (.by (sfn dynamic-fn)))
                           (FileIO/write))
                         (.to (or base-path "./")))
                     (merge named-schema text-writer-schema) opts)))

(defn read-text-file
  {:doc (with-opts-docstr "Reads a PCollection of Strings from disk or Google Storage, with records separated by newlines.

See https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TextIO.Read.html

Example:
```
(read-text-file \"gs://target/path\" pcoll)
```"
          base-schema text-reader-schema)
   :added "0.1.0"}
  ([from options p]
   (let [opts (assoc options
                     :coder (or
                             (:coder options)
                             (StringUtf8Coder/of)))]
     (-> p
         (cond-> (instance? Pipeline p) (PBegin/in))
         (apply-transform (.from (TextIO/read) from)
                          (merge named-schema text-reader-schema) opts))))
  ([from p] (read-text-file from {} p)))

(defn read-text-files
  {:doc (with-opts-docstr "Reads multiple text files from a PCollection of Strings from disk or Google Storage, with records separated by newlines.

See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/TextIO.ReadFiles.html

Example:
```
(->> (ds/generate-input [\"gs://target/path\" \"gs://target/another-path\"] pipeline)
     (read-text-files))
```")}
  ([options ^PCollection from]
   (let [opts (assoc options
                     :coder (or
                             (:coder options)
                             (StringUtf8Coder/of)))
         transform (ptransform
                    (or (:name options) "read-text-files")
                    [^PCollection pcoll]
                    (-> pcoll
                        (apply-transform (FileIO/matchAll)
                                         {} {})
                        (apply-transform (FileIO/readMatches)
                                         {} {})
                        (apply-transform (TextIO/readFiles)
                                         (merge named-schema text-reader-schema) (dissoc opts :name))))]
     (apply-transform from transform (merge {:name "read-text-files"} options) opts)))
  ([from] (read-text-files {} from)))

(defn read-edn-file
  {:doc (with-opts-docstr "Reads a PCollection of edn strings from disk or Google Storage, with records separated by newlines.
See https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/TextIO.Read.html
Example:
```
(read-edn-file \"gs://target/path\" pcoll)
```"
          base-schema text-reader-schema)
   :added "0.1.0"}
  ([from options ^Pipeline p]
   (let [opts (assoc options
                     :coder (or (:coder options) (make-nippy-coder)))]
     (pt->>
      (or (:name opts) (str "read-text-file-from"
                            (clean-filename from)))
      p
      (read-text-file from (-> opts
                               (dissoc :coder)
                               (assoc :name "read-text-file")))
      (from-edn (assoc opts :name "parse-edn")))))
  ([from p] (read-edn-file from {} p)))

(defn read-edn-files
  {:doc (with-opts-docstr "Reads multiple EDN files from a PCollection of Strings from disk or Google Storage, with records separated by newlines.

See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/TextIO.ReadFiles.html

Example:
```
(->> (ds/generate-input [\"gs://target/path\" \"gs://target/another-path\"] pipeline)
     (read-edn-files))
```"
          base-schema text-reader-schema)
   :added "0.6.5"}
  ([options ^PCollection from]
   (let [opts (assoc options
                     :coder (or (:coder options) (make-nippy-coder)))]
     (pt->>
      (or (:name opts) "read-edn-files")
      from
      (read-text-files (-> opts
                           (dissoc :coder)
                           (assoc :name "read-edn-files")))
      (from-edn (assoc opts :name "parse-edn")))))
  ([from] (read-edn-files {} from)))

(defn write-edn-file
  {:doc (with-opts-docstr
          "Writes a PCollection of data to disk or Google Storage, with edn records separated by newlines.

See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/TextIO.Write.html

Example:
```
(write-edn-file \"gs://target/path\" pcoll)
```"
          base-schema text-writer-schema)
   :added "0.1.0"}
  ([to options ^PCollection pcoll]
   (write-text-file to (assoc options :file-format :edn) pcoll))
  ([to pcoll] (write-edn-file to {} pcoll)))

(def json-reader-schema
  {:key-fn {:docstr "Selects a policy for parsing map keys. If true, keywordizes the keys. If given a fn, uses it to transform each map key."}
   :return-type {:docstr "Allows passing in a function to specify what kind of types to return."}})

(defn read-json-file
  {:doc (with-opts-docstr "Reads a PCollection of JSON strings from disk or Google Storage, with records separated by newlines.

See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/TextIO.Read.html for options.
JSON decode options include:
  - :key-fn - custom key transformation fn
  - :return-type - custom value transformation fn
  - :eof-error? - if the pipeline should fail on unexpected eof (default false)

Example:
```
(read-json-file \"gs://target/path\" pcoll)
```"
          base-schema text-reader-schema json-reader-schema)
   :added "0.2.0"}
  ([from {:keys [key-fn return-type eof-error?] :as options} ^Pipeline p]
   (let [opts (assoc options
                     :label (str "read-json-file-from-"
                                 (clean-filename from))
                     :coder (or (:coder options) (make-nippy-coder)))
         initialize-fn (fn []
                         (safe-exec
                          (charred/parse-json-fn (cond-> {:eof-error? false :eof-value nil}
                                                   key-fn (assoc :key-fn key-fn)
                                                   return-type (assoc :value-fn return-type)
                                                   eof-error? (assoc :eof-error? eof-error?)))))
         decode-fn (fn [s] ((system) s))]
     (pt->>
      (or (:name opts) (str "read-json-file-from-" (clean-filename from)))
      p
      (read-text-file from (-> opts
                               (dissoc :coder)
                               (assoc :name "read-text-file")))
      (dmap decode-fn
            (assoc opts :name "json-decode" :initialize-fn initialize-fn)))))
  ([from p] (read-json-file from {} p)))

(defn read-json-files
  {:doc (with-opts-docstr "Reads multiple JSON files from a PCollection of Strings from disk or Google Storage, with records separated by newlines.

See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/TextIO.ReadFiles.html
JSON decode options include:
  - :key-fn - custom key transformation fn
  - :return-type - custom value transformation fn
  - :eof-error? - if the pipeline should fail on unexpected eof (default false)

Example:
```
(->> (ds/generate-input [\"gs://target/path\" \"gs://target/another-path\"] pipeline)
     (read-json-files))
```"
          base-schema json-reader-schema)
   :added "0.6.5"}
  ([{:keys [key-fn return-type eof-error?] :as options} ^PCollection from]
   (let [opts (assoc options
                     :label (str "read-json-file-from-"
                                 (clean-filename from))
                     :coder (or (:coder options) (make-nippy-coder)))
         decode-fn  #(charred/read-json % (cond-> {:eof-error? false :eof-value nil}
                                            key-fn (assoc :key-fn key-fn)
                                            return-type (assoc :value-fn return-type)
                                            eof-error? (assoc :eof-error? eof-error?)))]
     (pt->>
      (or (:name opts) "read-json-files")
      from
      (read-text-files (-> opts
                           (dissoc :coder)
                           (assoc :name "read-json-files")))
      (dmap decode-fn
            (assoc options :name "json-decode")))))
  ([from] (read-json-files {} from)))

(def json-writer-schema
  {:date-format {:docstr "Pattern for encoding java.util.Date objects. Defaults to yyyy-MM-dd'T'HH:mm:ss'Z'"}
   :escape-non-ascii {:docstr "Generate JSON escaping UTF-8."}
   :key-fn {:docstr "Generate JSON and munge keys with a custom function."}})

(defn write-json-file
  {:doc (with-opts-docstr
          "Writes a PCollection of data to disk or Google Storage, with JSON records separated by newlines.

See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/TextIO.Write.html and https://github.com/dakrone/cheshire#encoding for details on options

Example:
```
(write-json-file \"gs://target/path\" pcoll)
```"
          base-schema text-writer-schema json-writer-schema)
   :added "0.2.0"}
  ([to options ^PCollection pcoll]
   (let [[base-path filename] (split-path to)]
     (write-text-file (or base-path "./")
                      (cond-> (assoc options :file-format :json)
                        filename (assoc :prefix filename)) pcoll)))
  ([to pcoll] (write-json-file to {} pcoll)))

(defn make-partition-mapping
  [coll]
  (zipmap coll (range (count coll))))

(defn reverse-map
  [m]
  (zipmap (vals m) (keys m)))

(defn write-text-file-by-transform
  [output-transform f mapping to options]
  (let [safe-opts (dissoc options :name)]
    (ptransform
     :write-file-by
     [^PCollection pcoll]
     (let [map-fn (fn [out] (get mapping out 0))
           mapped-fn (comp map-fn (fn [elt _] (f elt)))
           ^PCollectionList pcolls (dpartition-by mapped-fn (count mapping) safe-opts pcoll)
           target (if (fn? to) to (fn [out] (str to out)))
           reverse-mapping (reverse-map mapping)
           files-list (map-indexed (fn [idx coll] [idx (target (get reverse-mapping idx)) coll]) (.getAll pcolls))]
       (doseq [[idx path coll] files-list]
         (output-transform path (assoc safe-opts :name (str "write-partial-file-" idx)) coll))
       pcoll))))

;;;;;;;;;;;
;; Joins ;;
;;;;;;;;;;;

;;(defn- ->tuple-tag [x] (TupleTag. (str x)))

(defn make-keyed-pcollection-tuple
  [pcolls]
  (let [pipeline (.getPipeline (first pcolls))
        tag-pcoll (map (fn [i pcoll] [(TupleTag. (str i)) pcoll])
                       (range)
                       pcolls)
        pcolltuple (reduce
                    (fn [coll-tuple [tag pcoll]] (.and coll-tuple tag pcoll))
                    (KeyedPCollectionTuple/empty pipeline)
                    tag-pcoll)]
    {:ordered-tags (mapv first tag-pcoll)
     :pcolltuple pcolltuple}))

(defn make-group-specs
  [specs]
  (if (instance? GroupSpecs specs)
    specs
    (let [safe-specs (into []
                           (doall
                            (for [[pcoll key-fn spec] specs]
                              (cond
                                (nil? spec) [pcoll key-fn {:type :optional}]
                                (keyword? spec) [pcoll key-fn {:type spec}]
                                (map? spec) [pcoll key-fn spec]
                                :else (throw (ex-info "Invalid spec for cogroup" {:specs specs}))))))]
      (GroupSpecs. safe-specs))))

(defn greedy-read-cogbkresult
  [raw-values tag]
  (loop [^java.util.Iterator it (.iterator (.getAll raw-values tag))
         acc (list)]
    (if (.hasNext it)
      (recur it (conj acc (.next it)))
      acc)))

(defn greedy-emit-cogbkresult
  [raw-values size idx tag ^DoFn$ProcessContext context]
  (loop [^java.util.Iterator it (.iterator (.getAll raw-values tag))]
    (when (.hasNext it)
      (let [v (.next it)
            res (into []
                      (for [i (range size)]
                        (if (= idx i) (list v) (list))))]
        (.output context (make-kv nil res))
        (recur it)))))

(defn cogroup-transform
  ([{:keys [join-nil?] nam :name :as options}]
   (let [opts (assoc options :label :raw-cogroup)]
     (ptransform
      :cogroup
      [^GroupSpecs group-specs]
      (safe-exec-cfg
       options
       (let [root-name (if nam (name nam) "cogroup")
             pcolls (for [[_idx [pcoll f {:keys [drop-nil?] :as _opts}]]
                          (map-indexed (fn [idx s] (if (instance? PCollection s)
                                                     [idx [s nil nil]] [idx s])) (:specs group-specs))]
                      (let [local-name (str root-name "-" (if pcoll (.getName pcoll) "pcoll"))
                            op (if f
                                 (with-keys f {:name (str local-name "-group-by")} pcoll)
                                 pcoll)]
                        (if drop-nil?
                          (dfilter (if f
                                     (fn [^KV kv] (not (nil? (.getKey kv))))
                                     (fn [v] (not (nil? v))))
                                   {:name (str local-name "-drop-nil")
                                    :without-coercion-to-clj true}
                                   op)
                          op)))
             {:keys [pcolltuple ordered-tags]} (make-keyed-pcollection-tuple pcolls)
             rel (apply-transform pcolltuple (CoGroupByKey/create) base-schema opts)
             required-set (->> (:specs group-specs)
                               (map-indexed (fn [idx [_ _ {:keys [type]}]]
                                              (when (= type :required) idx)))
                               (remove nil?)
                               (into #{}))
             required-count (count required-set)
             final-rel (pardo
                        (fn [^DoFn$ProcessContext c]
                          (let [^KV kv (.element c)
                                k (.getKey kv)
                                ^CoGbkResult raw-values (.getValue kv)]
                            ;; skip if a required part of the group is empty
                            (when-not (some identity
                                            (map-indexed (fn [idx tag]
                                                           (if (required-set idx)
                                                             (not (-> (.getAll raw-values tag)
                                                                      (.iterator)
                                                                      (.hasNext)))
                                                             false))
                                                         ordered-tags))
                              (if (and (not join-nil?) (nil? k))
                                (cond
                                  (= required-count 0) (doseq [[idx tag]
                                                               (map-indexed
                                                                (fn [idx tag] [idx tag]) ordered-tags)]
                                                         (greedy-emit-cogbkresult
                                                          raw-values (count ordered-tags) idx tag c))
                                  (= required-count 1) (greedy-emit-cogbkresult
                                                        raw-values
                                                        (count ordered-tags)
                                                        (first required-set)
                                                        (nth ordered-tags
                                                             (first required-set))
                                                        c)
                                  :else nil)
                                (let [values (mapv
                                              (fn [tag]
                                                (greedy-read-cogbkresult raw-values tag))
                                              ordered-tags)]
                                  (.output c (make-kv k values)))))))
                        (assoc opts
                               :name (str root-name "-apply-requirements")
                               :without-coercion-to-clj true
                               :coder (make-kv-coder (.getKeyCoder (.getCoder rel))
                                                     (make-nippy-coder)))
                        rel)]
         final-rel))))))

(defn cogroup-by-transform
  [{:keys [collector] :as options}]
  (ptransform
   :cogroup-by
   [^GroupSpecs group-specs]
   (let [grouped-colls (apply-transform group-specs (cogroup-transform options) named-schema options)
         root-name (or (:name options) "cogroup-by")]
     (if collector
       (dmap collector (assoc options :name (str root-name "-collector")) grouped-colls)
       grouped-colls))))

(def cogroup-by-schema
  {:collector {:docstr "A collector fn to apply after the cogroup. The signature is
```
(fn [[key [list-of-elts-from-pcoll1 list-of-elts-from-pcoll2]]] ...)
```"}})
(defn cogroup-by
  {:doc (with-opts-docstr
          "Takes a specification of the join between pcolls and returns a PCollection of KVs (unless a :collector fn is given) with values being list of values corresponding to the key-fn. The specification is a list of triple [pcoll f options].

  - pcoll is a pcoll on which to join
  - f is a joining function, used to produce the keys on which to join. Can be nil if the coll is already made up of KVs
  - options is a map configuring each sides of the join

 Only one option is supported for now in join:

  - :type -> :optional or :required to select between left and right join. Defaults to :optional

Example:
```
(ds/cogroup-by {:name :my-cogroup-by
                :collector (fn [[key [list-of-elts-from-pcoll1 list-of-elts-from-pcoll2]]]
                               [key (concat list-of-elts-from-pcoll1 list-of-elts-from-pcoll2)])}
               [[pcoll1 :id {:type :required}]
                [pcoll2 (fn [elt] (:foreign-key elt)) {:type :optional}]])

```
See https://beam.apache.org/documentation/transforms/java/aggregation/cogroupbykey/ and for a different approach to joins see [[join-by]]"
          named-schema cogroup-by-schema)
   :added "0.1.0"}
  ([options specs reduce-fn]
   (let [full-opts (if reduce-fn (assoc options :collector reduce-fn) options)
         group-specs (make-group-specs specs)
         cogroup-by-tr (cogroup-by-transform full-opts)]
     (apply-transform group-specs cogroup-by-tr named-schema full-opts)))
  ([options specs] (cogroup-by options specs nil)))

(def join-by-schema
  {:collector {:docstr "A collector fn to apply after the join. The signature is like map, one element for each pcoll in the join."}})

(defn join-by
  {:doc (with-opts-docstr
          "Takes a specification of the join between pcolls and returns a PCollection of the cartesian product (only difference from cogroup-by) of all elements joined according to the spec. The specification is a list of triple [pcoll f options].

  - pcoll is a pcoll on which to join
  - f is a joining function, used to produce the keys on which to join. Can be nil if the coll is already made up of KVs
  - options is a map configuring each sides of the join

 Only one option is supported for now in join:

  - :type -> :optional or :required to select between left and right join. Defaults to :optional

Example:
```
(ds/join-by {:name :my-join-by
                :collector (fn [elt1 elt2]
                               (merge elt1 elt2))}
               [[pcoll1 :id {:type :required}]
                [pcoll2 (fn [elt] (:foreign-key elt)) {:type :optional}]])

```
See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/join/CoGroupByKey and for a different approach to joins see [[cogroup-by]]"
          named-schema join-by-schema)
   :added "0.1.0"}
  ([{nam :name :as options} specs join-fn]
   (let [root-name (if nam (name nam) "join-by")
         clean-join-fn (or join-fn (:collector options))]
     (pt->>
      root-name
      (cogroup-by (-> options
                      (assoc :name (str root-name "-cogroup-by"))
                      (dissoc :collector))
                  specs)
      (dmapcat (fn [^KV kv]
                 (let [results (.getValue kv)
                       results-ok (map #(if (empty? %) [nil] %) results)
                       raw-res (apply combo/cartesian-product results-ok)
                       res (map (fn [prod] (apply clean-join-fn prod)) raw-res)]
                   res))
               {:name (str root-name "-cartesian-product")
                :without-coercion-to-clj true}))))
  ([options specs] (if (fn? specs)
                     (throw (ex-info "Wrong type of argument for join-by, join-fn is now passed as a :collector key in options" {:specs specs}))
                     (join-by options specs nil))))

(defn ddistinct
  {:doc (with-opts-docstr
          "Removes duplicate element from PCollection.

See https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/RemoveDuplicates.html

Example:
```
(ds/distinct pcoll)
```"
          base-schema)
   :added "0.1.0"}
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :distinct)]
     (apply-transform pcoll (Distinct/create) base-schema opts)))
  ([pcoll] (ddistinct {} pcoll)))

(def scoped-ops-schema
  {:scope {:docstr "Scope given to the combinating operation. One of (:globally :per-key)."}})

(defn sample
  {:doc (with-opts-docstr
          "Takes samples of the elements in a PCollection, or samples of the values associated with each key in a PCollection of KVs.

See https://beam.apache.org/documentation/transforms/java/aggregation/sample/

  Example:
```
    (ds/sample 100 {:scope :per-key} pcoll)
```"
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

See https://beam.apache.org/documentation/transforms/java/other/flatten/

Example:
```
(ds/flatten [pcoll1 pcoll2 pcoll3])
```"
   :added "0.1.0"}
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :flatten)]
     (apply-transform pcoll (Flatten/iterables) base-schema opts)))
  ([pcoll] (dflatten {} pcoll)))

(defn- create-coll-list-transform [transform label]
  (fn [options & colls]
    (let [[real-options ^Iterable all-colls]
          (if (map? options)
            [options colls]
            [{} (conj colls options)])
          opts (assoc real-options :label label)
          coll-list (if (and (= 1 (count all-colls)) (instance? PCollectionList (first all-colls)))
                      (first all-colls)
                      (PCollectionList/of all-colls))]
      (apply-transform coll-list transform base-schema opts))))

(def
  ^{:doc "Returns a single PCollection containing all the given pcolls. Accepts an option map as an optional first arg.

Example:
```
(ds/concat pcoll1 pcoll2)
(ds/concat {:name :concat-node} pcoll1 pcoll2)
```"
    :added "0.1.0"}
  dconcat
  (create-coll-list-transform (Flatten/pCollections) :concat))

(def
  ^{:doc "Returns a single PCollection containing the distinct intersection of the given pcolls.
Accepts an option map as an optional first arg.

Example:
```
(ds/intersect-distinct pcoll1 pcoll2)
(ds/intersect-distinct {:name :intersect-node} pcoll1 pcoll2)
```"
    :added "0.7.13"}
  intersect-distinct

  (create-coll-list-transform (Sets/intersectDistinct) :intersect-distinct))

(def
  ^{:doc "Returns a single PCollection containing the distinct union of the given pcolls.
Accepts an option map as an optional first arg.

Example:
```
(ds/union-distinct pcoll1 pcoll2)
(ds/union-distinct {:name :intersect-node} pcoll1 pcoll2)
```"
    :added "0.7.13"}
  union-distinct
  (create-coll-list-transform (Sets/unionDistinct) :union-distinct))

(def
  ^{:doc "Returns a single PCollection containing the distinct difference (p1 - p2 - p3 ...) of the given pcolls.
Accepts an option map as an optional first arg.

Example:
```
(ds/except-distinct pcoll1 pcoll2)
(ds/except-distinct {:name :intersect-node} pcoll1 pcoll2)
```"
    :added "0.7.13"}
  except-distinct
  (create-coll-list-transform (Sets/exceptDistinct) :except-distinct))

;; https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/transforms/Combine.Globally

(def base-combine-schema
  {:fanout {:docstr "Uses an intermediate node to combine parts of the data to reduce load on the final global combine step. Can be either an integer or a fn from key to integer (for combine-by-key scope)."
            :action (fn [transform fanout] (.withHotKeyFanout transform
                                                              (if (fn? fanout) (sfn fanout) fanout)))}
   :without-defaults {:docstr "Boolean indicating if the transform should attempt to provide a default value in the case of empty input."
                      :action (fn [transform b] (when b (.withoutDefaults transform)))}})

(def combine-schema
  (merge
   base-schema
   base-combine-schema
   {:as-singleton-view {:docstr "The transform returns a PCollectionView whose elements are the result of combining elements per-window in the input PCollection."
                        :action (fn [transform b] (when b (.asSingletonView transform)))}
    :scope {:docstr "Specifies the combiner scope of application"
            :enum [:global :per-key]
            :default :global}}))

(defn combine
  {:doc (with-opts-docstr
          "Applies a CombineFn or a Clojure function with equivalent arities to the PCollection of KVs.

See https://beam.apache.org/documentation/transforms/java/aggregation/combine/"
          combine-schema)
   :added "0.1.0"}
  ([f {:keys [coder key-coder value-coder] :as options} ^PCollection pcoll]
   (let [scope (or (:scope options) :global)
         opts (assoc options
                     :label (or (:label options) (keyword (str "combine-" (name scope))))
                     :scope scope)
         cfn (->combine-fn f)
         base-opts (merge named-schema combine-schema)
         [ptrans _base-coder] (cond
                                (#{:local :per-key} scope)
                                [(Combine/perKey cfn)
                                 (or coder
                                     (KvCoder/of
                                      (or key-coder
                                          (-> pcoll
                                              (.getCoder)
                                              (.getKeyCoder)))
                                      (or value-coder (make-nippy-coder))))]

                                (#{:global :globally} scope)
                                [(Combine/globally cfn) (or coder (make-nippy-coder))]

                                :else (throw (ex-info (format "Option %s is not recognized" scope)
                                                      {:scope-given scope :allowed-scopes #{:global :per-key}})))]
     (apply-transform pcoll ptrans base-opts opts)))
  ([f pcoll] (combine f {} pcoll)))

(defn- combine-by-transform
  [key-fn f options]
  (let [safe-opts (-> options
                      (dissoc :name)
                      (assoc :scope :per-key))]
    (ptransform
     :combine-by
     [^PCollection pcoll]
     (->> pcoll
          (with-keys key-fn safe-opts)
          (combine f safe-opts)))))

(defn combine-by
  {:doc (with-opts-docstr
          "Shortcut to easily group values in a PColl by a function and combine all the values by key according to a combiner fn. Returns a PCollection of KVs.

Example:
```
;; Returns a pcoll of two KVs, with false and true as keys, and the sum of even? and odd? numbers as values
(->> pcoll
     (ds/combine-by even? (ds/sum-fn) {:name :my-combine-by}))
```

See https://beam.apache.org/documentation/transforms/java/aggregation/combine/ and [[combine-fn]] for options about creating a combiner function (combine-fn is applied on the given clojure fn if necessary, you do not need to call it yourself)"
          base-schema kv-coder-schema base-combine-schema)
   :added "0.1.0"}
  ([key-fn f options ^PCollection pcoll]
   (let [opts (-> options
                  (assoc :label :combine-by :scope :per-key))
         ptrans (combine-by-transform key-fn f options)]
     (apply-transform pcoll ptrans base-schema opts)))
  ([key-fn f pcoll] (combine-by key-fn f {} pcoll)))

(defn ddistinct-by
  {:doc
   (with-opts-docstr
     "Removes duplicate elements from PCollection using a representative value.

Args:
  - f, the function applied to each element to get a representative value

Example:
  With a pcoll like [[\"a\" 1] [\"b\" 2] [\"a\" 3]],
  the following code removes duplicate elements based on the first vector value.
  ```
  (ds/distinct-by first pcoll)
  ```
  Order is not preserved. Example output: [[\"a\" 3] [\"b\" 2]]"
     base-schema)
   :added "0.7.17"}
  ([f ^PCollection pcoll]
   (ddistinct-by f nil pcoll))
  ([f options ^PCollection pcoll]
   (let [nam (name (or (:name options) :distinct-by))]
     (pt->>
      nam
      pcoll
      (combine-by f
                  (combine-fn
                   (fn [acc elt] (if (nil? acc) elt acc))
                   identity
                   (fn [& accs]
                     (reduce (fn [_ e] (when (some? e) (reduced e))) nil accs))
                   (constantly nil))
                  {:name (str nam "--combine-by")})
      (dmap dval (assoc options :name (str nam "--collect")))))))

(defn count-fn
  ([& {:keys [mapper predicate]
       :or {mapper (fn [_] 1)
            predicate (constantly true)}}]
   (combine-fn
    (fn [acc elt] (if (predicate elt) (+ acc (mapper elt)) acc))
    identity
    +
    (constantly 0))))

(defn sum-fn
  ([& {:keys [mapper predicate]
       :or {mapper identity
            predicate (constantly true)}}]
   (combine-fn
    (fn [acc elt] (if (predicate elt)
                    (+ acc (mapper elt))
                    acc))
    identity
    +
    (constantly 0))))

(defn mean-fn
  ([& {:keys [mapper predicate]
       :or {mapper identity
            predicate (constantly true)}}]
   (combine-fn
    (fn [[sum total :as acc] elt] (if (predicate elt)
                                    [(+ sum (mapper elt)) (inc total)]
                                    acc))
    (fn [[sum total]] (if (> total 0) (/ sum (double total)) 0))
    (fn [& accs] (reduce
                  (fn [[all-sum all-total] [sum total]]
                    [(+ all-sum sum) (+ all-total total)])
                  [0 0] accs))
    (constantly [0 0]))))

(defn max-fn
  ([& {:keys [mapper predicate]
       :or {mapper identity
            predicate (constantly true)}}]
   (combine-fn
    (fn [acc elt] (if (predicate elt)
                    (if (nil? acc)
                      (mapper elt)
                      (if (> (mapper elt) acc)
                        (mapper elt)
                        acc))
                    acc))
    identity
    (fn [& accs]
      (apply max (remove nil? accs)))
    (constantly nil))))

(defn min-fn
  ([& {:keys [mapper predicate]
       :or {mapper identity
            predicate (constantly true)}}]
   (combine-fn
    (fn [acc elt] (if (predicate elt)
                    (if (nil? acc)
                      (mapper elt)
                      (if (< (mapper elt) acc)
                        (mapper elt)
                        acc))
                    acc))
    identity
    (fn [& accs] (apply min (remove nil? accs)))
    (constantly nil))))

(defn frequencies-fn
  ([& {:keys [mapper predicate]
       :or {mapper identity
            predicate (constantly true)}}]
   (combine-fn
    (fn [acc elt]
      (if (predicate elt)
        (update acc (mapper elt) (fnil inc 0))
        acc))
    identity
    (fn [& accs] (apply merge-with + accs))
    (constantly nil))))

(defn dfrequencies
  {:doc (with-opts-docstr
          "Returns the frequency of each unique element of the input PCollection.

See https://beam.apache.org/documentation/transforms/java/aggregation/count/

Example:
```
(ds/frequencies pcoll)
```"
          base-schema)
   :added "0.1.0"}
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :frequencies)]
     (apply-transform pcoll (Count/perElement) named-schema opts)))
  ([pcoll] (dfrequencies {} pcoll)))

(def accumulation-mode-enum
  {:accumulate #(.accumulatingFiredPanes %)
   :discard #(.discardingFiredPanes %)})

(def window-schema
  (merge
   named-schema
   {:trigger {:docstr "Adds a Trigger to the Window."
              :action (fn [transform ^Trigger t] (.triggering transform t))}
    :with-allowed-lateness {:docstr "Allow late data. Mandatory for custom trigger"
                            :action (fn [transform d] (.withAllowedLateness transform (->duration d)))}
    :accumulate-mode {:docstr "Accumulate mode when a Trigger is fired (accumulate or discard)"
                      :action (fn [transform acc] ((get accumulation-mode-enum acc) transform))}}))

(defn fixed-windows
  {:doc (with-opts-docstr
          "Apply a fixed window input PCollection (useful for unbounded PCollections).

See https://cloud.google.com/dataflow/model/windowing#setting-fixed-time-windows

Example:
```
(require '[clj-time.core :as time])
(ds/fixed-windows (time/minutes 30) pcoll)
```"
          window-schema)
   :added "0.5.0"}
  ([width options ^PCollection pcoll]
   (let [transform (-> (->duration width)
                       (FixedWindows/of)
                       (Window/into))]
     (apply-transform pcoll transform window-schema options)))
  ([width ^PCollection pcoll] (fixed-windows width {} pcoll)))

(defn sliding-windows
  {:doc (with-opts-docstr
          "Apply a sliding window to divide a PCollection (useful for unbounded PCollections).

See https://cloud.google.com/dataflow/model/windowing#setting-sliding-time-windows

Example:
```
(require '[clj-time.core :as time])
(ds/sliding-windows (time/minutes 30) (time/seconds 5) pcoll)
```"
          window-schema)
   :added "0.4.1"}
  ([width step options ^PCollection pcoll]
   (let [transform (-> (->duration width)
                       (SlidingWindows/of)
                       (.every  (->duration step))
                       (Window/into))]
     (apply-transform pcoll transform window-schema options)))
  ([width step ^PCollection pcoll] (sliding-windows width step {} pcoll)))

(defn session-windows
  {:doc (with-opts-docstr
          "Apply a Session window to divide a PCollection (useful for unbounded PCollections).

See https://cloud.google.com/dataflow/model/windowing#setting-session-windows

Example:
```
(require '[clj-time.core :as time])
(ds/session-windows (time/minutes 10) pcoll)
```"
          window-schema)
   :added "0.4.1"}
  ([gap options ^PCollection pcoll]
   (let [transform (-> (->duration gap)
                       (Sessions/withGapDuration)
                       (Window/into))]
     (apply-transform pcoll transform window-schema options)))
  ([gap ^PCollection pcoll] (session-windows gap {} pcoll)))

(defn- mk-default-windowed-fn
  [{:keys [file-name suffix] :as _options
    :or {file-name "file"}}]
  (fn [shard-number shard-count ^BoundedWindow window _]
    (safe-exec
     (let [timestamp (timf/unparse (:date-hour-minute timf/formatters)
                                   (.start window))]
       (str timestamp "-" file-name "-" shard-number "-of-" shard-count "." suffix)))))

(defn- mk-default-unwindowed-fn
  [{:keys [file-name suffix] :as _options
    :or {file-name "file"}}]
  (fn [shard-number shard-count  _]
    (safe-exec
     (str file-name "-" shard-number "of" shard-count  "." suffix))))

(def filename-schema
  {:file-name {:docstr "set the default filename prefix (only used when no custom function is set)"}
   :suffix {:docstr "set the default filename suffix (only used when no custom function is set)"}
   :windowed-fn {:docstr "override the filename function for windowed PCollection"}
   :unwindowed-fn {:docstr "override the filename function for unwindowed PCollection"}})

(defn filename-policy
  {:doc (with-opts-docstr
          "Create a filename-policy object

Examples:
```
(ds/filename-policy {:file-name \"file\"
                     :prefix \"gs://toto/\"
                     :suffix \"json\"})

;; with custom functions
(require '[clj-time.format :as tf])

(defn windowed-fn
  [shard-number shard-count ^BoundedWindow window _]
  (let [timestamp (tf/unparse (:date-hour-minute tf/formatters)
                              (.start window))]
    (str file-name \"-\" shard-number \"of\" shard-count \"-\" timestamp \".\" \"txt\")))

(ds/filename-policy {:windowed-fn windowed-fn
                     :unwindowed-fn (fn [_ _ _] \"file.txt\")})
```"
          filename-schema)
   :added "0.5.2"}
  ^datasplash.fns.FileNamePolicy
  [options]
  (datasplash.fns.FileNamePolicy. {"prefix"      (:prefix options)
                                   "windowed-fn" (or (:windowed-fn options)
                                                     (mk-default-windowed-fn options))
                                   "unwindowed-fn" (or (:unwindowed-fn options)
                                                       (mk-default-unwindowed-fn options))}))

(defn- parse-try
  "Separates body from catch/finally clauses."
  [body]
  (loop [expressions []
         clauses body]
    (let [[head & tail] clauses]
      (if (or (not head) (and (list? head) (#{'catch 'finally} (first head))))
        [expressions clauses]
        (recur (cons head expressions) tail)))))

(defmacro dtry
  {:doc "Just like try except it wraps the body in a safe-exec."
   :added "0.5.2"}
  [& body]
  (let [[expressions clauses] (parse-try body)]
    `(try (safe-exec ~@expressions)
          ~@clauses)))
