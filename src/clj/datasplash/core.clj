(ns datasplash.core
  (:require [cognitect.transit :as transit]
            [clojure.edn :as edn])
  (:import [datasplash.fns ClojureDoFn ClojureSerializableFn]
           [java.io InputStream OutputStream]
           [java.util UUID]
           [com.google.cloud.dataflow.sdk.options PipelineOptionsFactory]
           [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.io TextIO$Read TextIO$Write]
           [com.google.cloud.dataflow.sdk.transforms
            DoFn DoFn$Context ParDo DoFnTester Create PTransform
            SerializableFunction WithKeys GroupByKey]
           [com.google.cloud.dataflow.sdk.values PCollection TupleTag PBegin]
           [com.google.cloud.dataflow.sdk.coders ByteArrayCoder StringUtf8Coder CustomCoder Coder$Context]
           [com.google.cloud.dataflow.sdk.transforms.join KeyedPCollectionTuple CoGroupByKey])
  (:gen-class))

(def ops-counter (atom {}))

(defn dofn
  [f & {:keys [start-bundle finish-bundle] :as opts}]
  (ClojureDoFn. f))

(defn map-fn
  [f]
  (fn [^DoFn$Context c]
    (let [elt (.element c)
          result (f elt)]
      (.output c result))))

(defn mapcat-fn
  [f]
  (fn [^DoFn$Context c]
    (let [elt (.element c)
          result (f elt)]
      (doseq [atm result]
        (.output c atm)))))

(defn filter-fn
  [f]
  (fn [^DoFn$Context c]
    (let [elt (.element c)
          result (f elt)]
      (when result
        (.output c elt)))))

(defn make-transit-coder
  []
  (proxy [CustomCoder] []
    (encode [obj ^OutputStream out ^Coder$Context context]
      (require '[cognitect.transit :as transit])
      (let [wrt (transit/writer out :msgpack)]
        (transit/write wrt obj)))
    (decode [^InputStream in ^Coder$Context context]
      (require '[cognitect.transit :as transit])
      (let [rdr (transit/reader in :msgpack)]
        (transit/read rdr)))
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
      (fn [f# [k# apply#]]
        (if-let [v# (get (assoc ~opts :name full-name#) k#)]
          (apply# f# v#)
          f#))
      transform# ~schema)))

(def base-schema
  {:name (fn [transform n] (.withName transform n))})

(defn map-op
  ([transform label coder]
   (fn make-map-op
     ([f options ^PCollection pcoll]
      (let [opts (assoc options :label label)]
        (-> pcoll
            (.apply (with-opts base-schema opts
                      (ParDo/of (dofn (transform f)))))
            (.setCoder (or (:coder opts) coder)))))
     ([f pcoll] (make-map-op f {} pcoll))))
  ([transform label]
   (map-op transform label (make-transit-coder))))

(def dmap (map-op map-fn :map))
(def dmapcat (map-op mapcat-fn :mapcat))
(def dfilter (map-op filter-fn :filter))

(defn generate-input
  [coll options ^Pipeline p]
  (let [opts (assoc options :label :generate-input)]
    (-> p
        (.apply (with-opts base-schema opts
                  (Create/of (seq coll))))
        (.setCoder (or (:coder opts) (make-transit-coder))))))

(defn- to-edn*
  [^DoFn$Context c]
  (let [elt (.element c)
        result (pr-str elt)]
    (.output c result)))

(def to-edn (partial (map-op identity :to-edn (StringUtf8Coder/of)) to-edn*))
(def from-edn (partial dmap #(edn/read-string %)))

(defn sfn
  ^SerializableFunction [f]
  (ClojureSerializableFn. f))

(defn with-keys
  ([f options ^PCollection pcoll]
   (let [opts (assoc options :label :with-keys)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (WithKeys/of (sfn f)))))))
  ([f pcoll] (with-keys f {} pcoll)))

(defn group-by-key
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
  ([f options ^PCollection pcoll]
   (let [opts (assoc options :label :group-by)]
     (.apply pcoll (with-opts base-schema opts
                     (group-by-transform f options)))))
  ([f pcoll] (dgroup-by f {} pcoll)))

(defn make-pipeline
  [str-args]
  (let [builder (PipelineOptionsFactory/fromArgs
                 (into-array String str-args))
        options (.create builder)
        pipeline (Pipeline/create options)
        coder-registry (.getCoderRegistry pipeline)]
    (doto coder-registry
      (.registerCoder clojure.lang.IPersistentCollection (make-transit-coder))
      (.registerCoder clojure.lang.Keyword (make-transit-coder)))
    pipeline))

(defn load-text-file
  [from options ^Pipeline p]
  (let [opts (assoc options :label :load-text-file)]
    (-> p
        (.apply (with-opts base-schema opts
                  (TextIO$Read/from from)))
        (.setCoder (StringUtf8Coder/of)))))

(defn write-text-file
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-text-file)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (TextIO$Write/to to))))))
  ([to pcoll] (write-text-file to {} pcoll)))

(defn read-text-file
  ([from options ^PBegin p]
   (let [opts (assoc options :label :read-text-file)]
     (-> p
         (.apply (with-opts base-schema opts
                   (TextIO$Read/from from)))
         (.setCoder (StringUtf8Coder/of)))))
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
  ([from options ^Pipeline p]
   (let [opts (assoc options :label :read-edn-file)]
     (-> p
         (.apply (with-opts base-schema opts
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
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-edn-file)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (write-edn-file-transform to opts))))))
  ([to pcoll] (write-edn-file to {} pcoll)))

(defn make-keyed-pcollection-tuple
  [pcolls]
  (let [empty-kpct (KeyedPCollectionTuple/empty (.getPipeline (first pcolls)))]
    (reduce
     (fn [coll-tuple [idx pcoll]]
       (let [tag (TupleTag. (str idx))
             new-coll-tuple (.and coll-tuple tag pcoll)]
         new-coll-tuple))
     empty-kpct (map-indexed (fn [idx v] [idx v]) pcolls))))

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
                                   (into [] (conj values k)))) opts rel)]
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
