(ns datasplash.core
  (:require [cognitect.transit :as transit]
            [datasplash.code :as code]
            [clojure.edn :as edn]
            [clojure.math.combinatorics :as combo])
  (:import [java.io InputStream OutputStream]
           [java.util UUID]
           [com.google.cloud.dataflow.sdk.testing TestPipeline DataflowAssert]
           [com.google.cloud.dataflow.sdk.options PipelineOptionsFactory]
           [com.google.cloud.dataflow.sdk Pipeline]
           [com.google.cloud.dataflow.sdk.io TextIO$Read TextIO$Write]

           [com.google.cloud.dataflow.sdk.transforms
            DoFn DoFn$Context DoFn$ProcessContext ParDo DoFnTester Create PTransform
            SerializableFunction WithKeys GroupByKey RemoveDuplicates
            Flatten Combine$CombineFn Combine Sum View]
           [com.google.cloud.dataflow.sdk.values KV PCollection TupleTag PBegin PCollectionList]
           [com.google.cloud.dataflow.sdk.coders StringUtf8Coder CustomCoder Coder$Context KvCoder]
           [com.google.cloud.dataflow.sdk.transforms.join KeyedPCollectionTuple CoGroupByKey])
  (:gen-class))

(def ops-counter (atom {}))

(defmethod print-method KV [^KV kv ^java.io.Writer w]
  (.write w (str "[" (.getKey kv) ", " (.getValue kv) "]")))

(defn dofn
  ^DoFn [f & {:keys [start-bundle finish-bundle]
              :or {start-bundle (fn [_] nil)
                   finish-bundle (fn [_] nil)}
              :as opts}]
  (proxy [DoFn] []
    (processElement [^DoFn$ProcessContext context] (f context))
    (startBundle [^DoFn$Context context] (start-bundle context))
    (finishBundle [^DoFn$Context context] (finish-bundle context))))

(defn map-fn
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (.element c)
          result (f elt)]
      (.output c result))))

(defn mapcat-fn
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (.element c)
          result (f elt)]
      (doseq [res result]
        (.output c res)))))

(defn pardo-fn
  [f]
  (fn [^DoFn$ProcessContext c]
    (f c)))

(defn filter-fn
  [f]
  (fn [^DoFn$ProcessContext c]
    (let [elt (.element c)
          result (f elt)]
      (when result
        (.output c elt)))))

(defn didentity [c]
  (.output c (.element c)))


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
                                         (swap! ops-counter update-in [label#]
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

(def pardo-schema
  (merge
   base-schema
   {:side-inputs (fn [transform ^Iterable inputs] (.withSideInputs transform inputs))}))

(defn map-op
  ([transform label coder]
   (fn make-map-op
     ([f options ^PCollection pcoll]
      (let [opts (assoc options :label label)]
        (-> pcoll
            (.apply (with-opts pardo-schema opts
                      (ParDo/of (dofn (transform f)))))
            (.setCoder (or (:coder opts) coder)))))
     ([f pcoll] (make-map-op f {} pcoll))))
  ([transform label]
   (map-op transform label (make-transit-coder))))


(defmacro mapm
  [f options ^PCollection pcoll]
  `(let [opts# (assoc ~options :label :map)]
    (-> ~pcoll
        (.apply (with-opts pardo-schema opts#
                  (ParDo/of (dofn (map-fn (code/eval-from-var (code/trap ~f)))))))
        (.setCoder (or (:coder opts#) (make-transit-coder) ))))
  ;; ([f pcoll] (dmapm f {} pcoll))
  )
(defmacro filterm
  [f options ^PCollection pcoll]
  `(let [opts# (assoc ~options :label :filter)]
     (-> ~pcoll
         (.apply (with-opts pardo-schema opts#
                   (ParDo/of (dofn (filter-fn (code/eval-from-var (code/trap ~f)))))))
         (.setCoder (or (:coder opts#) (make-transit-coder) ))))
  ;; ([f pcoll] (dmapm f {} pcoll))
  )

(def dmap (map-op map-fn :map))
(def pardo (map-op pardo-fn :raw-pardo))
(def dmapcat (map-op mapcat-fn :mapcat))
(def dfilter (map-op filter-fn :filter))

(defn generate-input
  ([coll options ^Pipeline p]
   (let [opts (assoc options :label :generate-input)]
     (-> p
         (.apply (with-opts base-schema opts
                   (Create/of (seq coll))))
         (.setCoder (or (:coder opts) (make-transit-coder))))))
  ([coll p] (generate-input coll {} p)))


;; (defn- make-view-transform
;;   [options]
;;   (let [safe-opts (dissoc options :name)]
;;     (proxy [View$AsSingleton] []
;;       (getDefaultOutputCoder [_ _] (make-transit-coder)))))

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
                     :map (View/asMap))))
         )))
  ([pcoll] (view pcoll {})))



(defn- to-edn*
  [^DoFn$Context c]
  (let [elt (.element c)
        result (pr-str elt)]
    (.output c result)))

(def to-edn (partial (map-op identity :to-edn (StringUtf8Coder/of)) to-edn*))
(def from-edn (partial dmap #(read-string %)))

(defn sfn
  ^SerializableFunction
  [f]
  (proxy [SerializableFunction] []
    (apply [input]
      (f input))))

(definterface ICombineFn
  (getReduceFn [])
  (getExtractFn [])
  (getMergeFn [])
  (getInitFn []))

(defn combine-fn
  ^Combine$CombineFn
  ([reducef extractf combinef initf output-coder acc-coder]
   (proxy [Combine$CombineFn ICombineFn] []
     (createAccumulator [] (initf))
     (addInput [acc elt] (reducef acc elt))
     (mergeAccumulators [accs] (apply combinef accs))
     (extractOutput [acc] (extractf acc))
     (getDefaultOutputCoder [_ _] output-coder)
     (getAccumulatorCoder [_ _] acc-coder)
     (getReduceFn [] reducef)
     (getExtractFn [] extractf)
     (getMergeFn [] combinef)
     (getInitFn [] initf)))
  ([reducef extractf combinef initf output-coder] (combine-fn reducef extractf combinef initf output-coder (make-transit-coder)))
  ([reducef extractf combinef initf] (combine-fn reducef extractf combinef initf (make-transit-coder)))
  ([reducef extractf combinef] (combine-fn reducef extractf combinef reducef))
  ([reducef extractf] (combine-fn reducef extractf reducef))
  ([reducef] (combine-fn reducef identity)))

(defn ->combine-fn
  [f]
  (if (instance? Combine$CombineFn f) f (combine-fn f)))

(defn djuxt
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
                       (println "ACCS" accs)
                       (apply f (map #(nth % idx) accs)))) cfs))
     (fn []
       (map (fn [cf] (let [f (.getInitFn cf)]
                       (f))) cfs))
     (.getDefaultOutputCoder (first cfs) nil nil)
     (.getAccumulatorCoder (first cfs) nil nil))))

(defn with-keys
  ([f {:keys [key-coder value-coder] :as options} ^PCollection pcoll]
   (let [opts (assoc options :label :with-keys)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (WithKeys/of (sfn f))))
         (.setCoder (KvCoder/of
                     (or key-coder (make-transit-coder))
                     (or value-coder (make-transit-coder)))))))
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


(defn write-text-file
  ([to options ^PCollection pcoll]
   (let [opts (assoc options :label :write-text-file)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (TextIO$Write/to to))))))
  ([to pcoll] (write-text-file to {} pcoll)))

(defn read-text-file
  ([from options p]
   (let [opts (assoc options :label :read-text-file)]
     (-> p
         (cond-> (instance? Pipeline p) (PBegin/in))
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
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :distinct)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (RemoveDuplicates/create))))))
  ([pcoll] (ddistinct {} pcoll)))

(defn dflatten
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :flatten)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (Flatten/iterables))))))
  ([pcoll] (dflatten {} pcoll)))

(defn dconcat
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

(defn combine-globally
  ([f options ^PCollection pcoll]
   (let [opts (assoc options :label :combine-globally)
         cf (->combine-fn f)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (Combine/globally cf)))
         (.setCoder (make-transit-coder)))))
  ([f pcoll] (combine-globally f {} pcoll)))


(defn combine-by-key
  ([f options ^PCollection pcoll]
   (let [opts (assoc options :label :combine-by-keys)]
     (-> pcoll
         (.apply (Combine/perKey (combine-fn f)))
         (.setCoder (make-transit-coder)))))
  ([f pcoll] (combine-by-key f {} pcoll)))


(defn- combine-by-transform
  [key-fn f options]
  (let [safe-opts (dissoc options :name)]
    (proxy [PTransform] []
      (apply [^PCollection pcoll]
        (->> pcoll
             (with-keys key-fn safe-opts)
             (combine-by-key f safe-opts))))))

(defn combine-by
  ([key-fn f options ^PCollection pcoll]
   (let [opts (assoc options :label :combine-by)]
     (.apply pcoll (with-opts base-schema opts
                     (combine-by-transform key-fn f options)))))
  ([key-fn f pcoll] (combine-by key-fn f {} pcoll)))



(defn sum
  ([options ^PCollection pcoll]
   (let [opts (assoc options :label :sum)]
     (-> pcoll
         (.apply (with-opts base-schema opts
                   (Sum/longsGlobally))))))
  ([pcoll] (sum {} pcoll)))
