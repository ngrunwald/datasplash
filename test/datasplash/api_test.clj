(ns datasplash.api-test
  (:require
   [cheshire.core :as json]
   [clj-time.core :as time]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datasplash
    [api :as ds]
    [core :refer [clj->kv]]]
   [me.raynes.fs :as fs])
  (:import
   (java.io PushbackReader)
   (java.util.zip GZIPOutputStream GZIPInputStream)
   (org.apache.beam.sdk.testing TestPipeline PAssert)))

(defn glob-file
  [path]
  (fs/glob (str path "*")))

(defmacro with-files
  [files & body]
  `(let [~@(flatten
            (for [file files]
              [file (fs/temp-name (name file) ".edn")]))]
     (try
       ~@body
       (finally
         ~@(for [file files]
             `(doseq [f# (glob-file ~file)]
                (fs/delete (.getPath f#))))))))

(defn read-file
  [path]
  (with-open [rdr (PushbackReader. (io/reader path))]
    (loop [acc []]
      (let [line (edn/read {:eof ::eof} rdr)]
        (if (= line ::eof)
          acc
          (recur (conj acc line)))))))

(defn make-test-pipeline
  []
  (let [p (TestPipeline/create)
        coder-registry (.getCoderRegistry p)]
    (doto coder-registry
      (.registerCoderForClass clojure.lang.IPersistentCollection (ds/make-nippy-coder))
      (.registerCoderForClass clojure.lang.Keyword (ds/make-nippy-coder)))
    p))

(deftest basic-pipeline
  (let [p (ds/make-pipeline [])
        input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)
        proc (ds/map (fn [x] (inc x)) {:name "increment"} input)
        filter (ds/filter even? proc)]
    (-> (PAssert/that input) (.containsInAnyOrder #{1 2 3 4 5}))
    (-> (PAssert/that proc)  (.containsInAnyOrder #{2 3 4 5 6}))
    (-> (PAssert/that filter) (.containsInAnyOrder #{2 4 6}))
    (is "increment" (.getName proc))
    (ds/run-pipeline p)))

(def test-data [1 2 3 4 5])
(def json-file-path "json-test-input.json")
(def gzipped-json-file-path "json-test-input.ndjson.gz")

(defn write-data
  [path-or-stream data & {:keys [compression]}]
  (let [compression-fn (case compression
                         :gzip #(GZIPOutputStream. %)
                         identity)]
    (with-open [wrtr (io/writer (compression-fn (io/output-stream path-or-stream)))]
      (.write wrtr (str/join "\n" (for [l data] (json/encode l)))))))

(defn create-json-input-fixture
  [f]
  (do (write-data json-file-path test-data)
      (write-data gzipped-json-file-path test-data :compression :gzip))
  (f)
  (do (fs/delete json-file-path)
      (fs/delete gzipped-json-file-path)))

(use-fixtures :once create-json-input-fixture)

(deftest json-io
  (let [p (ds/make-pipeline [])
        input (ds/read-json-file json-file-path {:name :read-json} p)]
    (-> (PAssert/that input)  (.containsInAnyOrder (map int test-data)))
    (is "read-json" (.getName input))
    (ds/run-pipeline p))
  (testing "with key-fn"
    (let [p (ds/make-pipeline [])
          input (ds/read-json-file json-file-path {:name :read-json-k :key-fn keyword} p)]
      (-> (PAssert/that input)  (.containsInAnyOrder (map int test-data)))
      (is "read-json-k" (.getName input))
      (ds/run-pipeline p))))

(deftest intra-bundle-parallelization-test
  (with-files [intra-bundle-parallelization-test]
    (let [p (ds/make-pipeline [])
          input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)]
      (ds/->> :pipelined input
              (ds/map inc {:name :inc :intra-bundle-parallelization 5})
              (ds/filter even? {:name :even? :intra-bundle-parallelization 5})
              (ds/write-edn-file intra-bundle-parallelization-test {:num-shards 1}))
      (ds/run-pipeline p))
    (let [res (into #{} (read-file (first (glob-file intra-bundle-parallelization-test))))]
      (is (= res #{2 4 6})))))

(deftest pt-macro-test
  (with-files [pt-test]
    (let [p (ds/make-pipeline [])
          input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)]
      (ds/->> :pipelined input
              (ds/map inc {:name :inc})
              (ds/filter even? {:name :even?})
              (ds/write-edn-file pt-test {:num-shards 1}))
      (ds/run-pipeline p))
    (let [res (into #{} (read-file (first (glob-file pt-test))))]
      (is (= res #{2 4 6})))))

(deftest pt-cond-macro-test
  (with-files [pt-cond-test]
    (let [p (ds/make-pipeline [])
          input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)]
      (->> (ds/cond->> :pipelined input
                       true (ds/map inc {:name :inc})
                       false (ds/filter even? {:name :even?}))
           (ds/write-edn-file pt-cond-test {:num-shards 1}))
      (ds/run-pipeline p))
    (let [res (into #{} (read-file (first (glob-file pt-cond-test))))]
      (is (= res #{2 3 4 5 6})))))

(deftest partition-test
  (let [p (ds/make-pipeline [])
        input (ds/generate-input [1 2 3 4 5 6 7 8 9] p)
        pcolls (ds/partition-by (fn [e _] (if (odd? e) 1 0))
                                2
                                {:name :partition}
                                input)
        [even-coll odd-coll] (.getAll pcolls)]
    (-> (PAssert/that even-coll) (.containsInAnyOrder '(2 4 6 8)))
    (-> (PAssert/that odd-coll) (.containsInAnyOrder '(1 3 5 7 9)))
    (ds/run-pipeline p)))

(deftest side-inputs-test
  (with-files [side-test]
    (let [p (ds/make-pipeline [])
          input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)
          side-input (ds/view (ds/generate-input [{1 :a 2 :b 3 :c 4 :d 5 :e}] {:name :side-gen} p))]
      (->> (ds/map (fn [x] (get-in (ds/side-inputs) [:mapping x]))
                   {:side-inputs {:mapping side-input}}
                   input)
           (ds/write-edn-file side-test {:num-shards 1}))
      (ds/run-pipeline p))
    (let [res (into #{} (read-file (first (glob-file side-test))))]
      (is (= res #{:a :b :c :d :e})))))

(deftest side-outputs-test
  (with-files [sideout-simple-test sideout-multi-test]
    (let [p (ds/make-pipeline [])
          input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)
          {:keys [simple multi]} (ds/map (fn [x] (ds/side-outputs :simple x :multi (* x 10)))
                                         {:side-outputs [:simple :multi]} input)]
      (ds/write-edn-file sideout-simple-test {:num-shards 1 :name :simple} simple)
      (ds/write-edn-file sideout-multi-test {:num-shards 1 :name :multi} multi)
      (ds/run-pipeline p))
    (let [res-simple (into #{} (read-file (first (glob-file sideout-simple-test))))
          res-multi (into #{} (read-file (first (glob-file sideout-multi-test))))]
      (is (= res-simple #{1 2 3 4 5}))
      (is (= res-multi #{10 20 30 40 50})))))

(deftest group-test
  (with-files [group-test]
    (let [p (ds/make-pipeline [])
          input (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] p)
          grouped (ds/group-by :key {:name "group"} input)]
      (ds/write-edn-file group-test {:num-shards 1} grouped)
      (is "group" (.getName grouped))
      (ds/run-pipeline p)
      (let [res (->> (read-file (first (glob-file group-test)))
                     (map (fn [[k v]] [k (into #{} v)]))
                     (into #{}))]
        (is (res [:a #{{:key :a :lue 65} {:key :a :val 42}}]))
        (is (res [:b #{{:key :b :val 56}}]))))))

(deftest cogroup-test
  (testing "nominal case"
    (with-files [cogroup-test]
      (let [p (ds/make-pipeline [])
            input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
            input2 (ds/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
            grouped (ds/cogroup-by {:name "cogroup-test"}
                                   [[input1 :key] [input2 :key]])]
        (ds/write-edn-file cogroup-test {:num-shards 1} grouped)
        (ds/run-pipeline p)
        (is "cogroup-test" (.getName grouped))
        (let [res (->> (read-file (first (glob-file cogroup-test)))
                       (map (fn [[k [i1 i2]]] [k [(into #{} i1) (into #{} i2)]]))
                       (into #{}))]
          (is (= res #{[:a [#{{:key :a, :lue 65} {:key :a, :val 42}} #{{:key :a, :uel 65} {:key :a, :lav 42}}]]
                       [:c [#{} #{{:key :c, :foo 42}}]]
                       [:b [#{{:key :b, :val 56}} #{}]]}))))))

  (testing "large set of pcollection"
    (with-files [cogroup-test]
      (let [p (ds/make-pipeline [])
            ;; pcolls is something like
            ;; [ [{:i 0 :key 0}, {:i 1 :key 0} ...] 🠐 this is a pcoll
            ;;   [{:i 0 :key 1}, {:i 1 :key 1} ...]
            ;;   ... ]
            nb-pcolls 101
            pcolls (mapv (fn [k-pcoll]
                           (ds/generate-input
                            (map (fn [i] {:i i :key k-pcoll}) (range 5))
                            {:name (str "gen-" k-pcoll)} p))
                         (range nb-pcolls))
            grouped (ds/cogroup-by {:name "join-fitments"
                                    :collector (fn [[_id same-i]] same-i)}
                                   (mapv #(vector % :i) pcolls))]
        (ds/write-edn-file cogroup-test {:num-shards 1} grouped)
        (ds/run-pipeline p)
        (doseq [line (read-file (first (glob-file cogroup-test)))
                :let [same-i (mapcat identity line)]]
          (is (= 1 (count (distinct (map :i same-i)))))
          (is (= (range nb-pcolls) (map :key same-i))))))))

(deftest cogroup-drop-nil-test
  (with-files [cogroup-drop-nil-test]
    (let [p (ds/make-pipeline [])
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/cogroup-by {:name "cogroup-drop-nil-test"}
                                 [[input1 :key] [input2 :key {:drop-nil? true}]])]
      (ds/write-edn-file cogroup-drop-nil-test {:num-shards 1} grouped)
      (ds/run-pipeline p)
      (is "cogroup-drop-nil-test" (.getName grouped))
      (let [res (->> (read-file (first (glob-file cogroup-drop-nil-test)))
                     (map (fn [[k [i1 i2]]] [k [(into #{} i1) (into #{} i2)]]))
                     (into #{}))]
        (is (= res #{[:a [#{{:key :a, :lue 65} {:key :a, :val 42}} #{{:key :a, :lav 42}}]]
                     [:c [#{} #{{:key :c, :foo 42}}]]
                     [:b [#{{:key :b, :val 56}} #{}]]}))))))

(deftest cogroup-required-test
  (with-files [cogroup-required-test]
    (let [p (ds/make-pipeline [])
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/cogroup-by {:name "cogroup-required-test"}
                                 [[input1 :key {:type :required}] [input2 :key]])]
      (ds/write-edn-file cogroup-required-test {:num-shards 1} grouped)
      (ds/run-pipeline p)
      (is "cogroup-required-test" (.getName grouped))
      (let [res (->> (read-file (first (glob-file cogroup-required-test)))
                     (map (fn [[k [i1 i2]]] [k [(into #{} i1) (into #{} i2)]]))
                     (into #{}))]
        (is (= res #{[:a [#{{:key :a, :lue 65} {:key :a, :val 42}} #{{:key :a, :uel 65} {:key :a, :lav 42}}]]
                     [:b [#{{:key :b, :val 56}} #{}]]}))))))

(deftest cogroup-join-nil-test
  (with-files [cogroup-join-nil-test]
    (let [p (ds/make-pipeline [])
          input1 (ds/generate-input [{:key :a :val 42} {:val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/cogroup-by {:name "cogroup-join-nil-test"}
                                 [[input1 :key] [input2 :key]])]
      (ds/write-edn-file cogroup-join-nil-test {:num-shards 1} grouped)
      (ds/run-pipeline p)
      (is "cogroup-join-nil-test" (.getName grouped))
      (let [res (->> (read-file (first (glob-file cogroup-join-nil-test)))
                     (map (fn [[k [i1 i2]]] [k [(into #{} i1) (into #{} i2)]]))
                     (into #{}))]
        (is (= res #{[:a [#{{:key :a, :lue 65} {:key :a, :val 42}} #{{:key :a, :lav 42}}]]
                     [nil [#{{:val 56}} #{}]]
                     [nil [#{} #{{:uel 65}}]]
                     [:c [#{} #{{:key :c, :foo 42}}]]}))))))

(deftest join-test
  (with-files [join-test]
    (let [p (ds/make-pipeline [])
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/join-by {:name "join-test"}
                              [[input1 :key] [input2 :key]] merge)]
      (ds/write-edn-file join-test {:num-shards 1} grouped)
      (ds/run-pipeline p)
      (is "join-test" (.getName grouped))
      (let [res (into #{} (read-file (first (glob-file join-test))))]
        (is (= res #{{:key :b, :val 56} {:key :c, :foo 42} {:key :a, :lue 65, :lav 42} {:key :a, :val 42, :lav 42}
                     {:key :a, :val 42, :uel 65} {:key :a, :lue 65, :uel 65}}))))))

(deftest join-test-required
  (with-files [join-test-required]
    (let [p (ds/make-pipeline [])
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/join-by {:name "join-test-required" :collector merge}
                              [[input1 :key]
                               [input2 :key {:type :required}]]
                              merge)]
      (ds/write-edn-file join-test-required {:num-shards 1} grouped)
      (ds/run-pipeline p)
      (is "join-test-required" (.getName grouped))
      (let [res (into #{} (read-file (first (glob-file join-test-required))))]
        (is (= res #{{:key :c, :foo 42} {:key :a, :lue 65, :lav 42} {:key :a, :val 42, :lav 42}
                     {:key :a, :val 42, :uel 65} {:key :a, :lue 65, :uel 65}}))))))

(deftest combine-pipeline
  (let [p (ds/make-pipeline [])
        input (ds/generate-input [1 2 3 4 5] p)
        proc (ds/combine + {:name "combine" :scope :global} input)]
    (-> (PAssert/that proc) (.containsInAnyOrder #{15}))
    (is "combine" (.getName proc))
    (ds/run-pipeline p)))


(defn- test-combine-fn
  [combine-fn]
  (let [p (ds/make-pipeline [])
        input (ds/generate-input [{:a 1} {:b 2} {:c 3} {:d 4} {:e 5}] p)
        proc (ds/combine combine-fn
                         {:name "combine" :scope :global} input)]
    (-> (PAssert/that proc) (.containsInAnyOrder #{{:a 1 :b 2 :c 3 :d 4 :e 5}}))
    (is "combine" (.getName proc))
    (ds/run-pipeline p)))


(deftest combine-pipeline-map
  (let [reducef (fn [acc x] (merge acc x))
        extractf identity
        combinef (fn [& accs] (apply merge accs))
        initf (fn [] {})]

    (test-combine-fn (ds/combine-fn reducef extractf combinef initf))

    (doseq [m [{:reduce reducef :extract extractf :combine combinef :init initf}
               {:reduce reducef                   :combine combinef :init initf}
               {:reduce merge                                       :init initf}
               {:reduce merge}]]
      (test-combine-fn (ds/combine-fn m)))))

(deftest combine-juxt
  (with-files [combine-juxt-test]
    (let [p (ds/make-pipeline [])
          input (ds/generate-input [1 2 3 4 5] p)
          proc (ds/combine (ds/juxt
                            + *
                            (ds/sum-fn)
                            (ds/mean-fn)
                            (ds/max-fn)
                            (ds/min-fn)
                            (ds/count-fn) (ds/count-fn :predicate even?)
                            (ds/max-fn :mapper #(* 10 %)))
                           {:name "combine"} input)]
      (ds/write-edn-file combine-juxt-test {:num-shards 1} proc)
      (is "combine" (.getName proc))
      (ds/run-pipeline p)
      (let [res (into #{} (read-file (first (glob-file combine-juxt-test))))]
        (is (= res #{'(15 120 15 3.0 5 1 5 2 50)}))))))

(deftest math-and-diamond
  (with-files [math-and-diamond-test sample-test]
    (let [p (ds/make-pipeline [])
          input (ds/generate-input [1 2 3 4 5] p)
          p1 (ds/combine (ds/mean-fn) {:name :mean} input)
          p2 (ds/combine (ds/max-fn) {:name :max} input)
          p3 (ds/combine (ds/min-fn) {:name :min} input)
          p4 (ds/combine (ds/sum-fn) {:name :input} input)
          all (ds/concat p1 p2 p3 p4)
          ps (ds/sample 2 input)]
      (ds/write-edn-file math-and-diamond-test {:name :output-all :num-shards 1} all)
      (ds/write-edn-file sample-test {:name :output-sample :num-shards 1} ps)
      (ds/run-pipeline p)
      (let [res (read-file (first (glob-file math-and-diamond-test)))]
        (is (= '(1 3.0 5 15) (sort res))))
      (let [res (read-file (first (glob-file sample-test)))]
        (is (= 2 (count res)))))))

(deftest with-timestamp-test
  (let [p (ds/make-pipeline [])
        input (ds/generate-input [1 2 3 4 5] p)]
    (ds/map (fn [e] (ds/with-timestamp (time/now) e)) input)
    (ds/run-pipeline p))
  ;; TODO: actually add a test
  )

(deftest stateful-map
  (let [p (->> (ds/make-pipeline [])
               (ds/generate-input [(clj->kv [:a 1]) (clj->kv [:a 1])
                                   (clj->kv [:b 2]) (clj->kv [:b 2])]
                                  {:coder (ds/make-kv-coder)})
               (ds/map (fn [[k v]]
                         (let [state (ds/state)
                               current (or (.read state) 0)]
                           (.write state v)
                           (clj->kv [k (+ v current)])))
                       {:stateful? true}))]
    (.. PAssert (that p) (containsInAnyOrder #{(clj->kv [:a 1]) (clj->kv [:a 2]) (clj->kv [:b 2]) (clj->kv [:b 4])}))
    (ds/run-pipeline p)))


(deftest system-map
  (let [p (->> (ds/make-pipeline [])
               (ds/generate-input [{:a 1}
                                   {:b 2}
                                   {:c 3}])
               (ds/map (fn [x]
                         (merge x (ds/system)))
                       {:initialize-fn (fn [] {:init 10})}))]
    (.. PAssert (that p) (containsInAnyOrder #{{:a 1 :init 10}
                                               {:b 2 :init 10}
                                               {:c 3 :init 10}}))
    (ds/run-pipeline p)))


(deftest windows
  (let [now (time/now)
        p (->> (ds/make-pipeline [])
               (ds/generate-input [0. 0.5 2.5 3.])
               (ds/map (fn [e] (ds/with-timestamp (time/minus now (time/seconds e)) e)) {:name :timestamp}))
        _sliding (ds/sliding-windows (time/seconds 2) (time/seconds 1) {:name :sliding} p)
        _fixed (ds/fixed-windows (time/seconds 2) {:name :fixed} p)]
    (ds/run-pipeline p))
  ;; TODO: add test
  )

(deftest session-windows
  (let [now (time/now)
        p (->> (ds/make-pipeline [])
               (ds/generate-input [[:k0 0] [:k1 1] [:k1 2] [:k0 4]])
               (ds/map (fn [[k e]] (ds/with-timestamp (time/minus now (time/seconds e)) [k e])) {:name :timestamp})
               (ds/with-keys (fn [e] (get e 0))))
        session (->> (ds/session-windows (time/seconds 2) p)
                     (ds/group-by-key)
                     (ds/map (fn [[_ elts]] (reduce + (map (fn [[_k v]] v) elts))) {:name :sum}))]
    (.. PAssert (that session) (containsInAnyOrder #{0 3 4}))
    (ds/run-pipeline p)))

(deftest checkpoint
  (with-files [checkpoint-test]
    (let [p (->> (ds/make-pipeline [])
                 (ds/generate-input [0 1 2 3])
                 (ds/map inc {:name :inc :checkpoint checkpoint-test})
                 (ds/map inc {:name :inc-again}))]
      (ds/run-pipeline p)
      (let [cp (flatten (map read-file (glob-file checkpoint-test)))]
        (.. PAssert (that p) (containsInAnyOrder #{2 3 4 5}))
        (is (= '(1 2 3 4) (sort cp)))))))

(deftest compression-out-test
  (with-files [compression-out-test]
    (let [p (ds/make-pipeline [])
          input (ds/generate-input [1 2 3] p)]
      (ds/write-edn-file compression-out-test {:num-shards 1 :compression-type :gzip} input)
      (ds/run-pipeline p)
      (let [res (->> (read-file (GZIPInputStream. (io/input-stream (first (glob-file compression-out-test)))))
                     (into #{}))]
        (is (= res #{1 2 3}))))))

(deftest compression-in-test
  (let [p (ds/make-pipeline [])
        input (ds/read-json-file gzipped-json-file-path {:name :read-json :compression-type :gzip} p)]
    (-> (PAssert/that input)  (.containsInAnyOrder (map int test-data)))
    (ds/run-pipeline p)))
