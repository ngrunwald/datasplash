(ns datasplash.api-test
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [datasplash
             [api :as ds]]
            [me.raynes.fs :as fs]
            [clj-time.core :as time])
  (:import [com.google.cloud.dataflow.sdk.testing TestPipeline DataflowAssert]
           [java.io PushbackReader]))

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
      (.registerCoder clojure.lang.IPersistentCollection (ds/make-nippy-coder))
      (.registerCoder clojure.lang.Keyword (ds/make-nippy-coder)))
    p))

(deftest basic-pipeline
  (let [p (make-test-pipeline)
        input (ds/generate-input [1 2 3 4 5] p)
        proc (ds/map inc {:name "increment"} input)
        filter (ds/filter even? proc)]
    (.. DataflowAssert (that input) (containsInAnyOrder #{1 2 3 4 5}))
    (.. DataflowAssert (that proc) (containsInAnyOrder #{2 3 4 5 6}))
    (.. DataflowAssert (that filter) (containsInAnyOrder #{2 4 6}))
    (is "increment" (.getName proc))
    (ds/run-pipeline p)))

(def test-data [1 2 3 4 5])
(def json-file-path "json-test-input.json")

(defn create-json-input-fixture
  [f]
  (spit json-file-path (str/join "\n" (for [l test-data]
                                        (json/encode l))))
  (f)
  (fs/delete json-file-path))

(use-fixtures :once create-json-input-fixture)

(deftest json-io
  (let [p (make-test-pipeline)
        input (ds/read-json-file json-file-path {:name :read-json} p)]
    (.. DataflowAssert (that input) (containsInAnyOrder (map int test-data)))
    (is "read-json" (.getName input))
    (ds/run-pipeline p)))

(deftest intra-bundle-parallelization-test
  (with-files [intra-bundle-parallelization-test]
    (let [p (make-test-pipeline)
          input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)
          pipe (ds/->> :pipelined input
                       (ds/map inc {:name :inc :intra-bundle-parallelization 5})
                       (ds/filter even? {:name :even? :intra-bundle-parallelization 5}))
          output (ds/write-edn-file intra-bundle-parallelization-test pipe)]
      (ds/run-pipeline p))
    (let [res (into #{} (read-file (first (glob-file intra-bundle-parallelization-test))))]
      (is (= res #{2 4 6})))))

(deftest pt-macro-test
  (with-files [pt-test]
    (let [p (make-test-pipeline)
          input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)
          pipe (ds/->> :pipelined input
                       (ds/map inc {:name :inc})
                       (ds/filter even? {:name :even?}))
          output (ds/write-edn-file pt-test pipe)]
      (ds/run-pipeline p))
    (let [res (into #{} (read-file (first (glob-file pt-test))))]
      (is (= res #{2 4 6})))))

(deftest pt-cond-macro-test
  (with-files [pt-cond-test]
    (let [p (make-test-pipeline)
          input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)
          pipe (ds/cond->> :pipelined input
                           true (ds/map inc {:name :inc})
                           false (ds/filter even? {:name :even?}))
          output (ds/write-edn-file pt-cond-test pipe)]
      (ds/run-pipeline p))
    (let [res (into #{} (read-file (first (glob-file pt-cond-test))))]
      (is (= res #{2 3 4 5 6})))))

(deftest side-inputs-test
  (with-files [side-test]
    (let [p (make-test-pipeline)
          input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)
          side-input (ds/view (ds/generate-input [{1 :a 2 :b 3 :c 4 :d 5 :e}] {:name :side-gen} p))
          proc (ds/map (fn [x] (get-in (ds/side-inputs) [:mapping x]))
                       {:side-inputs {:mapping side-input}}
                       input)
          output (ds/write-edn-file side-test proc)]
      (ds/run-pipeline p))
    (let [res (into #{} (read-file (first (glob-file side-test))))]
      (is (= res #{:a :b :c :d :e})))))

(deftest side-outputs-test
  (with-files [sideout-simple-test sideout-multi-test]
    (let [p (make-test-pipeline)
          input (ds/generate-input [1 2 3 4 5] {:name :main-gen} p)
          {:keys [simple multi]} (ds/map (fn [x] (ds/side-outputs :simple x :multi (* x 10)))
                                         {:side-outputs [:simple :multi]} input)
          output-simple (ds/write-edn-file sideout-simple-test simple)
          output-multi (ds/write-edn-file sideout-multi-test multi)]
      (ds/run-pipeline p))
    (let [res-simple (into #{} (read-file (first (glob-file sideout-simple-test))))
          res-multi (into #{} (read-file (first (glob-file sideout-multi-test))))]
      (is (= res-simple #{1 2 3 4 5}))
      (is (= res-multi #{10 20 30 40 50})))))

(deftest group-test
  (with-files [group-test]
    (let [p (make-test-pipeline)
          input (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] p)
          grouped (ds/group-by :key {:name "group"} input)
          output (ds/write-edn-file group-test grouped)]
      (is "group" (.getName grouped))
      (ds/run-pipeline p)
      (let [res (->> (read-file (first (glob-file group-test)))
                     (map (fn [[k v]] [k (into #{} v)]))
                     (into #{}))]
        (is (res [:a #{{:key :a :lue 65} {:key :a :val 42}}]))
        (is (res [:b #{{:key :b :val 56}}]))))))

(deftest cogroup-test
  (with-files [cogroup-test]
    (let [p (make-test-pipeline)
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/cogroup-by {:name "cogroup-test"}
                                 [[input1 :key] [input2 :key]])
          output (ds/write-edn-file cogroup-test grouped)]
      (ds/run-pipeline p)
      (is "cogroup-test" (.getName grouped))
      (let [res (->> (read-file (first (glob-file cogroup-test)))
                     (map (fn [[k [i1 i2]]] [k [(into #{} i1) (into #{} i2)]]))
                     (into #{}))]
        (is (= res #{[:a [#{{:key :a, :lue 65} {:key :a, :val 42}} #{{:key :a, :uel 65} {:key :a, :lav 42}}]]
                     [:c [#{} #{{:key :c, :foo 42}}]]
                     [:b [#{{:key :b, :val 56}} #{}]]}))))))

(deftest cogroup-drop-nil-test
  (with-files [cogroup-drop-nil-test]
    (let [p (make-test-pipeline)
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/cogroup-by {:name "cogroup-drop-nil-test"}
                                 [[input1 :key] [input2 :key {:drop-nil? true}]])
          output (ds/write-edn-file cogroup-drop-nil-test grouped)]
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
    (let [p (make-test-pipeline)
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/cogroup-by {:name "cogroup-required-test"}
                                 [[input1 :key {:type :required}] [input2 :key]])
          output (ds/write-edn-file cogroup-required-test grouped)]
      (ds/run-pipeline p)
      (is "cogroup-required-test" (.getName grouped))
      (let [res (->> (read-file (first (glob-file cogroup-required-test)))
                     (map (fn [[k [i1 i2]]] [k [(into #{} i1) (into #{} i2)]]))
                     (into #{}))]
        (is (= res #{[:a [#{{:key :a, :lue 65} {:key :a, :val 42}} #{{:key :a, :uel 65} {:key :a, :lav 42}}]]
                     [:b [#{{:key :b, :val 56}} #{}]]}))))))

(deftest cogroup-join-nil-test
  (with-files [cogroup-join-nil-test]
    (let [p (make-test-pipeline)
          input1 (ds/generate-input [{:key :a :val 42} {:val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/cogroup-by {:name "cogroup-join-nil-test"}
                                 [[input1 :key] [input2 :key]])
          output (ds/write-edn-file cogroup-join-nil-test grouped)]
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
    (let [p (make-test-pipeline)
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/join-by {:name "join-test"}
                              [[input1 :key] [input2 :key]] merge)
          output (ds/write-edn-file join-test grouped)]
      (ds/run-pipeline p)
      (is "join-test" (.getName grouped))
      (let [res (into #{} (read-file (first (glob-file join-test))))]
        (is (= res #{{:key :b, :val 56} {:key :c, :foo 42} {:key :a, :lue 65, :lav 42} {:key :a, :val 42, :lav 42}
                     {:key :a, :val 42, :uel 65} {:key :a, :lue 65, :uel 65}}))))))

(deftest join-test-required
  (with-files [join-test-required]
    (let [p (make-test-pipeline)
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
          input2 (ds/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
          grouped (ds/join-by {:name "join-test-required" :collector merge}
                              [[input1 :key]
                               [input2 :key {:type :required}]]
                              merge)
          output (ds/write-edn-file join-test-required grouped)]
      (ds/run-pipeline p)
      (is "join-test-required" (.getName grouped))
      (let [res (into #{} (read-file (first (glob-file join-test-required))))]
        (is (= res #{{:key :c, :foo 42} {:key :a, :lue 65, :lav 42} {:key :a, :val 42, :lav 42}
                     {:key :a, :val 42, :uel 65} {:key :a, :lue 65, :uel 65}}))))))

(deftest combine-pipeline
  (let [p (make-test-pipeline)
        input (ds/generate-input [1 2 3 4 5] p)
        proc (ds/combine + {:name "combine" :scope :global} input)]
    (.. DataflowAssert (that proc) (containsInAnyOrder #{15}))
    (is "combine" (.getName proc))
    (ds/run-pipeline p)))

(deftest combine-juxt
  (with-files [combine-juxt-test]
    (let [p (make-test-pipeline)
          input (ds/generate-input [1 2 3 4 5] p)
          proc (ds/combine (ds/juxt
                            + *
                            (ds/sum-fn)
                            (ds/mean-fn)
                            (ds/max-fn)
                            (ds/min-fn)
                            (ds/count-fn) (ds/count-fn :predicate even?)
                            (ds/max-fn :mapper #(* 10 %)))
                           {:name "combine"} input)
          output (ds/write-edn-file combine-juxt-test proc)]
      (is "combine" (.getName proc))
      (ds/run-pipeline p)
      (let [res (into #{} (read-file (first (glob-file combine-juxt-test))))]
        (is (= res #{'(15 120 15 3.0 5 1 5 2 50)}))))))

(deftest math-and-diamond
  (with-files [math-and-diamond-test sample-test]
    (let [p (make-test-pipeline)
          input (ds/generate-input [1 2 3 4 5] p)
          p1 (ds/combine (ds/mean-fn) {:name :mean} input)
          p2 (ds/combine (ds/max-fn) {:name :max} input)
          p3 (ds/combine (ds/min-fn) {:name :min} input)
          p4 (ds/combine (ds/sum-fn) {:name :input} input)
          all (ds/concat p1 p2 p3 p4)
          ps (ds/sample 2 input)
          output1 (ds/write-edn-file math-and-diamond-test {:name :output-all} all)
          output2 (ds/write-edn-file sample-test {:name :output-sample} ps)]
      (ds/run-pipeline p)
      (let [res (read-file (first (glob-file math-and-diamond-test)))]
        (is (= '(1 3.0 5 15) (sort res))))
      (let [res (read-file (first (glob-file sample-test)))]
        (is (= 2 (count res)))))))

(deftest timestamp
  (let [p (make-test-pipeline)
        input (ds/generate-input [1 2 3 4 5] p)
        proc (ds/map (fn [e] (ds/with-timestamp (time/now) e)) input)]
    (ds/run-pipeline p)))

(deftest windows
  (let [now (time/now)
        p (->> (make-test-pipeline)
               (ds/generate-input [0. 0.5 2.5 3.])
               (ds/map (fn [e] (ds/with-timestamp (time/minus now (time/seconds e)) e)) {:name :timestamp}))
        sliding (ds/sliding-windows (time/seconds 2) (time/seconds 1) {:name :sliding} p)
        fixed (ds/fixed-windows (time/seconds 2) {:name :fixed} p)]
    (ds/run-pipeline p)))

(deftest session-windows
  (let [now (time/now)
        p (->> (make-test-pipeline)
               (ds/generate-input [[:k0 0] [:k1 1] [:k1 2] [:k0 4]])
               (ds/map (fn [[k e]] (ds/with-timestamp (time/minus now (time/seconds e)) [k e])) {:name :timestamp})
               (ds/with-keys (fn [e] (get e 0))))
        session (->> (ds/session-windows (time/seconds 2) p)
                     (ds/group-by-key)
                     (ds/map (fn [[_ elts]] (reduce + (map (fn [[k v]] v) elts))) {:name :sum}))]
    (.. DataflowAssert (that session) (containsInAnyOrder #{0 3 4}))
    (ds/run-pipeline p)))
