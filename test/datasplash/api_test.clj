(ns datasplash.api-test
  (:require [clojure.test :refer :all]
            [datasplash
             [api :as ds]]
            [me.raynes.fs :as fs]
            [clojure.java.io :as io]
            [clojure.edn :as edn])
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


(deftest side-inputs-test
  (with-files [side-test]
    (let [p (make-test-pipeline)
          input (ds/generate-input [1 2 3 4 5] p)
          side-input (ds/view (ds/generate-input [{1 :a 2 :b 3 :c 4 :d 5 :e}] p))
          proc (ds/map (fn [x] (get-in (ds/side-inputs) [:mapping x]))
                       {:side-inputs {:mapping side-input}} input)
          output (ds/write-edn-file side-test proc)]
      (ds/run-pipeline p))
    (let [res (into #{} (read-file (first (glob-file side-test))))]
      (is (= res #{:a :b :c :d :e})))))

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
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] p)
          input2 (ds/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] p)
          grouped (ds/cogroup-by {:name "cogroup-test"}
                                 [[input1 :key] [input2 :key]])
          output (ds/write-edn-file cogroup-test grouped)]
      (ds/run-pipeline p)
      (is "cogroup-test" (.getName grouped))
      (let [res (->> (read-file (first (glob-file cogroup-test)))
                     (map (fn [[k i1 i2]] [k (into #{} i1) (into #{} i2)]))
                     (into #{}))]
        (is (= res #{[:a #{{:key :a, :lue 65} {:key :a, :val 42}} #{{:key :a, :uel 65} {:key :a, :lav 42}}]
                     [:c #{} #{{:key :c, :foo 42}}] [:b #{{:key :b, :val 56}} #{}]}))))))

(deftest join-test
  (with-files [cogroup-test]
    (let [p (make-test-pipeline)
          input1 (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] p)
          input2 (ds/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] p)
          grouped (ds/join-by {:name "join-test"}
                              [[input1 :key] [input2 :key]] merge)
          output (ds/write-edn-file cogroup-test grouped)]
      (ds/run-pipeline p)
      (is "join-test" (.getName grouped))
      (let [res (into #{} (read-file (first (glob-file cogroup-test))))]
        (is (= res #{{:key :b, :val 56} {:key :c, :foo 42} {:key :a, :lue 65, :lav 42} {:key :a, :val 42, :lav 42}
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
          p1 (ds/mean input)
          p2 (ds/max input)
          p3 (ds/min input)
          p4 (ds/sum input)
          all (ds/concat p1 p2 p3 p4)
          ps (ds/sample 2 input)
          output1 (ds/write-edn-file math-and-diamond-test all)
          output2 (ds/write-edn-file sample-test ps)]
      (ds/run-pipeline p)
      (let [res (read-file (first (glob-file math-and-diamond-test)))]
        (is (= '(1 3.0 5 15) (sort res))))
      (let [res (read-file (first (glob-file sample-test)))]
        (is (= 2 (count res)))))))

(deftest write-by
  (with-files [true-vals false-vals]
    (let [p (make-test-pipeline)
                    input (ds/generate-input [1 2 3 4 5] p)
                    mapping (ds/make-partition-mapping [true false])
          _ (ds/write-edn-file-by even? mapping (fn [res] (if res true-vals false-vals)) {:name "write-by"} input)]
      (ds/run-pipeline p)
      (let [res-true (into #{} (read-file (first (glob-file true-vals))))
            res-false (into #{} (read-file (first (glob-file false-vals))))]
        (is (= #{2 4} res-true))
        (is (= #{1 3 5} res-false))))))
