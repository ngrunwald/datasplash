(ns datasplash.api-test
  (:require [clojure.test :refer :all]
            [datasplash.api :as ds]
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

(deftest basic-pipeline
  (let [p (TestPipeline/create)
        input (ds/generate-input [1 2 3 4 5] p)
        proc (ds/map inc {:name "increment"} input)
        filter (ds/filter even? proc)]
    (.. DataflowAssert (that input) (containsInAnyOrder #{1 2 3 4 5}))
    (.. DataflowAssert (that proc) (containsInAnyOrder #{2 3 4 5 6}))
    (.. DataflowAssert (that filter) (containsInAnyOrder #{2 4 6}))
    (is "increment" (.getName proc))
    (.run p)))

(deftest group-test
  (with-files [group-test]
    (let [p (TestPipeline/create)
          input (ds/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] p)
          grouped (ds/group-by :key {:name "group"} input)
          output (ds/write-edn-file group-test grouped)]
      (is "group" (.getName grouped))
      (.run p)
      (let [res (into #{} (read-file (first (glob-file group-test))))]
        (is (res [:a [{:key :a :lue 65} {:key :a :val 42}]]))
        (is (res [:b [{:key :b :val 56}]]))))))
