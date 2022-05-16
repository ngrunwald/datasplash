(ns datasplash.api-test
  (:require
   [charred.api :as charred]
   [clj-time.core :as time]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datasplash.api :as sut]
   [datasplash.core :refer [clj->kv]]
   [tools.io :as tio])
  (:import
   (java.io PushbackReader)
   (java.util.zip GZIPInputStream)
   (org.apache.beam.sdk.testing PAssert)
   (org.apache.beam.sdk.values PCollectionView)))

(defn- read-edn-file
  "Helper to check write-edn-file and ensure compression is used when specified."
  ([path] (read-edn-file nil path))
  ([{:keys [compressed?]} path]
   (let [in (cond->> (io/input-stream path)
              compressed? (GZIPInputStream.))]
     (with-open [rdr (PushbackReader. (io/reader in))]
       (loop [acc []]
         (let [line (edn/read {:eof ::eof} rdr)]
           (if (= line ::eof)
             acc
             (recur (conj acc line)))))))))

(defn- make-fixture
  "Pass a filename with either .edn or .json as an extension to select
  the output format. If it also suffixed with .gz (e.g.,
  'xx.json.gz'), the file will also be compressed."
  [base-path file-path data]
  (let [fmt (->> (re-matches #"(.*).(edn|json)(.*)?" file-path)
                 (drop 2)
                 first
                 keyword)
        writer-fn (case fmt
                    :json tio/write-jsons-file
                    tio/write-edns-file)
        dest-file-path (tio/join-path base-path file-path)]
    (writer-fn dest-file-path data)
    dest-file-path))

(deftest read-edn-file-test
  (tio/with-tempdir [tmp-dir]
    (let [data #{{:id "1" :name "elem 1"}
                 {:id "2" :name "elem 2"}
                 {:id "3" :name "elem 3"}
                 {:id "4" :name "elem 4"}
                 {:id "5" :name "elem 5"}}]

      (testing "nominal case"
        (let [p (sut/make-pipeline [])
              fixture-path (make-fixture tmp-dir "colls.edn" data)
              rslt (sut/read-edn-file fixture-path
                                      {:name :read-edn}
                                      p)]

          (is (str/starts-with? (.getName rslt) "read-edn"))

          (is (-> (PAssert/that rslt)
                  (.containsInAnyOrder data)))

          (sut/wait-pipeline-result (sut/run-pipeline p)))

        (testing "gz-compressed"
          (let [p (sut/make-pipeline [])
                fixture-path (make-fixture tmp-dir "colls.edn.gz" data)
                rslt (sut/read-edn-file fixture-path
                                        {:name :read-edn-gz
                                         :compression-type :gzip}
                                        p)]
            (is (str/starts-with? (.getName rslt) "read-edn-gz"))
            (is (-> (PAssert/that rslt)
                    (.containsInAnyOrder data)))

            (sut/wait-pipeline-result (sut/run-pipeline p)))))

      (testing "nested map w date fields"
        (let [data #{{:id "4" :name "elem 4"
                      :lifespan {:start #inst "2004-04-01T00:00:00.000-00:00"
                                 :end #inst "2007-01-01T00:00:00.000-00:00"}}
                     {:id "3" :name "elem 3"
                      :lifespan {:start #inst "2017-01-01T00:00:00.000-00:00"
                                 :end #inst "2019-12-01T00:00:00.000-00:00"}}
                     {:id "1" :name "elem 1"
                      :lifespan {:start #inst "2005-02-01T00:00:00.000-00:00"
                                 :end #inst "2006-11-01T00:00:00.000-00:00"}}}
              fixture-path (make-fixture tmp-dir "colls.edn" data)

              p (sut/make-pipeline [])
              rslt (sut/read-edn-file fixture-path
                                      {:name :read-edn}
                                      p)]

          (is (-> (PAssert/that rslt)
                  (.containsInAnyOrder data)))

          (sut/wait-pipeline-result (sut/run-pipeline p)))))))

(deftest read-edn-files-test
  (let [data #{{:id "1" :name "elem 1"}
               {:id "2" :name "elem 2"}
               {:id "3" :name "elem 3"}
               {:id "4" :name "elem 4"}
               {:id "5" :name "elem 5"}
               {:id "6" :name "elem 6"}}
        [fix-1 fix-2] (split-at 4 data)]

    (tio/with-tempdir [tmp-dir]
      (let [file-list [(make-fixture tmp-dir "01.edn" fix-1)
                       (make-fixture tmp-dir "02.edn" fix-2)]
            p (sut/make-pipeline [])
            rslt (-> (sut/generate-input file-list {:name :file-list} p)
                     sut/read-edn-files)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder data)))
        (sut/wait-pipeline-result (sut/run-pipeline p))))))

(deftest write-edn-file-test
  (testing "nominal case"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])]

        (->> (-> [1 2 3]
                 (sut/generate-input p))
             (sut/write-edn-file (tio/join-path tmp-dir "test")
                                 {:num-shards 1}))

        (sut/wait-pipeline-result (sut/run-pipeline p))

        (let [res (->> (read-edn-file (first (tio/list-files tmp-dir)))
                       (into #{}))]
          (is (= #{1 2 3}
                 res))))))

  (testing "gz-compression"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])]

        (->> (-> [1 2 3]
                 (sut/generate-input p))
             (sut/write-edn-file (tio/join-path tmp-dir "test")
                                 {:num-shards 1
                                  :compression-type :gzip}))

        (sut/wait-pipeline-result (sut/run-pipeline p))

        (let [file-path (first (tio/list-files tmp-dir))
              res (->> (read-edn-file {:compressed? true} file-path)
                       (into #{}))]
          (is (str/includes? file-path ".gz"))
          (is (= #{1 2 3}
                 res)))))))

(deftest read-json-file-test
  (tio/with-tempdir [tmp-dir]
    (testing "nominal case"
      (let [data #{1.0 2.0 3.0}
            fixture-path (make-fixture tmp-dir "colls.json" data)
            p (sut/make-pipeline [])
            input (sut/read-json-file fixture-path
                                      {:name :read-json}
                                      p)]
        (is (str/starts-with? (.getName input) "read-json"))

        (is (-> (PAssert/that input)
                (.containsInAnyOrder data)))

        (sut/wait-pipeline-result (sut/run-pipeline p))))

    (testing "with key-fn"
      (let [data #{{:id "1" :name "elem 1"}
                   {:id "2" :name "elem 2"}
                   {:id "3" :name "elem 3"}}
            fixture-path (make-fixture tmp-dir "colls.json" data)
            p (sut/make-pipeline [])
            input (sut/read-json-file fixture-path
                                      {:name :read-json-k
                                       :key-fn keyword}
                                      p)]
        (is (str/starts-with? (.getName input) "read-json-k"))

        (is (-> (PAssert/that input)
                (.containsInAnyOrder data)))

        (sut/wait-pipeline-result (sut/run-pipeline p))))

    (testing "nested map w date fields"
      (let [data #{{:id "3" :name "elem 3"
                    :created {:date "2017-01-01T00:00:00.000-00:00"}}
                   {:id "1" :name "elem 1"
                    :created {:date "2005-02-01T00:00:00Z"}}}
            fixture-path (make-fixture tmp-dir "colls.json" data)
            p (sut/make-pipeline [])
            input (sut/read-json-file fixture-path
                                      {:name :read-json-nested
                                       :key-fn keyword}
                                      p)]

        (is (-> (PAssert/that input)
                (.containsInAnyOrder data)))

        (sut/wait-pipeline-result (sut/run-pipeline p)))))

  (testing "gz-compressed"
    (tio/with-tempdir [tmp-dir]
      (let [data #{1.0 2.0 3.0}
            fixture-path (make-fixture tmp-dir "colls.json.gz" data)
            p (sut/make-pipeline [])
            input (sut/read-json-file fixture-path
                                      {:name :read-json-gz
                                       :compression-type :gzip}
                                      p)]
        (is (str/starts-with? (.getName input) "read-json-gz"))
        (is (-> (PAssert/that input)
                (.containsInAnyOrder data)))

        (sut/wait-pipeline-result (sut/run-pipeline p))))))

(deftest read-json-files-test
  (testing "with key-fn"
    (let [data #{{:id "1" :name "elem 1"}
                 {:id "2" :name "elem 2"}
                 {:id "3" :name "elem 3"}
                 {:id "4" :name "elem 4"}
                 {:id "5" :name "elem 5"}
                 {:id "6" :name "elem 6"}}
          [fix-1 fix-2] (split-at 4 data)]

      (tio/with-tempdir [tmp-dir]
        (let [file-list [(make-fixture tmp-dir "01.json" fix-1)
                         (make-fixture tmp-dir "02.json" fix-2)]
              p (sut/make-pipeline [])
              rslt (->> (sut/generate-input file-list {:name :file-list} p)
                        (sut/read-json-files {:key-fn keyword}))]

          (is (-> (PAssert/that rslt)
                  (.containsInAnyOrder data)))
          (sut/wait-pipeline-result (sut/run-pipeline p)))))))

(deftest generate-input-test
  (testing "nominal case"
    (let [input #{1 2 3}]
      (tio/with-tempdir [tmp-dir]
        (let [p (sut/make-pipeline [])
              rslt (sut/generate-input input {:name :test-input} p)]

          (is (str/starts-with? (.getName rslt) "test-input"))

          (sut/write-edn-file (tio/join-path tmp-dir "test")
                              {:num-shards 1} rslt)

          (sut/wait-pipeline-result (sut/run-pipeline p)))

        (let [rslt (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                        (into #{}))]
          (is (= input
                 rslt))))))

  (testing "map coll"
    (let [input #{{:id "1" :label "s"}
                  {:id "2" :label "cc"}}]
      (tio/with-tempdir [tmp-dir]
        (let [p (sut/make-pipeline [])
              rslt (sut/generate-input input p)]

          (sut/write-edn-file (tio/join-path tmp-dir "test")
                              {:num-shards 1} rslt)

          (sut/wait-pipeline-result (sut/run-pipeline p)))
        (let [rslt (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                        (into #{}))]
          (is (= input
                 rslt))))))

  (testing "booleans"
    (let [input #{true false}]
      (tio/with-tempdir [tmp-dir]
        (let [p (sut/make-pipeline [])
              rslt (sut/generate-input input p)]

          (sut/write-edn-file (tio/join-path tmp-dir "test")
                              {:num-shards 1} rslt)

          (sut/wait-pipeline-result (sut/run-pipeline p)))
        (let [rslt (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                        (into #{}))]
          (is (= input
                 rslt))))))

  ;; TODO: fix me! generate-input is broken with boolean values in maps :(
  ;; see issue #101
  ;; (testing "map coll w/ boolean values"
  ;;   (let [input #{{:done true} {:done false}}]
  ;;     (tio/with-tempdir [tmp-dir]
  ;;       (let [p (sut/make-pipeline [])
  ;;             rslt (sut/generate-input input p)]

  ;;         (sut/write-edn-file (tio/join-path tmp-dir "test")
  ;;                             {:num-shards 1} rslt)

  ;;         (sut/wait-pipeline-result (sut/run-pipeline p)))
  ;;       (let [rslt (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
  ;;                       (into #{}))]
  ;;         (is (= input
  ;;                rslt))))))
  )

(deftest map-test
  (testing "with system"
    (let [p (sut/make-pipeline [])
          input (-> [{:a 1} {:b 2} {:c 3}]
                    (sut/generate-input p))
          rslt (sut/map (fn [x]
                          (merge x (sut/system)))
                        {:name :map-w-sys
                         :initialize-fn (fn [] {:init 10})}
                        input)]

      (is (str/starts-with? (.getName rslt) "map-w-sys"))
      (is (-> (PAssert/that rslt)
              (.containsInAnyOrder [{:a 1 :init 10}
                                    {:b 2 :init 10}
                                    {:c 3 :init 10}])))

      (sut/wait-pipeline-result (sut/run-pipeline p))))

  (testing "stateful"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])
            input [(clj->kv [:a 1]) (clj->kv [:a 1])
                   (clj->kv [:b 2]) (clj->kv [:b 2])]
            rslt (->> (sut/generate-input input {:coder (sut/make-kv-coder)} p)
                      (sut/map (fn [[k v]]
                            (let [state (sut/state)
                                  current (or (.read state) 0)]
                              (.write state v)
                              (clj->kv [k (+ v current)])))
                          {:stateful? true}))]

        ;; TODO: check state

        (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} rslt)
        (sut/wait-pipeline-result (sut/run-pipeline p)))

      (let [rslt (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                      (into #{}))]
        (is (= #{[:a 2] [:b 2] [:a 1] [:b 4]}
               rslt)))))

  (testing "checkpoint"
    (tio/with-tempdir [tmp-dir]
      (let [p (->> (sut/make-pipeline []))
            rslt (->> (sut/generate-input [0 1 2 3] p)
                      (sut/map inc {:name :inc :checkpoint (tio/join-path tmp-dir "02-checkpoint")})
                      (sut/map inc {:name :inc-again}))]

        (sut/write-edn-file (tio/join-path tmp-dir "01-test") {:num-shards 1} rslt)
        (sut/wait-pipeline-result (sut/run-pipeline p))

        (let [file-list (sort (tio/list-files tmp-dir))]
          (is (> (count file-list) 1)
              "checkpoint file(s) generated")

          (let [rslt (->> (tio/read-edns-file (first file-list))
                          (into #{}))]
            (is (= #{2 3 4 5}
                   rslt)
                "result valid"))
          (let [checkpoint-rslt (->> (filter #(str/includes? % "checkpoint") file-list)
                                     tio/read-edns-files
                                     (into #{}))]
            (is (= #{1 2 3 4}
                   checkpoint-rslt)
                "checkpoint values valid")))))))

(deftest map-kv-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (-> [{:k 1 :v 2} {:k 3 :v 4} {:k 5 :v 6}]
                    (sut/generate-input p))
          rslt (sut/map-kv (fn [{:keys [k v]}]
                             [k v])
                           {:name :map-vals}
                           input)]

      (is (str/starts-with? (.getName rslt) "map-vals"))
      (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} rslt)

      (sut/wait-pipeline-result (sut/run-pipeline p)))

    (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                   (into #{}))]
      (is (= #{[3 4] [5 6] [1 2]}
             res)))))

(deftest mapcat-test
  (let [p (sut/make-pipeline [])
        input (-> [{:values [1 2]} {:values [3 4]} {:values [5 6]}]
                  (sut/generate-input p))
        rslt (sut/mapcat #(:values %)
                         {:name :mapcat-vals}
                         input)]

    (is (str/starts-with? (.getName rslt) "mapcat-vals"))
    (is (-> (PAssert/that rslt)
            (.containsInAnyOrder [1 2 3 4 5 6])))

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest filter-test
  (let [p (sut/make-pipeline [])
        input (sut/generate-input [1 2 3 4 5] {:name :main-gen} p)
        rslt (sut/filter even? {:name :filter-even} input)]

    (is (str/starts-with? (.getName rslt) "filter-even"))

    (is (-> (PAssert/that rslt)
            (.containsInAnyOrder #{2 4})))

    (let [status (sut/wait-pipeline-result (sut/run-pipeline p))]
      (is (= :done status)))))

(deftest with-keys-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (-> [{:id "1" :rank 4}
                     {:id "2" :rank 3}
                     {:id "3" :rank 4}
                     {:id "4" :rank 1}]
                    (sut/generate-input {:name :test-input} p))
          rslt (->> input
                    (sut/with-keys :rank {:name :w-k}))]

      (is (str/starts-with? (.getName rslt) "w-k"))

      (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} rslt)

      (sut/wait-pipeline-result (sut/run-pipeline p)))

    (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                   (into #{}))]
      (is (= #{[1 {:id "4" :rank 1}]
               [4 {:id "3" :rank 4}]
               [3 {:id "2" :rank 3}]
               [4 {:id "1" :rank 4}]}
             res)))))

(deftest group-by-key-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (-> [{:id "1" :rank 4 :name "no 1"}
                     {:id "2" :rank 3 :name "no 2"}
                     {:id "3" :rank 4 :name "no 3"}
                     {:id "4" :rank 1 :name "no 4"}]
                    (sut/generate-input {:name :test-input} p))
          rslt (->> input
                    (sut/with-keys :rank)
                    (sut/group-by-key {:name :group-by-rank}))]

      (is (str/starts-with? (.getName rslt) "group-by-rank"))

      (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} rslt)

      (sut/wait-pipeline-result (sut/run-pipeline p)))

    (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                   (filter (fn [[k _]] (= k 4)))
                   (map (fn [[_ v]] v))
                   first
                   (into #{}))]
      (is (= #{{:id "1" :rank 4 :name "no 1"} {:id "3" :rank 4 :name "no 3"}}
             res)))))

(deftest ->>-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [1 2 3 4 5] {:name :main-gen} p)]

      (sut/->> :pipelined input
               (sut/map inc {:name :inc})
               (sut/filter even? {:name :even?})
               (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1}))
      (sut/wait-pipeline-result (sut/run-pipeline p)))

    (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                   (into #{}))]
      (is (= #{2 4 6}
             res)))))

(deftest cond->>-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [1 2 3 4 5] {:name :main-gen} p)]

      (->> (sut/cond->> :pipelined input
                        true (sut/map inc {:name :inc})
                        false (sut/filter even? {:name :even?}))
           (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1}))
      (sut/wait-pipeline-result (sut/run-pipeline p)))

    (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                   (into #{}))]
      (is (= #{2 3 4 5 6}
             res)))))

(deftest partition-by-test
  (let [p (sut/make-pipeline [])
        input (sut/generate-input [1 2 3 4 5 6 7 8 9] p)
        pcolls (sut/partition-by (fn [e _] (if (odd? e) 1 0))
                                 2
                                 {:name :partition}
                                 input)
        [even-coll odd-coll] (.getAll pcolls)]

    (is (-> (PAssert/that even-coll)
            (.containsInAnyOrder '(2 4 6 8))))
    (is (-> (PAssert/that odd-coll)
            (.containsInAnyOrder '(1 3 5 7 9))))

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest view-test
  (let [p (sut/make-pipeline [])
        rslt (->> (sut/generate-input ["a" "b" "c"] {:name :side-gen} p)
                  (sut/combine (sut/combine-fn (fn [acc v] (conj acc v))
                                               identity
                                               (fn [& accs] (apply set/union accs))
                                               (fn [] #{}))
                               {:name :as-set})
                  (sut/view {:name :si}))
        output (->> (sut/generate-input [{:id "a"} {:id "d"} {:id "c"}] {:name :main-gen} p)
                    (sut/filter (fn [{:keys [id]}]
                                  (get-in (sut/side-inputs) [:mapping id]))
                                {:side-inputs {:mapping rslt}}))]

    (is (instance? PCollectionView rslt))
    (is (-> (PAssert/that output)
            (.containsInAnyOrder [{:id "a"} {:id "c"}])))

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest side-inputs-test
  (let [p (sut/make-pipeline [])
        side-input (-> [{1 :a 2 :b 3 :c 4 :d 5 :e}]
                       (sut/generate-input {:name :side-gen} p)
                       sut/view)
        rslt (->> (sut/generate-input [1 2 3 4 5] {:name :main-gen} p)
                  (sut/map (fn [x] (get-in (sut/side-inputs) [:mapping x]))
                           {:side-inputs {:mapping side-input}}))]

    (is (-> (PAssert/that rslt)
            (.containsInAnyOrder [:a :b :c :d :e])))

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest side-outputs-test
  (let [p (sut/make-pipeline [])
        input (sut/generate-input [1 2 3 4 5] {:name :main-gen} p)
        {:keys [simple multi]} (->> input
                                    (sut/map (fn [x] (sut/side-outputs :simple x :multi (* x 10)))
                                             {:side-outputs [:simple :multi]}))]

    (is (-> (PAssert/that simple)
            (.containsInAnyOrder [1 2 3 4 5])))

    (is (-> (PAssert/that multi)
            (.containsInAnyOrder [10 20 30 40 50])))

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest group-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (-> [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}]
                    (sut/generate-input p))
          grouped (sut/group-by :key {:name "group"} input)]

      (is (str/starts-with? (.getName grouped) "group"))

      (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} grouped)
      (sut/wait-pipeline-result (sut/run-pipeline p))

      (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                     (map (fn [[k v]] [k (into #{} v)]))
                     (into #{}))]
        (is (res [:a #{{:key :a :lue 65} {:key :a :val 42}}]))
        (is (res [:b #{{:key :b :val 56}}]))))))

(deftest cogroup-test
  (testing "nominal case"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])
            input1 (sut/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
            input2 (sut/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
            grouped (sut/cogroup-by {:name "cogroup-test"}
                                    [[input1 :key] [input2 :key]])]

        (is (str/starts-with? (.getName grouped) "cogroup-test"))

        (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} grouped)
        (sut/wait-pipeline-result (sut/run-pipeline p))

        (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                       (map (fn [[k [i1 i2]]] [k [(into #{} i1) (into #{} i2)]]))
                       (into #{}))]
          (is (= #{[:a [#{{:key :a :lue 65} {:key :a :val 42}} #{{:key :a :uel 65} {:key :a :lav 42}}]]
                   [:c [#{} #{{:key :c :foo 42}}]]
                   [:b [#{{:key :b :val 56}} #{}]]}
                 res))))))

  (testing "large set of pcollection"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])
            ;; pcolls is something like
            ;; [ [{:i 0 :key 0} {:i 1 :key 0} ...] 🠐 this is a pcoll
            ;;   [{:i 0 :key 1} {:i 1 :key 1} ...]
            ;;   ... ]
            nb-pcolls 101
            pcolls (mapv (fn [k-pcoll]
                           (sut/generate-input
                            (map (fn [i] {:i i :key k-pcoll}) (range 5))
                            {:name (str "gen-" k-pcoll)} p))
                         (range nb-pcolls))
            grouped (sut/cogroup-by {:name "join-fitments"
                                     :collector (fn [[_id same-i]] same-i)}
                                    (mapv #(vector % :i) pcolls))]

        (is (str/starts-with? (.getName grouped) "join-fitments"))

        (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} grouped)
        (sut/wait-pipeline-result (sut/run-pipeline p))

        (doseq [line (tio/read-edns-file (first (tio/list-files tmp-dir)))
                :let [same-i (mapcat identity line)]]
          (is (= 1
                 (count (distinct (map :i same-i)))))
          (is (= (map :key same-i)
                 (range nb-pcolls)))))))

  (testing "drop-nil"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])
            input1 (sut/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
            input2 (sut/generate-input [{:key :a :lav 42} {:uel 65} {:key :c :foo 42}] {:name :gen2} p)
            grouped (sut/cogroup-by {:name "cogroup-drop-nil-test"}
                                    [[input1 :key] [input2 :key {:drop-nil? true}]])]
        (is (str/starts-with? (.getName grouped) "cogroup-drop-nil-test"))

        (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} grouped)
        (sut/wait-pipeline-result (sut/run-pipeline p))

        (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                       (map (fn [[k [i1 i2]]] [k [(into #{} i1) (into #{} i2)]]))
                       (into #{}))]
          (is (= #{[:a [#{{:key :a :lue 65} {:key :a :val 42}} #{{:key :a :lav 42}}]]
                   [:c [#{} #{{:key :c :foo 42}}]]
                   [:b [#{{:key :b :val 56}} #{}]]}
                 res))))))

  (testing "required"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])
            input1 (sut/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
            input2 (sut/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
            grouped (sut/cogroup-by {:name "cogroup-required-test"}
                                    [[input1 :key {:type :required}] [input2 :key]])]

        (is (str/starts-with? (.getName grouped) "cogroup-required-test"))

        (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} grouped)
        (sut/wait-pipeline-result (sut/run-pipeline p))

        (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                       (map (fn [[k [i1 i2]]] [k [(into #{} i1) (into #{} i2)]]))
                       (into #{}))]
          (is (= #{[:a [#{{:key :a :lue 65} {:key :a :val 42}} #{{:key :a :uel 65} {:key :a :lav 42}}]]
                   [:b [#{{:key :b :val 56}} #{}]]}
                 res))))))

  (testing "join-nil"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])
            input1 (sut/generate-input [{:key :a :val 42} {:val 56} {:key :a :lue 65}] {:name :gen1} p)
            input2 (sut/generate-input [{:key :a :lav 42} {:uel 65} {:key :c :foo 42}] {:name :gen2} p)
            grouped (sut/cogroup-by {:name "cogroup-join-nil-test"}
                                    [[input1 :key] [input2 :key]])]

        (is (str/starts-with? (.getName grouped) "cogroup-join-nil-test"))

        (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} grouped)
        (sut/wait-pipeline-result (sut/run-pipeline p))

        (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                       (map (fn [[k [i1 i2]]] [k [(into #{} i1) (into #{} i2)]]))
                       (into #{}))]
          (is (= #{[:a [#{{:key :a :lue 65} {:key :a :val 42}} #{{:key :a :lav 42}}]]
                   [nil [#{{:val 56}} #{}]]
                   [nil [#{} #{{:uel 65}}]]
                   [:c [#{} #{{:key :c :foo 42}}]]}
                 res)))))))

(deftest join-by-test
  (testing "nomical case"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])
            input1 (sut/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
            input2 (sut/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
            grouped (sut/join-by {:name "join-test"}
                                 [[input1 :key] [input2 :key]] merge)]

        (is (str/starts-with? (.getName grouped) "join-test"))

        (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} grouped)
        (sut/wait-pipeline-result (sut/run-pipeline p))

        (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                       (into #{}))]
          (is (= #{{:key :b :val 56} {:key :c :foo 42} {:key :a :lue 65 :lav 42} {:key :a :val 42 :lav 42}
                   {:key :a :val 42 :uel 65} {:key :a :lue 65 :uel 65}}
                 res))))))

  (testing "required"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])
            input1 (sut/generate-input [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] {:name :gen1} p)
            input2 (sut/generate-input [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] {:name :gen2} p)
            grouped (sut/join-by {:name "join-test-required" :collector merge}
                                 [[input1 :key]
                                  [input2 :key {:type :required}]]
                                 merge)]

        (is (str/starts-with? (.getName grouped) "join-test-required"))

        (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} grouped)
        (sut/wait-pipeline-result (sut/run-pipeline p))

        (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                       (into #{}))]
          (is (= #{{:key :c :foo 42} {:key :a :lue 65 :lav 42} {:key :a :val 42 :lav 42}
                   {:key :a :val 42 :uel 65} {:key :a :lue 65 :uel 65}}
                 res)))))))

(deftest distinct-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [1 2 3 2 4 1 5] p)
          rslt (sut/distinct {:name :unique} input)]

      (is (str/starts-with? (.getName rslt) "unique"))
      (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} rslt)

      (sut/wait-pipeline-result (sut/run-pipeline p)))

    (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                   (sort))]
      (is (= '(1 2 3 4 5)
             res)))))

(deftest sample-test
  (tio/with-tempdir [tmp-dir]
    (let [data #{1 2 3 4 5 6 7}
          p (sut/make-pipeline [])
          input (sut/generate-input data p)
          rslt (sut/sample 3 input)]

      (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} rslt)

      (sut/wait-pipeline-result (sut/run-pipeline p))

      (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                     (into #{}))]
        (is (set/subset? res data))))))

(deftest concat-test
  (let [p (sut/make-pipeline [])
        input-1 (sut/generate-input [1 2 3] p)
        input-2 (sut/generate-input [4 5 6] p)
        rslt (sut/concat {:name :concat-pcolls} input-1 input-2)]

    (is (str/starts-with? (.getName rslt) "concat-pcolls"))
    (is (-> (PAssert/that rslt)
            (.containsInAnyOrder #{1 2 3 4 5 6})))

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest combine-test
  (let [p (sut/make-pipeline [])
        input (sut/generate-input [1 2 3 4 5] p)
        proc (sut/combine + {:name "combine" :scope :global} input)]

    (is (str/starts-with? (.getName proc) "combine"))
    (is (-> (PAssert/that proc)
            (.containsInAnyOrder #{15})))

    (sut/wait-pipeline-result (sut/run-pipeline p))))


(defn- test-combine-fn
  [combine-fn]
  (let [p (sut/make-pipeline [])
        input (sut/generate-input [{:a 1} {:b 2} {:c 3} {:d 4} {:e 5}] p)
        proc (sut/combine combine-fn
                         {:name "combine" :scope :global} input)]

    (is (str/starts-with? (.getName proc) "combine"))
    (is (-> (PAssert/that proc)
            (.containsInAnyOrder #{{:a 1 :b 2 :c 3 :d 4 :e 5}})))

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest combine-fn-test
  (let [reducef (fn [acc x] (merge acc x))
        extractf identity
        combinef (fn [& accs] (apply merge accs))
        initf (fn [] {})]

    (testing "discrete args"
      (test-combine-fn (sut/combine-fn reducef extractf combinef initf)))

    (testing "args map"
      (doseq [m [{:reduce reducef :extract extractf :combine combinef :init initf}
                 {:reduce reducef                   :combine combinef :init initf}
                 {:reduce merge                                       :init initf}
                 {:reduce merge}]]
        (test-combine-fn (sut/combine-fn m))))))

(deftest juxt-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [1 2 3 4 5] p)
          proc (sut/combine (sut/juxt
                             + *
                             (sut/sum-fn)
                             (sut/mean-fn)
                             (sut/max-fn)
                             (sut/min-fn)
                             (sut/count-fn)
                             (sut/count-fn :predicate even?)
                             (sut/max-fn :mapper #(* 10 %)))
                            {:name "combine-juxt"} input)]

      (is (str/starts-with? (.getName proc) "combine-juxt"))

      (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} proc)

      (sut/wait-pipeline-result (sut/run-pipeline p))

      (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                     (into #{}))]
        (is (= #{'(15 120 15 3.0 5 1 5 2 50)}
               res))))))

(deftest count-fn-test
  (testing "nominal case"
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [10 30 60] p)
          rslt (sut/combine (sut/count-fn) input)]

      (is (-> (PAssert/that rslt)
              (.containsInAnyOrder #{3})))

      (sut/wait-pipeline-result (sut/run-pipeline p))))

  (testing "with options"
    (testing "with pred"
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [1 2 3 4 5 6] p)
            rslt (sut/combine (sut/count-fn {:predicate even?}) input)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder #{3})))

        (sut/wait-pipeline-result (sut/run-pipeline p))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [{:ids #{1 2 3} :rank 2}
                                       {:ids #{4}}
                                       {:ids #{5} :rank 1}] p)
            rslt (sut/combine
                  (sut/count-fn {:predicate :rank :mapper #(count (:ids %))}) input)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder #{4})))

        (sut/wait-pipeline-result (sut/run-pipeline p))))))

(deftest sum-fn-test
  (testing "nominal case"
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [10 20 30] p)
          rslt (sut/combine (sut/sum-fn) input)]

      (is (-> (PAssert/that rslt)
              (.containsInAnyOrder #{60})))

      (sut/wait-pipeline-result (sut/run-pipeline p))))

  (testing "with options"
    (testing "with pred"
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [1 2 3 4 5 6 7] p)
            rslt (sut/combine (sut/sum-fn {:predicate odd?}) input)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder #{16})))

        (sut/wait-pipeline-result (sut/run-pipeline p))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [1 2 3 4 5 6 7] p)
            rslt (sut/combine
                  (sut/sum-fn {:predicate odd? :mapper inc}) input)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder #{20})))

        (sut/wait-pipeline-result (sut/run-pipeline p))))))

(deftest mean-fn-test
  (testing "nominal case"
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [1.0 2.0 3.0] p)
          rslt (sut/combine (sut/mean-fn) input)]

      (is (-> (PAssert/that rslt)
              (.containsInAnyOrder #{2.0})))

      (sut/wait-pipeline-result (sut/run-pipeline p))))

  (testing "with options"
    (testing "with pred"
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [1 2 3 4 5 6 7] p)
            rslt (sut/combine (sut/mean-fn {:predicate even?}) input)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder #{4.0})))

        (sut/wait-pipeline-result (sut/run-pipeline p))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [1 2 3 4 5 6 7] p)
            rslt (sut/combine
                  (sut/mean-fn {:predicate even? :mapper #(* % 10)}) input)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder #{40.0})))

        (sut/wait-pipeline-result (sut/run-pipeline p)))))  )

(deftest min-fn-test
  (testing "nominal case"
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [1.0 2.0 3.0] p)
          rslt (sut/combine (sut/min-fn) input)]

      (is (-> (PAssert/that rslt)
              (.containsInAnyOrder #{1.0})))

      (sut/wait-pipeline-result (sut/run-pipeline p))))

  (testing "with options"
    (testing "with pred"
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [1 2 3 4 5 6 7] p)
            rslt (sut/combine (sut/min-fn {:predicate even?}) input)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder #{2})))

        (sut/wait-pipeline-result (sut/run-pipeline p))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [1 2 3 4 5 6 7] p)
            rslt (sut/combine
                  (sut/min-fn {:predicate even? :mapper #(* % 10)}) input)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder #{20})))

        (sut/wait-pipeline-result (sut/run-pipeline p))))))

(deftest max-fn-test
  (testing "nominal case"
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [1.0 2.0 3.0] p)
          rslt (sut/combine (sut/max-fn) input)]

      (is (-> (PAssert/that rslt)
              (.containsInAnyOrder #{3.0})))

      (sut/wait-pipeline-result (sut/run-pipeline p))))

  (testing "with options"
    (testing "with pred"
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [1 2 3 4 5 6 7] p)
            rslt (sut/combine (sut/max-fn {:predicate even?}) input)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder #{6})))

        (sut/wait-pipeline-result (sut/run-pipeline p))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [1 2 3 4 5 6 7] p)
            rslt (sut/combine
                  (sut/max-fn {:predicate even? :mapper #(* % 10)}) input)]

        (is (-> (PAssert/that rslt)
                (.containsInAnyOrder #{60})))

        (sut/wait-pipeline-result (sut/run-pipeline p))))))

(deftest frequencies-fn-test
  (testing "nominal case"
    (tio/with-tempdir [tmp-dir]
      (let [p (sut/make-pipeline [])
            input (sut/generate-input [2 1 2 3 1 2] p)
            rslt (sut/combine (sut/frequencies-fn) input)]

        (sut/write-edn-file (tio/join-path tmp-dir "test")
                            {:num-shards 1} rslt)

        (sut/wait-pipeline-result (sut/run-pipeline p)))

      (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                     (into {}))]
        (is (= {2 3
                1 2
                3 1}
               res)))))

  (testing "with options"
    (testing "with pred"
      (tio/with-tempdir [tmp-dir]
        (let [p (sut/make-pipeline [])
              input (sut/generate-input [2 1 2 3 1 2] p)
              rslt (sut/combine (sut/frequencies-fn {:predicate even?}) input)]

          (sut/write-edn-file (tio/join-path tmp-dir "test")
                              {:num-shards 1} rslt)

          (sut/wait-pipeline-result (sut/run-pipeline p)))

        (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                       (into {}))]
          (is (= {2 3}
                 res)))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (tio/with-tempdir [tmp-dir]
        (let [p (sut/make-pipeline [])
              input (sut/generate-input [{:ranked? true :rank 2} {:rank 3} {:ranked? true :rank 1}] p)
              rslt (sut/combine
                    (sut/frequencies-fn {:predicate :ranked? :mapper #(:rank %)}) input)]

          (sut/write-edn-file (tio/join-path tmp-dir "test")
                              {:num-shards 1} rslt)

          (sut/wait-pipeline-result (sut/run-pipeline p)))

        (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                       (into {}))]
          (is (= {2 1 1 1}
                 res)))))))

(deftest frequencies-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [1 5 2 1 1 3 4 2 5 5] p)
          rslt (sut/frequencies {:name :elem-freq} input)]

      (is (str/starts-with? (.getName rslt) "elem-freq"))

      (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1} rslt)

      (sut/wait-pipeline-result (sut/run-pipeline p)))

    (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                   (into #{}))]
      (is (= #{[1 3] [2 2] [3 1] [4 1] [5 3]}
             res)))))

(deftest with-timestamp-test
  (let [p (sut/make-pipeline [])
        timestamp (time/now)
        input (sut/generate-input [1 2 3 4 5] p)
        _rslt (sut/map #(sut/with-timestamp timestamp %) input)]

    ;; TODO: add test

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest sliding-windows-test
  (let [now (time/now)
        p (sut/make-pipeline [])
        input (->> (sut/generate-input [0. 0.5 2.5 3.] p)
                   (sut/map #(sut/with-timestamp (time/minus now (time/seconds %)) %) {:name :timestamp}))

        _sliding (sut/sliding-windows (time/seconds 2) (time/seconds 1) {:name :sliding} input)]

    ;; TODO: add test

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest fixed-windows-test
  (let [now (time/now)
        p (sut/make-pipeline [])
        input (->> (sut/generate-input [0. 0.5 2.5 3.] p)
                   (sut/map #(sut/with-timestamp (time/minus now (time/seconds %)) %) {:name :timestamp}))

        _window (sut/fixed-windows (time/seconds 2) {:name :fixed} input)]

    ;; TODO: add test

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest session-windows-test
  (let [now (time/now)
        p (sut/make-pipeline [])
        input (->> (sut/generate-input [[:k0 0] [:k1 1] [:k1 2] [:k0 4]] p)
                   (sut/map (fn [[k e]] (sut/with-timestamp (time/minus now (time/seconds e)) [k e])) {:name :timestamp})
                   (sut/with-keys (fn [e] (get e 0))))

        session (->> (sut/session-windows (time/seconds 2) input)
                     (sut/group-by-key)
                     (sut/map (fn [[_ elts]]
                                (reduce + (map (fn [[_k v]] v) elts))) {:name :sum}))]

    (is (.. PAssert (that session)
            (containsInAnyOrder #{0 3 4})))

    (sut/wait-pipeline-result (sut/run-pipeline p))))

(deftest basic-pipeline-test
  (let [p (sut/make-pipeline [])
        input (sut/generate-input [1 2 3 4 5] {:name :main-gen} p)
        proc (sut/map (fn [x] (inc x)) {:name "increment"} input)
        filter (sut/filter even? proc)]

    (is (-> (PAssert/that input)
            (.containsInAnyOrder #{1 2 3 4 5})))
    (is (-> (PAssert/that proc)
            (.containsInAnyOrder #{2 3 4 5 6})))
    (is (-> (PAssert/that filter)
            (.containsInAnyOrder #{2 4 6})))

    (let [status (sut/wait-pipeline-result (sut/run-pipeline p))]
      (is (= :done status)))))

(deftest intra-bundle-parallelization-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [1 2 3 4 5] {:name :main-gen} p)]
      (sut/->> :pipelined input
               (sut/map inc {:name :inc :intra-bundle-parallelization 5})
               (sut/filter even? {:name :even? :intra-bundle-parallelization 5})
               (sut/write-edn-file (tio/join-path tmp-dir "test") {:num-shards 1}))
      (sut/wait-pipeline-result (sut/run-pipeline p)))
    (let [res (->> (tio/read-edns-file (first (tio/list-files tmp-dir)))
                   (into #{}))]
      (is (= #{2 4 6}
             res)))))

(deftest math-and-diamond-test
  (tio/with-tempdir [tmp-dir]
    (let [p (sut/make-pipeline [])
          input (sut/generate-input [1 2 3 4 5] p)
          p1 (sut/combine (sut/mean-fn) {:name :mean} input)
          p2 (sut/combine (sut/max-fn) {:name :max} input)
          p3 (sut/combine (sut/min-fn) {:name :min} input)
          p4 (sut/combine (sut/sum-fn) {:name :input} input)
          all (sut/concat p1 p2 p3 p4)
          ps (sut/sample 2 input)]

      (sut/write-edn-file (tio/join-path tmp-dir "01-test") {:name :output-all :num-shards 1} all)
      (sut/write-edn-file (tio/join-path tmp-dir "02-test") {:name :output-sample :num-shards 1} ps)

      (sut/wait-pipeline-result (sut/run-pipeline p))

      (let [file-list (sort (tio/list-files tmp-dir))]
        (testing "all"
          (let [res (->> (tio/read-edns-file (first file-list))
                         (into #{}))]
            (is (= #{1 3.0 5 15}
                   res))))
        (testing "sample"
          (let [res (->> (tio/read-edns-file (last file-list))
                         (into #{}))]
            (is (= 2
                   (count res)))))))))
