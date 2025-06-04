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
   [datasplash.testing :as dt]
   [datasplash.testing.assert :as assert]
   [tools.io :as tio])
  (:import
   (java.io PushbackReader)
   (java.util.zip GZIPInputStream)
   (org.apache.beam.sdk.values PCollectionView)))

(defn- assert-name
  [obj a-name]
  (is (str/starts-with? (.getName obj) a-name))
  obj)

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

(defn- file-format
  [path]
  (when-let [match (re-find #"\.(edn|json)s?\b" path)]
    (keyword (second match))))

(defn- make-fixture
  "Creates data files and returns their destination filepath.
   Pass a filename with either .edn or .json as an extension to select
   the output format. If it also suffixed with .gz (e.g.,
   'xx.json.gz'), the file will also be compressed."
  [base-path file-path data]
  (let [writer-fn (case (file-format file-path)
                    :json tio/write-jsons-file
                    tio/write-edns-file)
        dest-file-path (tio/join-path base-path file-path)]
    (writer-fn dest-file-path data)
    dest-file-path))

(defn- make-fixture-with-nil
  "Same as `make-fixture` but writes empty lines when it receives nil values."
  [base-path file-path data]
  (let [writer-fn (case (file-format file-path)
                    :json #(charred/write-json-str % :indent-str nil :escape-slash false)
                    prn-str)
        dest-file-path (tio/join-path base-path file-path)]
    (with-open [wrt (io/writer dest-file-path)]
      (doseq [x data]
        (when x
          (.write wrt (writer-fn x)))
        (.newLine wrt)))
    dest-file-path))

(deftest read-edn-file-test
  (tio/with-tempdir [tmp-dir]
    (let [data #{{:id "1" :name "elem 1"}
                 {:id "2" :name "elem 2"}
                 {:id "3" :name "elem 3"}
                 {:id "4" :name "elem 4"}
                 {:id "5" :name "elem 5"}}]

      (testing "nominal case"
        (dt/with-test-pipeline [p (dt/test-pipeline)]
          (-> (sut/read-edn-file (make-fixture tmp-dir "colls.edns" data)
                                 {:name :read-edn} p)
              (assert-name "read-edn")
              (assert/as-iterable)
              (assert/contains-only data)))

        (testing "gz-compressed"
          (dt/with-test-pipeline [p (dt/test-pipeline)]
            (-> (sut/read-edn-file (make-fixture tmp-dir "colls.edns.gz" data)
                                   {:name :read-edn-gz
                                    :compression-type :gzip} p)
                (assert-name "read-edn-gz")
                (assert/as-iterable)
                (assert/contains-only data))))))

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
            fixture-path (make-fixture tmp-dir "colls.edns" data)]

        (dt/with-test-pipeline [p (dt/test-pipeline)]
          (-> (sut/read-edn-file fixture-path
                                 {:name :read-edn} p)
              (assert/as-iterable)
              (assert/contains-only data)))))))

(deftest read-edn-files-test
  (tio/with-tempdir [tmp-dir]
    (let [data #{{:id "1" :name "elem 1"}
                 {:id "2" :name "elem 2"}
                 {:id "3" :name "elem 3"}
                 {:id "4" :name "elem 4"}
                 {:id "5" :name "elem 5"}
                 {:id "6" :name "elem 6"}}
          [fix-1 fix-2] (split-at 4 data)
          file-list [(make-fixture tmp-dir "01.edns" fix-1)
                     (make-fixture tmp-dir "02.edns" fix-2)]]

      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/read-edn-files (dt/generate file-list p))
            (assert/as-iterable)
            (assert/contains-only data))))))

(deftest write-edn-file-test
  (testing "nominal case"
    (tio/with-tempdir [tmp-dir]

      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (->> (dt/generate [1 2 3] p)
             (sut/write-edn-file (tio/join-path tmp-dir "test")
                                 {:num-shards 1})))

      (let [res (set (read-edn-file (first (tio/list-files tmp-dir))))]
        (is (= #{1 2 3} res)))))

  (testing "gz-compression"
    (tio/with-tempdir [tmp-dir]

      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (->> (dt/generate [1 2 3] p)
             (sut/write-edn-file (tio/join-path tmp-dir "test")
                                 {:num-shards 1
                                  :compression-type :gzip})))

      (let [file-path (first (tio/list-files tmp-dir))
            res (set (read-edn-file {:compressed? true} file-path))]
        (is (str/ends-with? file-path ".gz"))
        (is (= #{1 2 3} res))))))

(deftest read-json-file-test
  (tio/with-tempdir [tmp-dir]
    (testing "nominal case"
      (let [data [1.0 2.0 3.0]
            fixture-path (make-fixture tmp-dir "colls.jsons" data)]

        (dt/with-test-pipeline [p (dt/test-pipeline)]
          (-> (sut/read-json-file fixture-path
                                  {:name :read-json} p)
              (assert-name "read-json")
              (assert/as-iterable)
              (assert/contains-only data)))))

    (testing "with key-fn"
      (let [data [{:id "1" :name "elem 1"}
                  {:id "2" :name "elem 2"}
                  {:id "3" :name "elem 3"}]
            fixture-path (make-fixture tmp-dir "colls.jsons" data)]

        (dt/with-test-pipeline [p (dt/test-pipeline)]
          (-> (sut/read-json-file fixture-path
                                  {:name :read-json-k
                                   :key-fn keyword} p)
              (assert-name "read-json-k")
              (assert/as-iterable)
              (assert/contains-only data)))))

    (testing "nested map w date fields"
      (let [data [{:id "3" :name "elem 3"
                   :created {:date "2017-01-01T00:00:00.000-00:00"}}
                  {:id "1" :name "elem 1"
                   :created {:date "2005-02-01T00:00:00Z"}}]
            fixture-path (make-fixture tmp-dir "colls.jsons" data)]

        (dt/with-test-pipeline [p (dt/test-pipeline)]
          (-> (sut/read-json-file fixture-path
                                  {:name :read-json-nested
                                   :key-fn keyword} p)
              (assert/as-iterable)
              (assert/contains-only data)))))

    (testing "testing empty line"
      (let [data [{:id "3" :name "elem 3"
                   :created {:date "2017-01-01T00:00:00.000-00:00"}}
                  nil
                  {:id "1" :name "elem 1"
                   :created {:date "2005-02-01T00:00:00Z"}}]
            fixture-path (make-fixture-with-nil tmp-dir "colls.jsons" data)]

        (dt/with-test-pipeline [p (dt/test-pipeline)]
          (-> (sut/read-json-file fixture-path
                                  {:name :read-json-nested
                                   :key-fn keyword} p)
              (assert/as-iterable)
              (assert/contains-only data)))))

    (testing "gz-compressed"
      (let [data [1.0 2.0 3.0]
            fixture-path (make-fixture tmp-dir "colls.jsons.gz" data)]

        (dt/with-test-pipeline [p (dt/test-pipeline)]
          (-> (sut/read-json-file fixture-path
                                  {:name :read-json-gz
                                   :compression-type :gzip} p)
              (assert-name "read-json-gz")
              (assert/as-iterable)
              (assert/contains-only data)))))))

(deftest read-json-files-test
  (testing "with key-fn"
    (tio/with-tempdir [tmp-dir]
      (let [data [{:id "1" :name "elem 1"}
                  {:id "2" :name "elem 2"}
                  {:id "3" :name "elem 3"}
                  {:id "4" :name "elem 4"}
                  {:id "5" :name "elem 5"}
                  {:id "6" :name "elem 6"}]
            [fix-1 fix-2] (split-at 4 data)
            file-list [(make-fixture tmp-dir "01.jsons" fix-1)
                       (make-fixture tmp-dir "02.jsons" fix-2)]]

        (dt/with-test-pipeline [p (dt/test-pipeline)]
          (-> (sut/read-json-files {:key-fn keyword}
                                   (dt/generate file-list p))
              (assert/as-iterable)
              (assert/contains-only data)))))))

(deftest write-json-file-test
  (testing "nominal case"
    (tio/with-tempdir [tmp-dir]
      (let [values [{:id "23" :score 312 :vals #{{:id "41" :ratio 1.2 :view-ident [:a :b :c]} {:id "52" :view-ident '(:myns/bravo)}}}
                    {:id "15" :score 4 :vals #{{:id "44" :ratio 4 :view-ident [:x :y :z]} {:id "492" :view-ident '(:a :c)}}}]]

        (dt/with-test-pipeline [p (dt/test-pipeline)]
          (sut/write-json-file (tio/join-path tmp-dir "test")
                               {:num-shards 1}
                               (dt/generate values p)))

        (let [res (->> (first (tio/list-files tmp-dir))
                       slurp
                       str/split-lines
                       set)]
          (is (= #{"{\"id\":\"15\",\"score\":4,\"vals\":[{\"id\":\"44\",\"ratio\":4,\"view-ident\":[\"x\",\"y\",\"z\"]},{\"id\":\"492\",\"view-ident\":[\"a\",\"c\"]}]}"
                   "{\"id\":\"23\",\"score\":312,\"vals\":[{\"id\":\"41\",\"ratio\":1.2,\"view-ident\":[\"a\",\"b\",\"c\"]},{\"id\":\"52\",\"view-ident\":[\"myns/bravo\"]}]}"}
                 res))))))

  (testing "gz-compression"
    (tio/with-tempdir [tmp-dir]
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (sut/write-edn-file (tio/join-path tmp-dir "test")
                            {:num-shards 1
                             :compression-type :gzip}
                            (dt/generate [1 2 3] p)))

      (let [file-path (first (tio/list-files tmp-dir))
            res (->> file-path
                     (io/input-stream)
                     (GZIPInputStream.)
                     slurp
                     str/split-lines
                     set)]
        (is (str/ends-with? file-path ".gz"))
        (is (= #{"1" "2" "3"} res))))))

(deftest generate-input-test
  (testing "nominal case"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/generate-input [1 2 3] {:name :test-input} p)
          (assert-name "test-input")
          (assert/as-iterable)
          (assert/contains-only [1 2 3]))))

  (testing "map coll"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [values [{:id "1" :label "s"}
                    {:id "2" :label "cc"}]]
        (-> (sut/generate-input values p)
            (assert/as-iterable)
            (assert/contains-only values))))

    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [x {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8}
            y {:a 1 :b 2 :c 3 :d 4 :e 5 :f 6 :g 7 :h 8 :i 9 :j 10}
            values [x y]]

        (is (not= (.getClass x)  ; x => clojure.lang.PersistentArrayMap
                  (.getClass y)) ; y => clojure.lang.PersistentHashMap
            "underlying representation differs")

        ;; test fix for java.lang.IllegalArgumentException due to
        ;; generate-input not using a coder when a collection was
        ;; passed. This throws an exception in cases where the two
        ;; underlying types were different. See
        ;; https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/PersistentArrayMap.java
        (-> (sut/generate-input values p)
            (assert/as-iterable)
            (assert/contains-only values)))))

  (testing "booleans"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/generate-input [true false] p)
          (assert/as-iterable)
          (assert/contains-only [true false]))))

  ;; nippy fix, by @RolT. see #101
  (testing "map coll w/ boolean values"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [values [{:done true} {:done false}]]
        (-> (sut/generate-input values p)
            (assert/as-iterable)
            (assert/contains-only values))))))

(deftest map-test
  (testing "with initialize-fn"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [values [{:a 1} {:b 2} {:c 3}]]

        (-> (sut/map (fn [x] (merge x (sut/system)))
                     {:name :map-w-initialize-fn
                      :initialize-fn (fn [] {:init 10})}
                     (dt/generate values p))
            (assert-name "map-w-initialize-fn")
            (assert/as-iterable)
            (assert/contains-only [{:a 1 :init 10}
                                   {:b 2 :init 10}
                                   {:c 3 :init 10}])))))

  (testing "stateful"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [kvs (mapv sut/make-kv [[:a 1] [:a 1] [:b 2] [:b 2]])
            input (dt/generate kvs {:coder (sut/make-kv-coder)} p)
            rslt (sut/map (fn [[k v]]
                            (let [state (sut/state)
                                  current (or (.read state) 0)]
                              (.write state v)
                              (sut/make-kv k (+ v current))))
                          {:stateful? true}
                          input)]

        ;; TODO: check state
        (-> (assert/as-iterable rslt)
            (assert/contains-only (mapv sut/make-kv
                                        [[:a 2] [:b 2] [:a 1] [:b 4]]))))))

  (testing "checkpoint"
    (tio/with-tempdir [tmp-dir]

      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (->> (dt/generate [0 1 2 3] p)
                 (sut/map inc {:name :inc
                               :checkpoint (tio/join-path tmp-dir
                                                          "02-checkpoint")})
                 (sut/map inc {:name :inc-again}))
            (assert/as-iterable)
            (assert/contains-only [2 3 4 5])))

      (let [file-list (sort (tio/list-files tmp-dir))
            checkpoint-files (filter #(str/includes? % "checkpoint") file-list)
            checkpoint-rslt (->> checkpoint-files
                                 tio/read-edns-files
                                 set)]
        (is (>= (count checkpoint-files) 1)
            "checkpoint file(s) generated")

        (is (= #{1 2 3 4} checkpoint-rslt)
            "checkpoint values valid")))))

(deftest map-kv-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [values [{:k 1 :v 2} {:k 3 :v 4} {:k 5 :v 6}]]
      (-> (sut/map-kv #(vector (:k %) (:v %))
                      {:name :map-vals}
                      (dt/generate values p))
          (assert-name "map-vals")
          (assert/as-map)
          (assert/equals-to {1 2, 3 4, 5 6})))))

(deftest mapcat-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [values [{:values [1 2]} {:values [3 4]} {:values [5 6]}]]
      (-> (sut/mapcat :values
                      {:name :mapcat-vals}
                      (dt/generate values p))
          (assert-name "mapcat-vals")
          (assert/as-iterable)
          (assert/contains-only [1 2 3 4 5 6])))))

(deftest filter-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (-> (sut/filter even?
                    {:name :filter-even}
                    (dt/generate [1 2 3 4 5] p))
        (assert-name "filter-even")
        (assert/as-iterable)
        (assert/contains-only [2 4]))))

(deftest keep-test
  (testing "it keeps all elements"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [values [false true false]]
        (-> (sut/keep identity {:name :keep-test1}
                      (dt/generate values p))
            (assert-name "keep-test1")
            (assert/as-iterable)
            (assert/contains-only values)))))

  (testing "it discards all elements"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/keep parse-long {:name :keep-test2}
                    (dt/generate ["a" "b" "c"] p))
          (assert-name "keep-test2")
          (assert/as-iterable)
          (assert/is-empty))))

  (testing "it keeps some elements"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/keep parse-long {:name :keep-test3}
                    (dt/generate ["a" "1" "b"] p))
          (assert-name "keep-test3")
          (assert/as-iterable)
          (assert/contains-only [1])))))

(deftest with-keys-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [values [{:id "1" :rank 4}
                  {:id "2" :rank 3}
                  {:id "3" :rank 4}
                  {:id "4" :rank 1}]
          expected (map #(sut/make-kv (:rank %) %) values)]

      (-> (sut/with-keys :rank {:name :w-k} (dt/generate values p))
          (assert-name "w-k")
          (assert/as-iterable)
          (assert/contains-only expected)))))

(deftest group-by-key-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [input (dt/generate [{:id "1" :rank 4 :name "no 1"}
                              {:id "2" :rank 3 :name "no 2"}
                              {:id "3" :rank 4 :name "no 3"}
                              {:id "4" :rank 1 :name "no 4"}] p)]

      (-> (sut/group-by-key {:name :group-by-rank}
                            (sut/with-keys :rank input))
          (assert-name "group-by-rank")
          (assert/as-map)
          (assert/satisfies (fn [m]
                              (= (update-vals m set)
                                 {1 #{{:id "4" :rank 1 :name "no 4"}}
                                  3 #{{:id "2" :rank 3 :name "no 2"}}
                                  4 #{{:id "1" :rank 4 :name "no 1"}
                                      {:id "3" :rank 4 :name "no 3"}}})))))))

(deftest ->>-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (-> (sut/->> :pipelined
                 (dt/generate [1 2 3 4 5] p)
                 (sut/map inc {:name :inc})
                 (sut/filter even? {:name :even?}))
        (assert/as-iterable)
        (assert/contains-only [2 4 6]))))

(deftest cond->>-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (-> (sut/cond->> :pipelined
          (dt/generate [1 2 3 4 5] p)
          true (sut/map inc {:name :inc})
          false (sut/filter even? {:name :even?}))
        (assert/as-iterable)
        (assert/contains-only [2 3 4 5 6]))))

(deftest partition-by-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [input (dt/generate [1 2 3 4 5 6 7 8 9] p)
          pcolls (sut/partition-by (fn [e _] (if (odd? e) 1 0))
                                   2
                                   {:name :partition}
                                   input)
          [even-coll odd-coll] (.getAll pcolls)]

      (-> (assert/as-iterable even-coll)
          (assert/contains-only [2 4 6 8]))
      (-> (assert/as-iterable odd-coll)
          (assert/contains-only [1 3 5 7 9])))))

(deftest view-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [a-view (->> (dt/generate ["a" "b" "c"] p)
                      (sut/combine (sut/combine-fn
                                    conj
                                    identity
                                    (fn [& accs] (apply set/union accs))
                                    (fn [] #{}))
                                   {:name :as-set})
                      (sut/view {:name :si}))
          output (->> (dt/generate [{:id "a"} {:id "d"} {:id "c"}] p)
                      (sut/filter (fn [{:keys [id]}]
                                    (get-in (sut/side-inputs) [:mapping id]))
                                  {:side-inputs {:mapping a-view}}))]

      (is (instance? PCollectionView a-view))

      (-> (assert/as-iterable output)
          (assert/contains-only [{:id "a"} {:id "c"}])))))

(deftest side-inputs-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [side-input (-> [{1 :a 2 :b 3 :c 4 :d 5 :e}]
                         (dt/generate p)
                         sut/view)]

      (-> (sut/map (fn [x] (get-in (sut/side-inputs) [:mapping x]))
                   {:side-inputs {:mapping side-input}}
                   (dt/generate [1 2 3 4 5] p))
          (assert/as-iterable)
          (assert/contains-only [:a :b :c :d :e])))))

(deftest side-outputs-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [{:keys [simple multi]}
          (sut/map (fn [x] (sut/side-outputs :simple x
                                             :multi (* x 10)))
                   {:side-outputs [:simple :multi]}
                   (dt/generate [1 2 3 4 5] p))]

      (-> (assert/as-iterable simple)
          (assert/contains-only [1 2 3 4 5]))

      (-> (assert/as-iterable multi)
          (assert/contains-only [10 20 30 40 50])))))

(deftest group-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [input (dt/generate [{:key :a :val 42}
                              {:key :b :val 56}
                              {:key :a :lue 65}] p)]

      (-> (sut/group-by :key {:name "group"} input)
          (assert-name "group")
          (assert/as-map)
          (assert/satisfies (fn [m]
                              (= (update-vals m set)
                                 {:a #{{:key :a :lue 65} {:key :a :val 42}}
                                  :b #{{:key :b :val 56}}})))))))

(deftest cogroup-test
  (testing "nominal case"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [input1 (dt/generate [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] p)
            input2 (dt/generate [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] p)]

        (-> (sut/cogroup-by {:name "cogroup-test"}
                            [[input1 :key] [input2 :key]])
            (assert-name "cogroup-test")
            (assert/as-map)
            (assert/satisfies
             (fn [m]
               (= (update-vals m #(mapv set %))
                  {:a [#{{:key :a :lue 65} {:key :a :val 42}} #{{:key :a :uel 65} {:key :a :lav 42}}]
                   :c [#{} #{{:key :c :foo 42}}]
                   :b [#{{:key :b :val 56}} #{}]})))))))

  (testing "large set of pcollection"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [nb-pcolls 101
            ;; pcolls is something like
            ;; [ [{:i 0 :key 0} {:i 1 :key 0} ...] 🠐 this is a pcoll
            ;;   [{:i 0 :key 1} {:i 1 :key 1} ...]
            ;;   ... ]
            pcoll-ids (range nb-pcolls)
            pcolls (mapv (fn [k-pcoll]
                           (dt/generate
                            (mapv (fn [i] {:i i :key k-pcoll}) (range 5)) p))
                         pcoll-ids)]

        (-> (sut/cogroup-by {:name "join-fitments"
                             :collector (fn [[_id same-i]] same-i)}
                            (mapv #(vector % :i) pcolls))
            (assert-name "join-fitments")
            (assert/as-iterable)
            (assert/satisfies
             (fn [cogroups]
               (every? (fn [cogroup]
                         (let [same-i (mapcat identity cogroup)]
                           (and (= 1 (count (distinct (mapv :i same-i))))
                                (= (mapv :key same-i) pcoll-ids))))
                       cogroups)))))))

  (testing "drop-nil"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [input1 (dt/generate [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] p)
            input2 (dt/generate [{:key :a :lav 42} {:uel 65} {:key :c :foo 42}] p)]

        (-> (sut/cogroup-by {:name "cogroup-drop-nil-test"}
                            [[input1 :key] [input2 :key {:drop-nil? true}]])
            (assert-name "cogroup-drop-nil-test")
            (assert/as-map)
            (assert/satisfies
             (fn [m]
               (= (update-vals m #(mapv set %))
                  {:a [#{{:key :a :lue 65} {:key :a :val 42}} #{{:key :a :lav 42}}]
                   :c [#{} #{{:key :c :foo 42}}]
                   :b [#{{:key :b :val 56}} #{}]})))))))

  (testing "required"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [input1 (dt/generate [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] p)
            input2 (dt/generate [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] p)]

        (-> (sut/cogroup-by {:name "cogroup-required-test"}
                            [[input1 :key {:type :required}] [input2 :key]])
            (assert-name "cogroup-required-test")
            (assert/as-map)
            (assert/satisfies
             (fn [m]
               (= (update-vals m #(mapv set %))
                  {:a [#{{:key :a :lue 65} {:key :a :val 42}} #{{:key :a :uel 65} {:key :a :lav 42}}]
                   :b [#{{:key :b :val 56}} #{}]})))))))

  (testing "join-nil"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [input1 (dt/generate [{:key :a :val 42} {:val 56} {:key :a :lue 65}] p)
            input2 (dt/generate [{:key :a :lav 42} {:uel 65} {:key :c :foo 42}] p)]

        (-> (sut/cogroup-by {:name "cogroup-join-nil-test"
                             ;; convert KV obj to MapEntry
                             :collector identity}
                            [[input1 :key] [input2 :key]])
            (assert-name "cogroup-join-nil-test")
            (assert/as-iterable)
            (assert/satisfies
             (fn [cogroups]
               (= (set (mapv (fn [[k values]]
                               [k (mapv set values)])
                             cogroups))
                  #{[:a [#{{:key :a :lue 65} {:key :a :val 42}} #{{:key :a :lav 42}}]]
                    [nil [#{{:val 56}} #{}]]
                    [nil [#{} #{{:uel 65}}]]
                    [:c [#{} #{{:key :c :foo 42}}]]}))))))))

(deftest join-by-test
  (testing "nomical case"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [input1 (dt/generate [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] p)
            input2 (dt/generate [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] p)]

        (-> (sut/join-by {:name "join-test"}
                         [[input1 :key] [input2 :key]]
                         merge)
            (assert-name "join-test")
            (assert/as-iterable)
            (assert/contains-only [{:key :b :val 56} {:key :c :foo 42} {:key :a :lue 65 :lav 42} {:key :a :val 42 :lav 42}
                                   {:key :a :val 42 :uel 65} {:key :a :lue 65 :uel 65}])))))

  (testing "required"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [input1 (dt/generate [{:key :a :val 42} {:key :b :val 56} {:key :a :lue 65}] p)
            input2 (dt/generate [{:key :a :lav 42} {:key :a :uel 65} {:key :c :foo 42}] p)]

        (-> (sut/join-by {:name "join-test-required" :collector merge}
                         [[input1 :key]
                          [input2 :key {:type :required}]]
                         merge)
            (assert-name "join-test-required")
            (assert/as-iterable)
            (assert/contains-only [{:key :c :foo 42} {:key :a :lue 65 :lav 42} {:key :a :val 42 :lav 42}
                                   {:key :a :val 42 :uel 65} {:key :a :lue 65 :uel 65}]))))))

(deftest distinct-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [values [1 2 3 2 4 1 5]]
      (-> (sut/distinct {:name :unique}
                        (dt/generate values p))
          (assert-name "unique")
          (assert/as-iterable)
          (assert/contains-only (distinct values))))))

(deftest distinct-by-test
  (testing "it returns distinct element using a representative value."
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (let [values [["b" 1] ["a" 2] ["b" 3]]]
        (-> (sut/distinct-by first
                             {:name "distinct-by-first"}
                             (dt/generate values p))
            (assert-name "distinct-by-first")
            (assert/as-iterable)
            (assert/satisfies
             (fn [coll]
               (and (= #{"a" "b"} (set (mapv first coll)))
                    (every? (set values) coll)))))))))

(deftest sample-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [data #{1 2 3 4 5 6 7}]
      (-> (sut/sample 3 (dt/generate data p))
          (assert/as-iterable)
          (assert/satisfies #(and (= 3 (count %))
                                  (set/subset? (set %) data)))))))

(deftest concat-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [vals1 [1 2 3] vals2 [4 5 6]]
      (-> (sut/concat {:name :concat-pcolls}
                      (dt/generate vals1 p)
                      (dt/generate vals2 p))
          (assert-name "concat-pcolls")
          (assert/as-iterable)
          (assert/contains-only (concat vals1 vals2))))))

(deftest combine-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [values [1 2 3 4 5]]
      (-> (sut/combine +
                       {:name "combine" :scope :global}
                       (dt/generate values p))
          (assert-name "combine")
          (assert/as-singleton)
          (assert/equals-to (apply + values))))))

(defn- test-combine-fn
  [combine-fn]
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [values [{:a 1} {:b 2} {:c 3} {:d 4} {:e 5}]]
      (-> (sut/combine combine-fn
                       {:name "combine" :scope :global}
                       (dt/generate values p))
          (assert-name "combine")
          (assert/as-singleton)
          (assert/equals-to (apply merge values))))))

(deftest combine-fn-test
  (let [reducef merge
        extractf identity
        combinef (fn [& accs] (apply merge accs))
        initf (constantly {})]

    (testing "discrete args"
      (test-combine-fn (sut/combine-fn reducef extractf combinef initf)))

    (testing "args map"
      (doseq [m [{:reduce reducef :extract extractf :combine combinef :init initf}
                 {:reduce reducef                   :combine combinef :init initf}
                 {:reduce merge                                       :init initf}
                 {:reduce merge}]]
        (test-combine-fn (sut/combine-fn m))))))

(deftest juxt-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (-> (sut/combine (sut/juxt +
                               *
                               (sut/sum-fn)
                               (sut/mean-fn)
                               (sut/max-fn)
                               (sut/min-fn)
                               (sut/count-fn)
                               (sut/count-fn :predicate even?)
                               (sut/max-fn :mapper #(* 10 %)))
                     {:name "combine-juxt"}
                     (dt/generate [1 2 3 4 5] p))
        (assert-name "combine-juxt")
        (assert/as-singleton)
        (assert/equals-to [15 120 15 3.0 5 1 5 2 50]))))

(deftest count-fn-test
  (testing "nominal case"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/combine (sut/count-fn)
                       (dt/generate [10 30 60] p))
          (assert/as-singleton)
          (assert/equals-to 3))))

  (testing "with options"
    (testing "with pred"
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/count-fn {:predicate even?})
                         (dt/generate [1 2 3 4 5 6] p))
            (assert/as-singleton)
            (assert/equals-to 3))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/count-fn {:predicate :rank
                                        :mapper #(count (:ids %))})
                         (dt/generate [{:ids #{1 2 3} :rank 2}
                                       {:ids #{4}}
                                       {:ids #{5} :rank 1}] p))
            (assert/as-singleton)
            (assert/equals-to 4))))))

(deftest sum-fn-test
  (testing "nominal case"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/combine (sut/sum-fn)
                       (dt/generate [10 20 30] p))
          (assert/as-singleton)
          (assert/equals-to 60))))

  (testing "with options"
    (testing "with pred"
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/sum-fn {:predicate odd?})
                         (dt/generate [1 2 3 4 5 6 7] p))
            (assert/as-singleton)
            (assert/equals-to 16))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/sum-fn {:predicate odd? :mapper inc})
                         (dt/generate [1 2 3 4 5 6 7] p))
            (assert/as-singleton)
            (assert/equals-to 20))))))

(deftest mean-fn-test
  (testing "nominal case"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/combine (sut/mean-fn)
                       (dt/generate [1.0 2.0 3.0] p))
          (assert/as-singleton)
          (assert/equals-to 2.0))))

  (testing "with options"
    (testing "with pred"
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/mean-fn {:predicate even?})
                         (dt/generate [1 2 3 4 5 6 7] p))
            (assert/as-singleton)
            (assert/equals-to 4.0))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/mean-fn {:predicate even? :mapper #(* % 10)})
                         (dt/generate [1 2 3 4 5 6 7] p))
            (assert/as-singleton)
            (assert/equals-to 40.0))))))

(deftest min-fn-test
  (testing "nominal case"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/combine (sut/min-fn)
                       (dt/generate [1.0 2.0 3.0] p))
          (assert/as-singleton)
          (assert/equals-to 1.0))))

  (testing "with options"
    (testing "with pred"
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/min-fn {:predicate even?})
                         (dt/generate [1 2 3 4 5 6 7] p))
            (assert/as-singleton)
            (assert/equals-to 2))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/min-fn {:predicate even? :mapper #(* % 10)})
                         (dt/generate [1 2 3 4 5 6 7] p))
            (assert/as-singleton)
            (assert/equals-to 20))))))

(deftest max-fn-test
  (testing "nominal case"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/combine (sut/max-fn)
                       (dt/generate [1.0 2.0 3.0] p))
          (assert/as-singleton)
          (assert/equals-to 3.0))))

  (testing "with options"
    (testing "with pred"
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/max-fn {:predicate even?})
                         (dt/generate [1 2 3 4 5 6 7] p))
            (assert/as-singleton)
            (assert/equals-to 6))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/max-fn {:predicate even? :mapper #(* % 10)})
                         (dt/generate [1 2 3 4 5 6 7] p))
            (assert/as-singleton)
            (assert/equals-to 60))))))

(deftest frequencies-fn-test
  (testing "nominal case"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/combine (sut/frequencies-fn)
                       (dt/generate [2 1 2 3 1 2] p))
          (assert/as-singleton)
          (assert/equals-to {2 3
                             1 2
                             3 1}))))

  (testing "with options"
    (testing "with pred"
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/frequencies-fn {:predicate even?})
                         (dt/generate [2 1 2 3 1 2] p))
            (assert/as-singleton)
            (assert/equals-to {2 3}))))

    (testing "with pred & mapper"
      ;; NOTE: mapper without a predicate is ignored
      (dt/with-test-pipeline [p (dt/test-pipeline)]
        (-> (sut/combine (sut/frequencies-fn {:predicate :ranked?
                                              :mapper :rank})
                         (dt/generate [{:ranked? true :rank 2}
                                       {:rank 3}
                                       {:ranked? true :rank 1}] p))
            (assert/as-singleton)
            (assert/equals-to {2 1 1 1}))))))

(deftest frequencies-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [data [1 5 2 1 1 3 4 2 5 5]]
      (-> (sut/frequencies {:name :elem-freq}
                           (dt/generate data p))
          (assert-name "elem-freq")
          (assert/as-map)
          (assert/equals-to (frequencies data))))))

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
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [now (time/now)
          input (->> (dt/generate [[:k0 0] [:k1 1] [:k1 2] [:k0 4]] p)
                     (sut/map (fn [[k e]] (sut/with-timestamp (time/minus now (time/seconds e)) [k e])) {:name :timestamp})
                     (sut/with-keys (fn [e] (get e 0))))

          session (->> (sut/session-windows (time/seconds 2) input)
                       (sut/group-by-key)
                       (sut/map (fn [[_ elts]]
                                  (reduce + (map (fn [[_k v]] v) elts))) {:name :sum}))]

      (-> (assert/as-iterable session)
          (assert/contains-only [0 3 4])))))

(deftest basic-pipeline-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [input (dt/generate [1 2 3 4 5] p)
          mapped (sut/map inc {:name :map-inc} input)
          filtered (sut/filter even? mapped)]

      (-> (assert/as-iterable input) (assert/contains-only [1 2 3 4 5]))
      (-> (assert/as-iterable mapped) (assert/contains-only [2 3 4 5 6]))
      (-> (assert/as-iterable filtered) (assert/contains-only [2 4 6])))))

(deftest intra-bundle-parallelization-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (-> (sut/->> :pipelined
                 (dt/generate [1 2 3 4 5] p)
                 (sut/map inc {:name :inc
                               :intra-bundle-parallelization 5})
                 (sut/filter even? {:name :even?
                                    :intra-bundle-parallelization 5}))
        (assert/as-iterable)
        (assert/contains-only [2 4 6]))))

(deftest math-and-diamond-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [input (dt/generate [1 2 3 4 5] p)
          p1 (sut/combine (sut/mean-fn) {:name :mean} input)
          p2 (sut/combine (sut/max-fn) {:name :max} input)
          p3 (sut/combine (sut/min-fn) {:name :min} input)
          p4 (sut/combine (sut/sum-fn) {:name :input} input)]

      (-> (sut/sample 2 input)
          (assert/as-iterable)
          (assert/satisfies #(= 2 (count %))))

      (-> (sut/concat p1 p2 p3 p4)
          (assert/as-iterable)
          (assert/contains-only [1 3.0 5 15])))))

(deftest intersect-distinct-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [input-1 (dt/generate [1 2 2 3 4 4 5] p)
          input-2 (dt/generate [2 3 3 5 6 6 7] p)]

      (-> (sut/intersect-distinct {:name :intersect-pcolls} input-1 input-2)
          (assert/as-iterable)
          (assert/contains-only [2 3 5])))))

(deftest union-distinct-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [input-1 (dt/generate [1 2 2 3 4 4 5] p)
          input-2 (dt/generate [2 3 3 5 6 6 7] p)]

      (-> (sut/union-distinct {:name :union-pcolls} input-1 input-2)
          (assert/as-iterable)
          (assert/contains-only [1 2 3 4 5 6 7])))))

(deftest except-distinct-test
  (dt/with-test-pipeline [p (dt/test-pipeline)]
    (let [input-1 (dt/generate [1 2 2 3 4 4 5] p)
          input-2 (dt/generate [2 3 3 5 6 6 7] p)]

      (-> (sut/except-distinct {:name :except-pcolls} input-1 input-2)
          (assert/as-iterable)
          (assert/contains-only [1 4])))))
