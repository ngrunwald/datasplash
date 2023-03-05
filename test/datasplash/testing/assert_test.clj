(ns datasplash.testing.assert-test
  (:require
   [clojure.set :as set]
   [clojure.test :refer [deftest is testing]]
   [datasplash.core :as ds]
   [datasplash.testing :as dt]
   [datasplash.testing.assert :as sut])
  (:import
   (org.apache.beam.sdk.coders IterableCoder)
   (org.apache.beam.sdk.values PCollectionList)))

(set! *warn-on-reflection* true)

(def ^:private iterable-coder
  (IterableCoder/of (ds/make-nippy-coder)))

(defn- pcoll-list
  [colls p]
  (PCollectionList/of ^Iterable (mapv #(dt/generate % p) colls)))

(defn- kv-pcoll
  [kvs p]
  (ds/map-kv #(vector (first %) (second %))
             {:name (str "kv-pcoll-map-" (System/nanoTime))}
             (dt/generate kvs p)))

(deftest as-iterable-test
  (testing "Assert iterable equals"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (dt/generate [:a :b :c] p)
          (sut/as-iterable)
          (sut/contains-only [:a :b :c]))

      (-> (dt/generate [[:a :b :c]] {:coder iterable-coder} p)
          (sut/as-singleton-iterable)
          (sut/contains-only [:a :b :c]))

      (-> (pcoll-list [[:a :b] [:c]] p)
          (sut/as-flattened)
          (sut/contains-only [:a :b :c]))))

  (testing "Assert iterable equals failure!"
    (is (thrown? AssertionError
                 (dt/with-test-pipeline [p (dt/test-pipeline)]
                   (-> (sut/as-iterable (dt/generate [1 2] p))
                       (sut/contains-only [:ko]))))))

  (testing "Assert iterable is empty"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (dt/generate p)
          (sut/as-iterable)
          (sut/is-empty))

      (-> (dt/generate [[]] {:coder iterable-coder} p)
          (sut/as-singleton-iterable)
          (sut/is-empty))

      (-> (pcoll-list [[]] p)
          (sut/as-flattened)
          (sut/is-empty))))

  (testing "Assert iterable is empty failure!"
    (is (thrown? AssertionError
                 (dt/with-test-pipeline [p (dt/test-pipeline)]
                   (-> (sut/as-iterable (dt/generate [1 2] p))
                       (sut/is-empty))))))

  (testing "Assert iterable satisfies"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (dt/generate [:a :b :c] p)
          (sut/as-iterable)
          (sut/satisfies #(set/superset? (set %) #{:a :b})))

      (-> (dt/generate [[:a :b :c]] {:coder iterable-coder} p)
          (sut/as-singleton-iterable)
          (sut/satisfies #(set/superset? (set %) #{:a :b})))

      (-> (pcoll-list [[:a :b] [:c]] p)
          (sut/as-flattened)
          (sut/satisfies #(set/superset? (set %) #{:a :b})))))

  (testing "Assert iterable satisfies failure!"
    (is (thrown? AssertionError
                 (dt/with-test-pipeline [p (dt/test-pipeline)]
                   (-> (sut/as-iterable (dt/generate p))
                       (sut/satisfies seq))))))

  (testing "Assert iterable matchers composition"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/as-iterable (dt/generate p))
          (sut/is-empty)
          (sut/contains-only [])
          (sut/satisfies empty?)))))

(deftest as-singleton-test
  (testing "Assert singleton equals"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (dt/generate [[1 2 3]] p)
          (sut/as-singleton)
          (sut/equals-to [1 2 3]))

      (-> (kv-pcoll [[:a 1] [:b 2]] p)
          (sut/as-map)
          (sut/equals-to {:a 1 :b 2}))

      (-> (kv-pcoll [[:a 1] [:b 2]] p)
          (sut/as-multimap)
          (sut/equals-to {:a [1] :b [2]}))))

  (testing "Assert singleton equals failure!"
    (is (thrown? AssertionError
                 (dt/with-test-pipeline [p (dt/test-pipeline)]
                   (-> (dt/generate [42] p)
                       (sut/as-singleton)
                       (sut/equals-to 77))))))

  (testing "Assert singleton not equals"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (dt/generate [[1 2 3]] p)
          (sut/as-singleton)
          (sut/not-equals-to [2 3]))

      (-> (kv-pcoll [[:a 1] [:b 2]] p)
          (sut/as-map)
          (sut/not-equals-to {:a 2 :b 2}))

      (-> (kv-pcoll [[:a 1] [:b 2] [:a 3]] p)
          (sut/as-multimap)
          (sut/not-equals-to {:a [1] :b [2]}))))

  (testing "Assert singleton not equals failure!"
    (is (thrown? AssertionError
                 (dt/with-test-pipeline [p (dt/test-pipeline)]
                   (-> (dt/generate [42] p)
                       (sut/as-singleton)
                       (sut/not-equals-to 42))))))

  (testing "Assert singleton satisfies"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (dt/generate [[1 2 3]] p)
          (sut/as-singleton)
          (sut/satisfies #(= 3 (count %))))

      (-> (kv-pcoll [[:a 1] [:b 2]] p)
          (sut/as-map)
          (sut/satisfies #(even? (:b %))))

      (-> (kv-pcoll [[:a 1] [:b 2] [:a 3]] p)
          (sut/as-multimap)
          (sut/satisfies #(= #{1 3} (set (:a %)))))))

  (testing "Assert singleton satisfies failure!"
    (is (thrown? AssertionError
                 (dt/with-test-pipeline [p (dt/test-pipeline)]
                   (-> (dt/generate [42] p)
                       (sut/as-singleton)
                       (sut/satisfies odd?))))))

  (testing "Assert singleton matchers composition"
    (dt/with-test-pipeline [p (dt/test-pipeline)]
      (-> (sut/as-singleton (dt/generate [42] p))
          (sut/equals-to 42)
          (sut/not-equals-to 77)
          (sut/satisfies even?)))))
