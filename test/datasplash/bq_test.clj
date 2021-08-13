(ns datasplash.bq-test
  (:require
   [clojure.test :refer [are deftest is testing]]
   [datasplash.bq :as sut]))

(deftest auto-parse-val-test
  (testing "num-string->long?"
    (is (= 3134 (sut/auto-parse-val "3134"))))
  (testing "string->string?"
    (is (= "asd" (sut/auto-parse-val "asd"))))
  (testing "other->other?"
    (is (= 3134 (sut/auto-parse-val 3134)))))

(deftest coerce-by-bq-val-test
  ;; TODO: add java.util.Date

  (testing "set"
    (let [rslt (sut/coerce-by-bq-val #{1 2 3})]
      (testing "as list?"
        (is (list? rslt)))
      (testing "same values?"
        (is (= #{1 2 3}
               (set rslt))))))

  (testing "keyword"
    (let [rslt (sut/coerce-by-bq-val :test)]
      (testing "as name?"
        (is (string? rslt)))
      (testing "same value?"
        (is (= :test
               (keyword rslt))))))

  (testing "symbol"
    (let [rslt (sut/coerce-by-bq-val 'test)]
      (testing "as name?"
        (is (string? rslt)))
      (testing "same value?"
        (is (= 'test
               (symbol rslt))))))

  (testing "etc"
    (let [d {:a 1}]
      (is (= d (sut/coerce-by-bq-val d))))))

(deftest bqize-keys-test
  (is (= {"this" {"is" {"a_a" {"test_1" 1}
                        "b__" {"test_2" 2}}
                  "another" {"_c_" {"test" 3}}
                  "32" {"3" 1}}}
         (sut/bqize-keys {"this" {"is" {"a-a" {"test-1" 1}
                                        "b--" {"test-2" 2}}
                                  "another" {"-c-" {"test?" 3}}
                                  32 {3 1}}}))))

(deftest ->time-partitioning-test
  (are [opts expected]
    (let [tp (-> opts sut/->time-partitioning bean (select-keys [:type :expirationMs]))]
      (= expected tp))
    {} {:type "DAY" :expirationMs nil}
    {:type :day} {:type "DAY" :expirationMs nil}
    {:type :day :expiration-ms 1000} {:type "DAY" :expirationMs 1000}
    {:expiration-ms "bad format"} {:type "DAY" :expirationMs nil}))
