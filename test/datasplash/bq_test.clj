(ns datasplash.bq-test
  (:require [clojure.test :refer :all]
            [datasplash.bq :refer :all]))

(deftest ->time-partitioning-test
  (are [opts expected]
    (let [tp (-> opts ->time-partitioning bean (select-keys [:type :expirationMs]))]
      (= expected tp))
    {} {:type "DAY" :expirationMs nil}
    {:type :day} {:type "DAY" :expirationMs nil}
    {:type :day :expiration-ms 1000} {:type "DAY" :expirationMs 1000}
    {:expiration-ms "bad format"} {:type "DAY" :expirationMs nil}))
