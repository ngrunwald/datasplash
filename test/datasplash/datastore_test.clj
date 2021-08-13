(ns datasplash.datastore-test
  (:require
   [clojure.test :refer [deftest is]]
   [datasplash.datastore :as sut])
  (:import
   (com.google.datastore.v1 Entity)
   (java.util Date)))

(deftest datastore-conversion
  (let [o {:string "string" :integer 42 :double 65.78 :nil nil
           :array [1 "two" 3] :entity {:deeply "nested"}}
        coord {:key "key" :kind "kind" :namespace "ns" :path [{:kind "kind" :key "first"}
                                                              {:kind "kind" :key "last"}]}
        entity (sut/make-ds-entity o)
        ba (byte-array [(byte 0x43)])
        now (Date.)]
    (is (instance? Entity entity))
    (is (= o (sut/entity->clj entity)))
    (is (= coord (meta (sut/entity->clj (sut/make-ds-entity {} coord)))))
    (is (= (seq ba) (seq (sut/value->clj (.build (sut/make-ds-value-builder ba))))))
    (is (= now (sut/value->clj (.build (sut/make-ds-value-builder now)))))))
