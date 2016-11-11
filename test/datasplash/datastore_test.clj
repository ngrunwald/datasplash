(ns datasplash.datastore-test
  (:require [datasplash.datastore :refer :all]
            [clojure.test :refer :all])
  (:import [com.google.datastore.v1 Entity]
           [java.util Date]))
           
(deftest datastore-conversion
  (let [o {:string "string" :integer 42 :double 65.78 :nil nil
           :array [1 "two" 3] :entity {:deeply "nested"}}
        coord {:ds-key "key" :ds-kind "kind" :ds-namespace "ns"}
        entity (make-ds-entity o)
        ba (byte-array [(byte 0x43)])
        now (Date.)]
    (is (instance? Entity entity))
    (is (= o (entity->clj entity)))
    (is (= coord (meta (entity->clj (make-ds-entity {} coord)))))
    (is (= (seq ba) (seq (value->clj (.build (make-ds-value-builder ba))))))
    (is (= now (value->clj (.build (make-ds-value-builder now)))))))
