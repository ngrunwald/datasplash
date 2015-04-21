(ns datasplash.core-test
  (:require [clojure.test :refer :all]
            [datasplash.core :refer :all]))

(let [p (make-pipeline [])
      final (->> p
                 (generate-input [1 2 3])
                 (dmap inc)
                 (to-edn)
                 (write-file "ptest"))]

  (.run p))
